/*****************************************************************************\
|* QuickSilver :: Engine :: QSIO
|*
|* QSEngine is the client-facing interface to the database. It holds the entities
|* (which in turn hold the models) and has an ivar representing the SQLite
|* database
|*
|* Created by ThrudTheBarbarian on 4/8/24
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation
import OSLog
import SQLite3

public class QSIO : NSObject
	{
	/*************************************************************************\
	|* How much to enforce the D in ACID
	|* off		: just depend on the filesystem to flush disk
	|* normal	: call sync() to force a flush to disk
	|* full		: call fullfsync() to wait until drive has committed data. Slow
	\*************************************************************************/
	public enum Synchronicity
		{
		case off, normal, full, extra
		}

	enum NumberEncoding
		{
		case bool, int64, double
		}

	/*************************************************************************\
	|* Implement the description method
	\*************************************************************************/
	public override var description:String
		{
		return String(format:"%@ (RO:%@): errors:%d, %@in transaction, uncommitted:%d",
				self.className,
				self.readOnly ? "yes" : "no",
				self.errorCount,
				self.inTransaction ? "" : "not ",
				self.uncommittedUpdates)
		}
	/*************************************************************************\
	|* Instance variables
	\*************************************************************************/
		
	private var dbLock: NSLock							// Protect the database
	private var isLocked: Bool							// Locked right now ?
	private var currentThread: Thread!					// Our thread
	
	private var colXref: [String: [String]]				// columns by table
	private var preparedStatements: Set<QSPreparedSql>	// Current statements
	
	var bgWriteQueue: QSOperationQueue					// Background queue
	
	private var commitStmt: QSPreparedSql!				// don't keep creating
	private var beginStmt: QSPreparedSql!				// these two over again
	private var deferredStmt: QSPreparedSql!			// these two over again
	
	private var encodeMap:[String:NumberEncoding]		// used for types
	
	var db : OpaquePointer?								// Database reference
	var dbPath: String									// Path to DB file
	var activeResults: QSResultSet!						// Current query results

	var readOnly: Bool									// Is DB read-only
	
	var errorCount: Int									// # SQL errors so far
	var logsErrors: Bool								// Does DB log errors
	var crashOnErrors: Bool								// Crash on error ?
	var busyRetryTimeout: Int64							// # retries before fail

	var inTransaction: Bool								// In a transaction
	var uncommittedUpdates: Int							// #pending since commit

	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public init(withPath path: String)
		{
		/*********************************************************************\
		|* Instantiate any instance vars
		\*********************************************************************/
		self.dbLock 			= NSLock()
		self.isLocked			= false
		self.currentThread		= nil
		self.dbPath				= path
		self.colXref			= [String:[String]]()
		self.preparedStatements	= Set<QSPreparedSql>()
		self.bgWriteQueue		= QSOperationQueue()
		self.readOnly			= false
		self.errorCount			= 0
		self.logsErrors			= false
		self.crashOnErrors		= false
		self.busyRetryTimeout	= 42
		self.inTransaction		= false
		self.uncommittedUpdates	= 0
		
		self.encodeMap			= ["i" : NumberEncoding.int64,
								   "s" : NumberEncoding.int64,
								   "l" : NumberEncoding.int64,
								   "q" : NumberEncoding.int64,
								   "I" : NumberEncoding.int64,
								   "S" : NumberEncoding.int64,
								   "L" : NumberEncoding.int64,
								   "Q" : NumberEncoding.int64,
								   "f" : NumberEncoding.double,
								   "d" : NumberEncoding.double,
								   "B" : NumberEncoding.bool
								  ]
		super.init()
		}
	
	/*************************************************************************\
	|* Destruction
	\*************************************************************************/
	deinit
		{
		if self.db != nil
			{
			self.commitStmt = nil
			self.beginStmt = nil
			self.deferredStmt = nil
			_ = self.close()
			self.db = nil
			}
		}
		
	// MARK: Utility
	/*************************************************************************\
	|* Utility: return the SQLite library version
	\*************************************************************************/
	public func sqlLibraryVersion() -> String
		{
		return String(format:"%s", sqlite3_libversion())
		}
	
	// MARK: Locking
	/*************************************************************************\
	|* Locking: lock the database for this thread's access
	\*************************************************************************/
	public func lockDatabase()
		{
		self.dbLock.lock()
		self.currentThread = Thread.current
		self.isLocked = true
		}
	
	/*************************************************************************\
	|* Locking: unlock the database for this thread's access
	\*************************************************************************/
	public func unlockDatabase()
		{
		self.dbLock.unlock()
		self.currentThread = nil
		self.isLocked = false
		}
	
	/*************************************************************************\
	|* Locking: are we locked for this thread's access
	\*************************************************************************/
	public func isLockedForThread(_ thread:Thread) -> Bool
		{
		return self.isLocked || (self.currentThread == thread)
		}
		
	
	// MARK: Error management
	/*************************************************************************\
	|* Errors: Increment the error count
	\*************************************************************************/
	public func incrementSqlErrorCount()
		{
		self.errorCount += 1
		}

	/*************************************************************************\
	|* Errors: Get the last error message
	\*************************************************************************/
	public func lastErrorMessage() -> String
		{
		String(cString: sqlite3_errmsg(self.db))
		}

	/*************************************************************************\
	|* Errors: Get the last error code
	\*************************************************************************/
	public func lastErrorCode() -> Int
		{
		Int(sqlite3_errcode(self.db))
		}

	/*************************************************************************\
	|* Errors: Did the last operation proceed without error
	\*************************************************************************/
	public func hadError() -> Bool
		{
		return self.lastErrorCode() != SQLITE_OK
		}
	
	// MARK: Management
	
	/*************************************************************************\
	|* Management: Create a SQL statement to do a commit with
	\*************************************************************************/
	public func commitSql() -> QSPreparedSql
		{
		if self.commitStmt == nil
			{
			self.commitStmt = QSPreparedSql.init(withIo: self,
													sql: "COMMIT TRANSACTION",
													prepare: true)
			}
		return self.commitStmt
		}
		
	/*************************************************************************\
	|* Management: Create a SQL statement to do a begin-transaction with
	\*************************************************************************/
	public func beginTransactionSql(deferred:Bool = false) -> QSPreparedSql
		{
		if self.beginStmt == nil
			{
			self.beginStmt = QSPreparedSql.init(withIo: self,
												   sql: "BEGIN TRANSACTION",
											   prepare: true)
			}
		if self.deferredStmt == nil
			{
			self.deferredStmt = QSPreparedSql.init(withIo: self,
												   sql: "BEGIN DEFERRED TRANSACTION",
											   prepare: true)
			}
		return deferred ? self.deferredStmt : self.beginStmt
		}
		
	/*************************************************************************\
	|* Management: Commit and optionally begin a new transaction
	\*************************************************************************/
	public func commit(beginNewTransaction reopen:Bool = true) -> Bool
		{
		var success = true
		
		if self.readOnly
			{
			success = false
			}
		else
			{
			let commitSql:QSPreparedSql = self.commitSql()
			
			defer
				{
				self.unlockDatabase()
				}
			self.lockDatabase()
			
			if self.inTransaction
				{
				if !commitSql.update()
					{
					Logger.quicksilver.error("Failed to commit transaction!")
					success = false
					}
				else
					{
					self.inTransaction = false
					if !reopen
						{
						self.uncommittedUpdates = 0
						}
					else
						{
						let beginSql:QSPreparedSql = self.beginTransactionSql()
						if !beginSql.update()
							{
							Logger.quicksilver.error("Failed to begin new transaction!")
							success = false
							}
						else
							{
							self.inTransaction = true
							self.uncommittedUpdates = 0
							}
						}
					}
				}
			}
		return success
		}
		
	/*************************************************************************\
	|* Management: Begin a transaction
	\*************************************************************************/
	public func beginTransaction(deferred:Bool = false) -> Bool
		{
		var ok = false
		
		if !self.readOnly
			{
			let beginSql = self.beginTransactionSql(deferred:deferred)

			
			// Lock the database access while we're creating the transaction
			self.lockDatabase()
			defer
				{
				self.unlockDatabase()
				}
			
			if !beginSql.update()
				{
				Logger.quicksilver.log("Failed to begin a transaction")
				}
			else
				{
				self.inTransaction = true
				ok = true
				}
			}
		return ok
		}
		
	/*************************************************************************\
	|* Management: Rollback a transaction
	\*************************************************************************/
	public func rollback() -> Bool
		{
		let result = self.update("ROLLBACK TRANSACTION")
		if (result)
			{
			self.inTransaction = false
			self.uncommittedUpdates = 0
			}
		return result
		}
				
	/*************************************************************************\
	|* Management: Add a commit to the background queue
	\*************************************************************************/
	public func backgroundCommit(beginNewTransaction renew:Bool = false)
		{
		if !self.readOnly
			{
			let op = QSCommitOperation.init(with:self, beginNewTransaction:renew)
			self.bgWriteQueue.add(operation:op)
			}
		}
		
				
	/*************************************************************************\
	|* Management: Vacuum the database
	\*************************************************************************/
	public func vacuum()
		{
		if !self.readOnly
			{
			/*****************************************************************\
			|* We can't use these prepared statements after a vacuum, so clean
			|* them out now
			\*****************************************************************/
			self.finalisePreparedStatements()
			
			/*****************************************************************\
			|* Lock the database for this scope
			\*****************************************************************/
			self.lockDatabase()
			defer
				{
				self.unlockDatabase()

				/*************************************************************\
				|* Double-check, to ensure we didn't create any prepared
				|* statements during the vacuum (that would now be invalid)
				\*************************************************************/
				self.finalisePreparedStatements()
				}
			
			/*****************************************************************\
			|* We must commit, can't have outstanding transaction during vacuum
			\*****************************************************************/
			if self.inTransaction
				{
				if !self.update("COMMIT TRANSACTION")
					{
					Logger.quicksilver.warning("Failed to commit transaction in vacuum")
					}
				self.inTransaction = false
				}
			
			/*****************************************************************\
			|* Perform vacuum
			\*****************************************************************/
			if !self.update("VACUUM")
				{
				Logger.quicksilver.warning("Failed to perform VACUUM")
				}
			self.uncommittedUpdates = 0
			}
		}
				
	/*************************************************************************\
	|* Management: Analyse the database
	\*************************************************************************/
	public func analyse()
		{
		if !self.readOnly
			{
			/*****************************************************************\
			|* We can't use these prepared statements after a vacuum, so clean
			|* them out now
			\*****************************************************************/
			self.finalisePreparedStatements()
			
			/*****************************************************************\
			|* Lock the database for this scope
			\*****************************************************************/
			self.lockDatabase()
			defer
				{
				self.unlockDatabase()

				/*************************************************************\
				|* Double-check, to ensure we didn't create any prepared
				|* statements during the vacuum (that would now be invalid)
				\*************************************************************/
				self.finalisePreparedStatements()
				}
			
			/*****************************************************************\
			|* Perform analyse
			\*****************************************************************/
			if !self.update("ANALYZE")
				{
				Logger.quicksilver.warning("Failed to perform ANALYZE")
				}
			}
		}

	/*************************************************************************\
	|* Management: Set how we manage synchronicity
	\*************************************************************************/
	public func setSynchronicity(_ state:Synchronicity)
		{
		if !self.readOnly
			{
			let beginSql					= self.beginTransactionSql()
			let commitSql 					= self.commitSql()
			var syncSql : QSPreparedSql! 	= nil
			
			switch state
				{
				case .off:
					syncSql = QSPreparedSql.init(withIo: self,
													sql: "PRAGMA SYNCHRONOUS=OFF",
												prepare: false)
				case .normal:
					syncSql = QSPreparedSql.init(withIo: self,
													sql: "PRAGMA SYNCHRONOUS=NORMAL",
												prepare: false)
				case .full:
					syncSql = QSPreparedSql.init(withIo: self,
													sql: "PRAGMA SYNCHRONOUS=FULL",
												prepare: false)
				case .extra:
					syncSql = QSPreparedSql.init(withIo: self,
													sql: "PRAGMA SYNCHRONOUS=EXTRA",
												prepare: false)
				}
				
			if let syncSql = syncSql
				{
				self.lockDatabase()
				defer
					{
					self.unlockDatabase()
					}
				
				if self.inTransaction
					{
					_ = commitSql.update()
					_ = syncSql.update()
					_ = beginSql.update()
					self.uncommittedUpdates = 0
					}
				else
					{
					_ = syncSql.update()
					}
				}
			}
		}
	
	/*************************************************************************\
	|* Management: Fetch how we manage synchronicity
	\*************************************************************************/
	public func synchronicity() -> Synchronicity!
		{
	
		let sync = self.int64For("PRAGMA SYNCHRONOUS")
		switch (sync)
			{
			case 0:
				return Synchronicity.off
			case 1:
				return Synchronicity.normal
			case 2:
				return Synchronicity.full
			case 3:
				return Synchronicity.extra
			default:
				return nil
			}
		}

	/*************************************************************************\
	|* Management: Set the locking mode
	\*************************************************************************/
	public func setLockingMode(exclusive:Bool)
		{
		self.lockDatabase()
		defer
			{
			self.unlockDatabase()
			}
		
		if self.inTransaction
			{
			if !self.commit(beginNewTransaction:false)
				{
				Logger.quicksilver.warning("Failed to terminate transaction")
				}
			if !self.update(exclusive ? "PRAGMA locking_mode=EXCLUSIVE"
									  : "PRAGMA locking_mode=NORMAL",nil)
				{
				Logger.quicksilver.warning("Failed to change locking mode")
				}
			if !self.beginTransaction()
				{
				Logger.quicksilver.warning("Failed to restart transaction")
				}
			}
		else
			{
			if !self.update(exclusive ? "PRAGMA locking_mode=EXCLUSIVE"
									  : "PRAGMA locking_mode=NORMAL",nil)
				{
				Logger.quicksilver.warning("Failed to change locking mode")
				}
			}
		}


	/*************************************************************************\
	|* Management: Create a simple single-column index on a table
	\*************************************************************************/
	public func createIndex(onTable table:String, column colName:String, named:String! = nil) -> Bool
		{
		return self.createIndex(onTable:table, columns:[colName], named:named)
		}

	public func createIndex(onTable table:String,
					  columns cols:[String],
						named name:String! = nil) -> Bool
		{
		var created = false
		
		if !self.readOnly
			{
			let clist = cols.joined(separator: ",")
			let index = name == nil
					  ? "idx_" + table + "_" + cols.joined(separator: "_")
					  : name!
			let sql	  = String(format:"CREATE INDEX IF NOT EXISTS %@ " +
									  "ON %@(%@)", index, table, clist)

			/*****************************************************************\
			|* Wait for background work to complete
			\*****************************************************************/
			self.bgWriteQueue.waitUntilAllOperationsAreFinished()
			
			created   = self.update(sql)
			}
		
		return created
		}
		
		
	// MARK: i/o
	
	/*************************************************************************\
	|* i/o: Open the database, optionally as read-only
	\*************************************************************************/
	public func open(asReadOnly readOnly:Bool = false) -> Bool
		{
		var ok 			= false
		self.readOnly 	= readOnly

		// Don't set up the actual pointer until we're fully open
		var db : OpaquePointer? = nil
		let flags = readOnly
				  ? SQLITE_OPEN_READONLY
				  : SQLITE_OPEN_READWRITE
				  | SQLITE_OPEN_CREATE
				  | SQLITE_OPEN_FULLMUTEX
				  
		// Lock the database access while we're creating/configuring
		self.lockDatabase()
		defer
			{
			self.unlockDatabase()
			}
		
		let err = sqlite3_open_v2(self.dbPath, &db, flags, nil)
		if err != SQLITE_OK
			{
			let msg = String(cString: sqlite3_errstr(err))
			Logger.quicksilver.log("Error opening \(self.dbPath): \(msg)")
			}
		else
			{
			self.db = db
			ok 		= true
			if !self.readOnly
				{
				/*************************************************************\
				|* Turn on incremental vacuum (works if no tables created yet)
				\*************************************************************/
				sqlite3_exec(db, "pragma auto_vacuum=incremental", nil, nil, nil);
				sqlite3_exec(db, "pragma cache_size=2000", nil, nil, nil);
				sqlite3_exec(db, "pragma fullfsync=NO", nil, nil, nil);

				/*************************************************************\
				|* Try out changing the journal mode to something faster -
				|* Truncates journal files down to 5 Mb when we're done.
				\*************************************************************/
				sqlite3_exec(db, "pragma journal_mode=persist", nil, nil, nil);
				sqlite3_exec(db, "pragma journal_size_limit=5000000", nil, nil, nil);
				}
			}
		
		return ok
		}
	
	/*************************************************************************\
	|* i/o: return the last inserted auto-updated row-id
	\*************************************************************************/
	public func lastInsertRowId() -> Int64
		{
		self.lockDatabase()
		defer
			{
			self.unlockDatabase()
			}
			
		return Int64(sqlite3_last_insert_rowid(self.db))
		}
		
	/*************************************************************************\
	|* i/o: test the database accessibility
	\*************************************************************************/
	public func isActive() -> Bool
		{
		var active = false
		
		if self.db == nil
			{
			return false
			}
			
		let brt = self.busyRetryTimeout
		self.busyRetryTimeout = 20
		
		if let rs = self.query("SELECT name FROM sqlite_master WHERE type='table'")
			{
			defer
				{
				rs.close()
				}
			active = rs.next()
			}
		
		self.busyRetryTimeout = brt
		return active
		}
	
	/*************************************************************************\
	|* i/o: Close the database
	\*************************************************************************/
	public func close() -> Bool
		{
		if self.db == nil
			{
			return true
			}
		
		/*********************************************************************\
		|* Wait for background work to complete
		\*********************************************************************/
		self.bgWriteQueue.waitUntilAllOperationsAreFinished()
		
		/*********************************************************************\
		|* Check the result-set
		\*********************************************************************/
		if self.activeResults != nil
			{
			Logger.quicksilver.warning("Closing down with an active result-set\nresults: \(String(describing: self.activeResults))\nin database:\(self.dbPath)")
			}
		
		/*********************************************************************\
		|* We can't shut down with active statements, so finalise them all now
		\*********************************************************************/
		self.finalisePreparedStatements()
		if (self.preparedStatements.count != 0)
			{
			Logger.quicksilver.warning("Shutting down with \(self.preparedStatements.count) prepared statements left open\n\(self.preparedStatements)" )
			}

		if !self.readOnly
			{
			sqlite3_exec(self.db, "pragma journal_mode=delete", nil, nil, nil);
			sqlite3_exec(self.db, "begin transaction", nil, nil, nil);
			sqlite3_exec(self.db, "commit", nil, nil, nil);

			sqlite3_exec(self.db, "pragma incremental_vacuum(1000)", nil, nil, nil);
			}

		/*********************************************************************\
		|* Attempt to close down the database connection. Note that to prevent
		|* a hang in an error condition, you *must* set the busyRetryTimeout.
		\*********************************************************************/
		var retry = true
		var numRetries = 0
		repeat
			{
			retry 	= false
			let rc	= sqlite3_close(self.db)
			
			if (SQLITE_BUSY == rc)
				{
				if (self.busyRetryTimeout > 0)
					{
					numRetries += 1
					if (numRetries > self.busyRetryTimeout)
						{
						Logger.quicksilver.error("Failed to close the DB after \(self.busyRetryTimeout) attempts")
						self.db = nil
						return false
						}
					
					retry = true
					Thread.sleep(forTimeInterval: 0.2)
					}
				else
					{
					retry = true
					Thread.sleep(forTimeInterval: 0.2)
					}
				}
			} while (retry)

		self.db = nil
		return true
		}
		

	// MARK: prepared statements

	/*****************************************************************************\
	|* statement: We can't close cleanly while prepared statements remain prepared...
	\*****************************************************************************/
	public func finalisePreparedStatements()
		{
		self.lockDatabase()
		defer
			{
			self.unlockDatabase()
			}
			
		for psql in self.preparedStatements
			{
			psql.finaliseSqlWithoutLock()
			}
		self.preparedStatements.removeAll()
		
		}
	
	/*************************************************************************\
	|* statement: register the prepared statement into the cache
	\*************************************************************************/
	public func registerPreparedStatement(_ ps: QSPreparedSql!)
		{
		if let ps = ps
			{
			self.preparedStatements.insert(ps)
			}
		}
		
	/*************************************************************************\
	|* statement: unregister the prepared statement into the cache
	\*************************************************************************/
	public func unregisterPreparedStatement(_ ps: QSPreparedSql!)
		{
		if let ps = ps
			{
			self.preparedStatements.remove(ps)
			}
		}
		
	/*************************************************************************\
	|* statement: bind an object to a statement
	\*************************************************************************/
	public func bind(item obj:AnyObject,
			  toColumn idx:Int32,
			  inStatement stmt:OpaquePointer?)
		{
		let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1),
								to: sqlite3_destructor_type.self)

		if let stmt = stmt
			{
			/*****************************************************************\
			|* Cope with strings...
			\*****************************************************************/
			if obj is NSString
				{
				let str = obj as! String
				sqlite3_bind_text(stmt, idx, str, -1, SQLITE_TRANSIENT);
				}

			/*****************************************************************\
			|* Cope with numbers...
			\*****************************************************************/
			else if obj is NSNumber
				{
				let num 	= obj as! NSNumber
				let type	= String(cString: num.objCType)
				let form	= self.encodeMap[type]
				
				if let form = form
					{
					switch (form)
						{
						case NumberEncoding.bool:
							sqlite3_bind_int(stmt, idx, num.boolValue ? 1 : 0)
						case NumberEncoding.int64:
							sqlite3_bind_int64(stmt, idx, num.int64Value)
						case NumberEncoding.double:
							sqlite3_bind_double(stmt, idx, num.doubleValue)
						}
					}
				else
					{
					sqlite3_bind_text(stmt,
									  idx,
									  num.description,
									  -1,
									  SQLITE_TRANSIENT)
					}
		
				}
			/*****************************************************************\
			|* Cope with NULL, passed as [NSNull null]
			\*****************************************************************/
			else if obj is NSNull
				{
				sqlite3_bind_null(stmt, idx)
				}
			
			/*****************************************************************\
			|* Cope with Date, use unix-time
			\*****************************************************************/
			else if obj is NSDate
				{
				let cron = obj as! NSDate
				sqlite3_bind_double(stmt, idx, cron.timeIntervalSince1970)
				}
				
			/*****************************************************************\
			|* Cope with Blob,
			\*****************************************************************/
			else if obj is NSData
				{
				let data = obj as! NSData
				sqlite3_bind_blob(stmt,
								  idx,
								  data.bytes,
								  Int32(data.length),
								  SQLITE_TRANSIENT)
				}

			/*****************************************************************\
			|* WTF ?
			\*****************************************************************/
			else
				{
				Logger.quicksilver.log("Found unexpected object, using string")
				let type = obj as! NSObject
				sqlite3_bind_text(stmt,
							  idx,
							  type.description,
							  -1,
							  SQLITE_TRANSIENT)
				}
			}
		}
	
	// MARK: column level i/o
	
	/*************************************************************************\
	|* column io: Return a string for a query of only one column
	\*************************************************************************/
	public func stringFor(_ sql:Any, _ args:AnyObject...) -> String!
		{
		var psql:QSPreparedSql! = nil
		
		if (sql is QSPreparedSql)
			{
			psql = sql as? QSPreparedSql
			}
		else if (sql is String)
			{
			psql = QSPreparedSql(withIo:self, sql:sql as! String, prepare:false)
			}
	
		if let psql = psql
			{
			let filtered = psql.filter(args:args)
			if let rs = psql.query(with: filtered)
				{
				defer
					{
					rs.close()
					}
					
				if rs.next()
					{
					return rs.stringForColumn(withIndex: 0)
					}
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* column io: Return a number for a query of only one column
	\*************************************************************************/
	public func numberFor(_ sql:Any, _ args:AnyObject...) -> NSNumber!
		{
		var psql:QSPreparedSql! = nil
		
		if (sql is QSPreparedSql)
			{
			psql = sql as? QSPreparedSql
			}
		else if (sql is String)
			{
			psql = QSPreparedSql(withIo:self, sql:sql as! String, prepare:false)
			}
	
		if let psql = psql
			{
			let filtered = psql.filter(args:args)
			if let rs = psql.query(with: filtered)
				{
				defer
					{
					rs.close()
					}
					
				if rs.next()
					{
					return rs.numberForColumn(withIndex: 0)
					}
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* column io: Return a date for a query of only one column
	\*************************************************************************/
	public func dateFor(_ sql:Any, _ args:AnyObject...) -> Date!
		{
		var psql:QSPreparedSql! = nil
		
		if (sql is QSPreparedSql)
			{
			psql = sql as? QSPreparedSql
			}
		else if (sql is String)
			{
			psql = QSPreparedSql(withIo:self, sql:sql as! String, prepare:false)
			}
	
		if let psql = psql
			{
			let filtered = psql.filter(args:args)
			if let rs = psql.query(with: filtered)
				{
				defer
					{
					rs.close()
					}
					
				if rs.next()
					{
					return rs.dateForColumn(withIndex: 0)
					}
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* column io: Return a data for a query of only one column
	\*************************************************************************/
	public func dataFor(_ sql:Any, _ args:AnyObject...) -> Data!
		{
		var psql:QSPreparedSql! = nil
		
		if (sql is QSPreparedSql)
			{
			psql = sql as? QSPreparedSql
			}
		else if (sql is String)
			{
			psql = QSPreparedSql(withIo:self, sql:sql as! String, prepare:false)
			}
	
		if let psql = psql
			{
			let filtered = psql.filter(args:args)
			if let rs = psql.query(with: filtered)
				{
				defer
					{
					rs.close()
					}
					
				if rs.next()
					{
					return rs.dataForColumn(withIndex: 0)
					}
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* column io: Return a bool for a query of only one column
	\*************************************************************************/
	public func boolFor(_ sql:Any, _ args:AnyObject...) -> Bool!
		{
		var psql:QSPreparedSql! = nil
		
		if (sql is QSPreparedSql)
			{
			psql = sql as? QSPreparedSql
			}
		else if (sql is String)
			{
			psql = QSPreparedSql(withIo:self, sql:sql as! String, prepare:false)
			}
	
		if let psql = psql
			{
			let filtered = psql.filter(args:args)
			if let rs = psql.query(with: filtered)
				{
				defer
					{
					rs.close()
					}
					
				if rs.next()
					{
					return rs.boolForColumn(withIndex: 0)
					}
				}
			}
		return nil
		}
		
	/*************************************************************************\
	|* column io: Return an int64 for a query of only one column
	\*************************************************************************/
	public func int64For(_ sql:Any, _ args:AnyObject...) -> Int64!
		{
		var psql:QSPreparedSql! = nil
		
		if (sql is QSPreparedSql)
			{
			psql = sql as? QSPreparedSql
			}
		else if (sql is String)
			{
			psql = QSPreparedSql(withIo:self, sql:sql as! String, prepare:false)
			}
	
		if let psql = psql
			{
			let filtered = psql.filter(args:args)
			if let rs = psql.query(with: filtered)
				{
				defer
					{
					rs.close()
					}
					
				if rs.next()
					{
					return rs.int64ForColumn(withIndex: 0)
					}
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* column io: Return an int64 for a query of only one column
	\*************************************************************************/
	public func int64For(_ sql:Any) -> Int64!
		{
		return self.int64For(sql, NSNull.init())
		}
		
	// MARK: Execution
	
	
	/*************************************************************************\
	|* execution: Execute a query on the db
	\*************************************************************************/
	public func query(_ sql:Any, withArgs args:[Any?]) -> QSResultSet!
		{
		var psql:QSPreparedSql! = nil
		
		if sql is String
			{
			psql = QSPreparedSql.init(withIo:self, sql:sql as! String, prepare:false)
			}
		else if sql is QSPreparedSql
			{
			psql = sql as? QSPreparedSql
			}

		if let psql = psql
			{
			let filtered = QSPreparedSql.filter(all:args)
			return psql.query(with: filtered)
			}
		else
			{
			Logger.quicksilver.log("QSIO:query passed incorrect object \(String (describing: sql))")
			return nil
			}
		}
	
	/*************************************************************************\
	|* execution: Execute a query on the db, varargs version
	\*************************************************************************/
	public func query(_ sql:Any, _ args:Any?...) -> QSResultSet!
		{
		return self.query(sql, withArgs:args)
		}
	
	/*************************************************************************\
	|* execution: Execute an update on the db
	\*************************************************************************/
	public func update(_ sql:Any, withArgs args:[Any?]) -> Bool
		{
		var psql:QSPreparedSql! = nil
		
		if sql is String
			{
			psql = QSPreparedSql.init(withIo:self, sql:sql as! String, prepare:false)
			}
		else if sql is QSPreparedSql
			{
			psql = sql as? QSPreparedSql
			}

		if let psql = psql
			{
			let filtered = QSPreparedSql.filter(all:args)
			return psql.update(with: filtered)
			}
		else
			{
			Logger.quicksilver.log("QSIO:update passed incorrect object \(String (describing: sql))")
			return false
			}
		}
	
	/*************************************************************************\
	|* execution: Execute a query on the db, varargs version
	\*************************************************************************/
	public func update(_ sql:Any, _ args:Any?...) -> Bool
		{
		return self.update(sql, withArgs:args)
		}
	}
