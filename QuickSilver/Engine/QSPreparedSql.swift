/*****************************************************************************\
|* QuickSilver :: Engine :: QSPreparedSql
|*
|* This is the interface to the SQL statement. We can prepare statements (and
|* potentially cache them later), and substitute parameters into the statements
|* as needed
|*
|* Created by ThrudTheBarbarian on 4/9/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/
import Foundation
import OSLog
import SQLite3

/*****************************************************************************\
|* Class definition
\*****************************************************************************/
@objc public class QSPreparedSql : NSObject
	{
	/*************************************************************************\
	|* Instance variables
	\*************************************************************************/
	private(set) var io: QSIO					// Link to the db
	private(set) var stmt: OpaquePointer?		// Statement in use
	
	private var sqlError: Int32					// Error from SQLite
	private var multiBindSql: Bool				// This uses #? bindings
	private var numBindPoints: Int				// # bind points
	
	// Only used with multi-bind-point SQL
	private var fixedBindPoints: Int			// # non-multi bind points
	private var multiBindFragments: [String]	// pieces not multi-bound
	private var multiBindIndexes: IndexSet		// # multi-bind points
	private var expandedSql: String				// expanded multi-bind sql

	var isPrepared: Bool						// Are we prepared yet
	var sql: String								// Query/update in progress
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	@objc public init(withIo io: QSIO, sql:String, prepare:Bool)
		{
		/*********************************************************************\
		|* Set up the instance variables
		\*********************************************************************/
		self.io 				= io
		self.stmt				= nil
		self.sql 				= sql
		
		self.sqlError			= SQLite3.SQLITE_OK
		self.multiBindSql		= false
		self.numBindPoints		= 0
		
		self.expandedSql		= ""
		self.multiBindFragments	= [String]()
		self.multiBindIndexes	= IndexSet()
		self.fixedBindPoints	= 0

		self.isPrepared			= false

		/*********************************************************************\
		|* Create the parent object instance
		\*********************************************************************/
		super.init()
		
		/*********************************************************************\
		|* Sql using multi-bind points (#?) can never be prepared
		\*********************************************************************/
		self.multiBindSql = sql.contains("#?")
		
		if !self.multiBindSql
			{
			self.numBindPoints = QSPreparedSql.countBindPoints(self.sql)
			if (prepare)
				{
				self.prepareSql()
				}
			}
		else
			{
			self.multiBindFragments = self.sql.components(separatedBy: "#?")

			/*****************************************************************\
			|* Count up the number of fixed bind points, and figure out where 
			|* the multi-bind points are at
			\*****************************************************************/
			var atMultiBindPoint = false
			var multiBindIndex = 0
			var numMultiBindIndices = 0
			
			for fragment in self.multiBindFragments
				{
				if atMultiBindPoint
					{
					self.multiBindIndexes.insert(multiBindIndex)
					}
				self.fixedBindPoints += QSPreparedSql.countBindPoints(fragment)
				atMultiBindPoint 	  = true
				multiBindIndex		  = self.fixedBindPoints + numMultiBindIndices
				numMultiBindIndices  += 1
				}
			}
		}

	// MARK: static utility functions
	
	/*************************************************************************\
	|* Count the number of bind-points in a given object
	\*************************************************************************/
	static func countBindPoints(_ sql:Any) -> Int
		{
		var bindCount:Int = 0
		
		switch sql
			{
			case let sql as QSPreparedSql:
				bindCount = sql.numBindPoints
			case let sql as String:
				bindCount = sql.components(separatedBy: "?").count - 1
			default:
				Logger.quicksilver.error("Unknown class presented to QSPreparedSql.countBindPoints")
			}
		return bindCount
		}
	
	/*************************************************************************\
	|* Returns a list of question marks that can be used in query binding
	\*************************************************************************/
	static func questionMarkList(_ count:Int) -> String
		{
		if (count < 0)
			{
			Logger.quicksilver.warning("Asked for -ve list of ?'s (!)")
			return ""
			}
		if (count == 0)
			{
			return ""
			}
		if (count == 1)
			{
			return "?"
			}
			
		return "?"+String(repeating: ",?", count: count-1)
		}

	/*************************************************************************\
	|* Get rid of any characters that could confuse LIKE by escaping them
	\*************************************************************************/
	static func escapeLikeSpecialChars(for input:String) -> String
		{
		var set = CharacterSet.init()
		set.insert(charactersIn: "%_\\")
		
		let range = input.rangeOfCharacter(from: set)
		if range != nil
			{
			/*****************************************************************\
			|* We're going to always use \ as the escape character, so
			|* escape any preexisting \'s first
			\*****************************************************************/
			var replace = input.replacingOccurrences(of: "\\", with: "\\\\")
			replace = replace.replacingOccurrences(of: "%", with: "\\%")
			replace = replace.replacingOccurrences(of: "_", with: "\\_")
			return replace
			}
		else
			{
			return input
			}
		}
	
	// MARK: Debugging
	
	/*************************************************************************\
	|* Implement the description method
	\*************************************************************************/
	@objc public override var description:String
		{
		return String(format:"%@ (%@): %@",
				self.className,
				self.isPrepared ? "prepared" : "unprepared",
				self.sql)
		}
	
	
	// MARK: Derivations
	
	/*************************************************************************\
	|* Calculate the number of arguments
	\*************************************************************************/
	@objc func numberOfArguments() -> Int
		{
		return (self.multiBindSql)
			? self.fixedBindPoints + self.multiBindIndexes.count
			: self.numBindPoints
		}
	
	/*************************************************************************\
	|* Calculate the number of columns
	\*************************************************************************/
	@objc func columnCount() -> Int
		{
		Int(sqlite3_column_count(self.stmt))
		}
		
	/*************************************************************************\
	|* Return the name of a column
	\*************************************************************************/
	@objc func columnName(forIndex idx:Int) -> String!
		{
		var result:String! = nil
		
		if (idx >= 0) && (idx < self.columnCount())
			{
			result = String(cString: sqlite3_column_name(self.stmt, Int32(idx)))
			}
		return result
		}
		
	/*************************************************************************\
	|* Step to the next row
	\*************************************************************************/
	@objc func step() -> Int32
		{
		sqlite3_step(self.stmt)
		}
		
	/*************************************************************************\
	|* Latest error from the statement
	\*************************************************************************/
	@objc func lastError() -> String
		{
		String(cString: sqlite3_errmsg(self.stmt))
		}
		
	// MARK: Argument processing
	
	/*************************************************************************\
	|* Convert an arguments into a classe. We currently support:
	|*
	|* String					=> NSString
	|* Array					=> NSArray
	|* Set						=> NSSet
	|* Int,Int64,Double,Bool	=> NSNumber
	\*************************************************************************/
	static func convertArg(_ arg:Any?) -> AnyObject
		{
		if (arg == nil)
			{ return NSNull.init() }
		else if arg is String
			{ return arg.unsafelyUnwrapped as! NSString }
		else if arg is Date
			{ return arg.unsafelyUnwrapped as! NSDate }
		else if arg is NSArray
			{ return arg.unsafelyUnwrapped as! NSArray }
		else if arg is NSSet
			{ return arg.unsafelyUnwrapped as! NSSet }
		else if arg is Int
			{ return arg.unsafelyUnwrapped as! NSNumber }
		else if arg is Int64
			{ return arg.unsafelyUnwrapped as! NSNumber }
		else if arg is Double
			{ return arg.unsafelyUnwrapped as! NSNumber }
		else if arg is Bool
			{ return arg.unsafelyUnwrapped as! NSNumber }
			
		Logger.quicksilver.error("Unknown type \(String(describing:arg)) passed into convertArg")
		return NSString.init(string:"(unknown type)")
		}
		
	/*************************************************************************\
	|* Convert a list of arguments into a list of classes.
	\*************************************************************************/
	static func filter(all args:[Any?]) -> [AnyObject]
		{
		var filtered = [AnyObject]()
		for arg in args
			{
			filtered.append(QSPreparedSql.convertArg(arg))
			}
			
		return filtered
		}
		
	/*************************************************************************\
	|* Convert a list of arguments into a list of classes.
	\*************************************************************************/
	func filter(args:[Any?]) -> [AnyObject]
		{
		if (self.numberOfArguments() != args.count)
			{
			Logger.quicksilver.error("Number of arguments doesn't match list for \(self.sql)")
			}
			
		var filtered = [AnyObject]()
		for arg in args
			{
			filtered.append(QSPreparedSql.convertArg(arg))
			}
			
		return filtered
		}
	
	/*************************************************************************\
	|* Make sure nil -> [NSNull null] in the arguments for SQL
	\*************************************************************************/
	static func filter(args:[AnyObject?], for sql:String) -> [AnyObject]
		{
		let bindCount = QSPreparedSql.countBindPoints(sql)
		
		if (bindCount != args.count)
			{
			Logger.quicksilver.error("Number of bind points doesn't match for \(sql)")
			}
			
		var filtered = [AnyObject]()
		for arg in args
			{
			filtered.append(convertArg(arg))
			}
			
		return filtered
		}
		
	/*************************************************************************\
	|* Expand SQL so we can bind an array/set to the command
	\*************************************************************************/
	func expandSql(for args:[AnyObject]?)
		{
		if self.multiBindSql
			{
			if let args = args
				{
				self.expandedSql 	= ""
				self.numBindPoints  = self.fixedBindPoints
				if var multiBindIndex	= self.multiBindIndexes.min()
					{
					for sqlFragment in self.multiBindFragments
						{
						self.expandedSql.append(sqlFragment)
						
						var multiBindArg: AnyObject? = nil
						
						if (multiBindIndex < args.count)
							{
							multiBindArg = args[multiBindIndex]
							}
						else
							{
							Logger.quicksilver.error(
								"Not enough arguments passed for sql:\(self.sql), args: \(args)")
							}
							
						
						if let multiBindArg = multiBindArg
							{
							var bindPoints = 0
							
							if (multiBindArg is NSNull)
								{
								// That means an empty array was passed,
								// which is fine
								}
							else if multiBindArg is NSSet
								{
								bindPoints = multiBindArg.count
								}
							else if multiBindArg is NSArray
								{
								bindPoints = multiBindArg.count
								}
							else
								{
								Logger.quicksilver.error(
									"Incorrect class \(multiBindArg.className) attempted to bind to a multi-bind point (#?)")
								}
							if (bindPoints > 0)
								{
								let qs = QSPreparedSql.questionMarkList(bindPoints)
								self.expandedSql.append(qs)
								self.numBindPoints += bindPoints
								}
							multiBindIndex = self.multiBindIndexes.integerGreaterThan(multiBindIndex) ?? -1
							}
						}
					}
				}
			}
		}
		
	/*************************************************************************\
	|* Prepare the statement
	\*************************************************************************/
	@objc public func prepare() -> Bool
		{
		var isPrepared:Bool = true
		
		if !self.isPrepared
			{
			let sql = (self.multiBindSql) ? self.expandedSql : self.sql
			if !sql.isEmpty
				{
				self.sqlError = sqlite3_prepare_v2(self.io.db,
												   sql,
												   -1,
												   &self.stmt,
												   nil)
				if (self.sqlError != SQLITE_OK)
					{
					isPrepared = false
					sqlite3_finalize(self.stmt)
					self.stmt = nil
					self.io.incrementSqlErrorCount()
					Logger.quicksilver.error("Could not prepare SQL:\(sql)")
					}
				}
			else
				{
				Logger.quicksilver.error("No SQL provided in prepare()")
				isPrepared = false
				}
			}
			
		return isPrepared
		}
		
	/*************************************************************************\
	|* Bind the arguments
	\*************************************************************************/
	@objc public func bind(arguments args:[AnyObject]) -> Int32
		{
		var rc = SQLITE_OK
		
		if args.count > 0
			{
			if self.multiBindSql
				{
				var idx:Int32		= 1
				var originalIndex	= 0
				if var multiBindIndex = self.multiBindIndexes.min()
					{
					for arg in args
						{
						if (originalIndex == multiBindIndex)
							{
							if arg is NSNull
								{
								// This means an empty set or array was passed
								}
							else if arg is NSSet
								{
								let set = arg as! NSSet
								for sub in set
									{
									self.io.bind(item:QSPreparedSql.convertArg(sub),
												 toColumn:idx,
												 inStatement:self.stmt)
									idx += 1
									}
								}
							else if arg is NSArray
								{
								let array = arg as! NSArray
								for sub in array
									{
									self.io.bind(item:QSPreparedSql.convertArg(sub),
												 toColumn:idx,
												 inStatement:self.stmt)
									idx += 1
									}
								}
							else
								{
								Logger.quicksilver.error(
									"Incorrect class \(arg.className) attempted to bind to a multi-bind point (#?)")
								rc = SQLITE_ERROR
								break
								}
							
							multiBindIndex = self.multiBindIndexes.integerGreaterThan(multiBindIndex) ?? -1
							if multiBindIndex < 0
								{
								Logger.quicksilver.error("Cannot find sufficient args to bind")
								break
								}
							}
						else
							{
							self.io.bind(item:arg,
										 toColumn:idx,
										 inStatement:self.stmt)
							idx += 1
							}
							
						originalIndex += 1
						}
					}
				}
			else
				// Non-multi-bind-sql case
				{
				if args.count != self.numBindPoints
					{
					Logger.quicksilver.error("Wrong number of arguments \(args.count) supplied to sql:\(self.sql)")
					rc = SQLITE_RANGE
					}
				else
					{
					var idx:Int32 = 1
					for arg in args
						{
						self.io.bind(item:arg,
									 toColumn:idx,
									 inStatement:self.stmt)
						idx += 1
						}
					}
				}
			}
		return rc
		}
		
	/*************************************************************************\
	|* Close the statement
	\*************************************************************************/
	@objc public func close()
		{
		if self.isPrepared
			{
			sqlite3_reset(self.stmt)
			sqlite3_clear_bindings(self.stmt)
			}
		else
			{
			sqlite3_finalize(self.stmt)
			self.stmt = nil
			}
		}
		
	/*************************************************************************\
	|* Prepare the statement
	\*************************************************************************/
	@objc public func prepareSql()
		{
		if !self.multiBindSql && !self.isPrepared
			{
			defer
				{
				self.io.unlockDatabase()
				}
				
			// If code is stuck here, try printint out self.db.activeResults
			// to see which resultSet wasn't closed
			self.io.lockDatabase()
			
			self.sqlError = sqlite3_prepare_v2(self.io.db,
											   sql,
											   -1,
											   &self.stmt,
											   nil)
			if (self.sqlError != SQLITE_OK)
				{
				isPrepared = false
				sqlite3_finalize(self.stmt)
				self.stmt = nil
				self.io.incrementSqlErrorCount()
				Logger.quicksilver.error("Could not prepare SQL:\(self.sql)")
				}
			else
				{
				self.isPrepared = true
				self.io.registerPreparedStatement(self)
				}
			}
		}
		
	/*************************************************************************\
	|* Update using a set of args
	\*************************************************************************/
	@objc public func update(with args:[AnyObject] = []) -> Bool
		{
		var selfLocked = false
		
		if !self.io.isLockedForThread(Thread.current)
			{
			self.io.lockDatabase()
			selfLocked = true
			}
					
		/*********************************************************************\
		|* For multi-bind SQL (containing #?), we need to expand to the actual
		|* SQL we'll use
		\*********************************************************************/
		self.expandSql(for: args)
		
		/*********************************************************************\
		|* If this statement isn't prepared, then we need to do so now
		\*********************************************************************/
		if !self.prepare()
			{
			Logger.quicksilver.error("Failed to prepare statement!")
			return false
			}
			
		/*********************************************************************\
		|* Bind the objects to their parameters in the query...
		\*********************************************************************/
		if self.bind(arguments: args) != SQLITE_OK
			{
			Logger.quicksilver.error("Failed to bind arguments \(args)!")
			return false
			}
		
		/*********************************************************************\
		|* And go for it...
		|*
		|* Call sqlite3_step() to run the virtual machine. Since the SQL being
		|* executed is not a SELECT statement, assume no data will be returned
		\*********************************************************************/
		let rc = sqlite3_step(self.stmt)
		
		if (rc == SQLITE_DONE) || (rc == SQLITE_ROW)
			{
			// Everything is ok
			if (self.io.inTransaction)
				{
				self.io.uncommittedUpdates += 1
				}
			}
		else if (rc == SQLITE_ERROR)
			{
			let msg:String = String(cString: sqlite3_errmsg(self.stmt))
			Logger.quicksilver.error("QSPreparedSql updateWithArgs (ERROR) \(msg)")
			Logger.quicksilver.error("for SQL:\(self.sql)")
			self.io.incrementSqlErrorCount()
			}
		else if (rc == SQLITE_MISUSE)
			{
			let msg:String = String(cString: sqlite3_errmsg(self.stmt))
			Logger.quicksilver.error("QSPreparedSql updateWithArgs (MISUSE) \(msg)")
			Logger.quicksilver.error("for SQL:\(self.sql)")
			self.io.incrementSqlErrorCount()
			}
		else
			{
			let msg:String = String(cString: sqlite3_errmsg(self.stmt))
			Logger.quicksilver.error("QSPreparedSql updateWithArgs (UNKNOWN) \(msg)")
			Logger.quicksilver.error("for SQL:\(self.sql)")
			self.io.incrementSqlErrorCount()
			}
			
		self.close()
		if selfLocked
			{
			self.io.unlockDatabase()
			}
			
		return (rc == SQLITE_DONE) || (rc == SQLITE_OK) || (rc == SQLITE_ROW)
		}

		
	/*************************************************************************\
	|* Query using a set of args
	\*************************************************************************/
	@objc public func query(with args:[AnyObject]) -> QSResultSet!
		{
		/*********************************************************************\
		|* If code is stuck here, try printint out self.db.activeResults
		|* to see which resultSet wasn't closed
		\*********************************************************************/
		self.io.lockDatabase()
		
		/*********************************************************************\
		|* For multi-bind SQL (containing #?), we need to expand to the actual
		|* SQL we'll use
		\*********************************************************************/
		self.expandSql(for: args)
		
		/*********************************************************************\
		|* If this statement isn't prepared, then we need to do so now
		\*********************************************************************/
		if !self.prepare()
			{
			Logger.quicksilver.error("Failed to prepare statement!")
			return nil
			}
			
		/*********************************************************************\
		|* Bind the objects to their parameters in the query...
		\*********************************************************************/
		if self.bind(arguments: args) != SQLITE_OK
			{
			Logger.quicksilver.error("Failed to bind arguments \(args)!")
			return nil
			}
		
		/*********************************************************************\
		|* Go for it
		\*********************************************************************/
		self.io.activeResults = QSResultSet.withPreparedSql(sql: self)
		
		/*********************************************************************\
		|* If for some reason we are not returning a resultSet, we need to 
		|* unlock the database, since the caller won't be able to call 
		|* [resultSet close] on this nil pointer.
		\*********************************************************************/
		if self.io.activeResults == nil
			{
			self.io.unlockDatabase()
			}
		
		return self.io.activeResults
		}
	
	// MARK: i/o
	
	/*************************************************************************\
	|* Finalise
	\*************************************************************************/
	@objc public func finaliseSql()
		{
		if self.stmt != nil
			{
			self.io.lockDatabase()
			defer
				{
				self.io.unlockDatabase()
				}
				
			let error = sqlite3_finalize(self.stmt)
			if error != SQLITE_OK
				{
				Logger.quicksilver.error("Could not finalise statement for \(self.sql)")
				}
			self.stmt = nil
			self.isPrepared = false
			
			self.io.unregisterPreparedStatement(self)
			}
		}
	
	/*************************************************************************\
	|* Finalise without locking. Only for the use of QSIO during shutdown
	\*************************************************************************/
	func finaliseSqlWithoutLock()
		{
		if self.stmt != nil
			{
			let error = sqlite3_finalize(self.stmt)
			if error != SQLITE_OK
				{
				Logger.quicksilver.error("Could not finalise statement for \(self.sql)")
				}
			self.stmt = nil
			self.isPrepared = false
			}
		}
	}
	
