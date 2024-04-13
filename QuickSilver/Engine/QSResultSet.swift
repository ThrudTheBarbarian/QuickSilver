/*****************************************************************************\
|* QuickSilver :: Engine:: QSResultSet
|*
|* QSResultSet allows iteration over the rows returned by a QSQuery. Sqlite
|* only allows one cursor at a time, so this is a modal operation (ie: you
|* must completely finish with one iteration before issuing another query). 
|*
|* Yes, this can sometimes be awkward, but it's generally a lot easier to handle
|* the specific cases where multiple queries can be issued in parallel than for
|* this class to try and provide a generic answer.
|*
|* Note that any QSResultSet must be closed after use. Do not depend on the
|* framework to close it for you...
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
class QSResultSet : NSObject
	{
	/*************************************************************************\
	|* Instance variables
	\*************************************************************************/
	private var preparedSql: QSPreparedSql!				// Sql to run with
	private var columnNameToIndexMap: [String : Int]	// map columns to indices
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public override init()
		{
		self.preparedSql = nil
		self.columnNameToIndexMap = [String:Int]()
		super.init()
		}
		
	public static func withPreparedSql(sql: QSPreparedSql) -> QSResultSet
		{
		let rs = QSResultSet.init()
		rs.preparedSql = sql
		return rs
		}
	
	/*************************************************************************\
	|* Destruction
	\*************************************************************************/
	deinit
		{
		if (self.preparedSql != nil)
			{
			Logger.quicksilver.error("Deleting resultset which preparedSQL is non-nil. This is an error")
			}
		close()
		}
		
	/*************************************************************************\
	|* For debugging
	\*************************************************************************/
	public override var description:String
		{
		let sql = self.preparedSql.sql.description
		return String(format: "\(self.className): \(sql))")
		}
	
	// MARK: i/o
	
	/*************************************************************************\
	|* close this result-set. Note that this will be called on dealloc too
	|* but that should not be depended upon
	\*************************************************************************/
	func close()
		{
		if (self.preparedSql != nil)
			{
			let io = self.preparedSql?.io
			io?.activeResults = nil
			
			self.preparedSql.close()
			self.preparedSql = nil
			
			io?.unlockDatabase()
			}
		}
	
	/*************************************************************************\
	|* advance the cursor
	\*************************************************************************/
	func next() -> Bool
		{
		var rc:Int32 = SQLITE_OK		// result code
		var retry:Bool = true			// whether to retry
		var numRetries:Int = 0			// How many retries
		
		repeat
			{
			retry 	= false
			let db 	= self.preparedSql.io
			rc		= self.preparedSql.step()
			
			if rc == SQLITE_BUSY
				{
				Logger.quicksilver.info("Got SQLITE_BUSY during QSResultSet:next")
				
				// If we've tried and tried, return false
				let timeout = self.preparedSql.io.busyRetryTimeout
				numRetries += 1
				if (timeout > 0) && (numRetries > timeout)
					{
					Logger.quicksilver.info("Giving up after \(numRetries) tries")
					return false
					}
					
				// Try waiting a little to see if we get access then
				retry = true
				Thread.sleep(forTimeInterval: 0.02)	// 20 millisecs
				}
				
			else if (rc == SQLITE_DONE) || (rc == SQLITE_ROW)
				{
				// all is sweetness and light
				}
			
			else if (rc == SQLITE_ERROR)
				{
				let msg = self.preparedSql.lastError()
				Logger.quicksilver.error("QSResultSet:next SQLITE_ERROR (\(msg))")
				db.incrementSqlErrorCount()
				return false
				}
			
			else if (rc == SQLITE_MISUSE)
				{
				let msg = self.preparedSql.lastError()
				Logger.quicksilver.error("QSResultSet:next SQLITE_MISUSE (\(msg))")
				db.incrementSqlErrorCount()
				return false
				}
			
			else
				{
				let msg = self.preparedSql.lastError()
				Logger.quicksilver.error("QSResultSet:next UNKNOWN error (\(msg))")
				db.incrementSqlErrorCount()
				return false
				}
			}
		while retry
		
		return rc == SQLITE_ROW
		}

	// MARK: metadata

	/*************************************************************************\
	|* Return the query
	\*************************************************************************/
	func query() -> String
		{
		self.preparedSql.sql
		}
		
	/*************************************************************************\
	|* Return the column index for a given column's name
	\*************************************************************************/
	func columnIndex(forName name:String) -> Int32!
		{
		if self.columnNameToIndexMap.isEmpty
			{
			self.setupColumnNames()
			}
		
		if let num = self.columnNameToIndexMap[name]
			{
			return Int32(num)
			}
		
		if let num = self.columnNameToIndexMap[name.lowercased()]
			{
			return Int32(num)
			}
		
		if let num = self.columnNameToIndexMap[name.uppercased()]
			{
			return Int32(num)
			}
		
		Logger.quicksilver.error("Cannot find column named (\(name))")
		return nil
		}

	/*************************************************************************\
	|* Return the column index map
	\*************************************************************************/
	func columnNameIndexMap() -> [String : Int]
		{
		if self.columnNameToIndexMap.isEmpty
			{
			self.setupColumnNames()
			}
		return self.columnNameToIndexMap
		}
		
	// MARK: column values

	/*************************************************************************\
	|* Return an integer for a given column name
	\*************************************************************************/
	func intForColumn(named name:String) -> Int!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return Int(sqlite3_column_int(self.preparedSql.stmt, idx))
			}
		return nil
		}

	/*************************************************************************\
	|* Return an integer for a given column index
	\*************************************************************************/
	func intForColumn(withIndex idx:Int) -> Int!
		{
		if (idx < 0) || (idx > self.preparedSql.columnCount())
			{
			return nil
			}
		return Int(sqlite3_column_int(self.preparedSql.stmt, Int32(idx)))
		}


	/*************************************************************************\
	|* Return an int64 for a given column name
	\*************************************************************************/
	func int64ForColumn(named name:String) -> Int64!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return Int64(sqlite3_column_int64(self.preparedSql.stmt, idx))
			}
		return nil
		}

	/*************************************************************************\
	|* Return an int64 for a given column index
	\*************************************************************************/
	func int64ForColumn(withIndex idx:Int) -> Int64!
		{
		if (idx < 0) || (idx > self.preparedSql.columnCount())
			{
			return nil
			}
		return Int64(sqlite3_column_int64(self.preparedSql.stmt, Int32(idx)))
		}


	/*************************************************************************\
	|* Return a bool for a given column name
	\*************************************************************************/
	func boolForColumn(named name:String) -> Bool!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return sqlite3_column_int(self.preparedSql.stmt, idx) == 0
					? false : true
			}
		return nil
		}

	/*************************************************************************\
	|* Return a bool for a given column index
	\*************************************************************************/
	func boolForColumn(withIndex idx:Int) -> Bool!
		{
		if (idx < 0) || (idx > self.preparedSql.columnCount())
			{
			return nil
			}
		return sqlite3_column_int(self.preparedSql.stmt, Int32(idx)) == 0
				? false : true
		}


	/*************************************************************************\
	|* Return a double for a given column name
	\*************************************************************************/
	func doubleForColumn(named name:String) -> Double!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return Double(sqlite3_column_double(self.preparedSql.stmt, idx))
			}
		return nil
		}

	/*************************************************************************\
	|* Return a double for a given column index
	\*************************************************************************/
	func doubleForColumn(withIndex idx:Int) -> Double!
		{
		if (idx < 0) || (idx > self.preparedSql.columnCount())
			{
			return nil
			}
		return Double(sqlite3_column_double(self.preparedSql.stmt, Int32(idx)))
		}


	/*************************************************************************\
	|* Return a string for a given column name
	\*************************************************************************/
	func stringForColumn(named name:String) -> String!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return String(cString: sqlite3_column_text(self.preparedSql.stmt, idx))
			}
		return nil
		}

	/*************************************************************************\
	|* Return a string for a given column index
	\*************************************************************************/
	func stringForColumn(withIndex idx:Int) -> String!
		{
		if (idx < 0) || (idx > self.preparedSql.columnCount())
			{
			return nil
			}
		return String(cString: sqlite3_column_text(self.preparedSql.stmt, Int32(idx)))
		}


	/*************************************************************************\
	|* Return a number for a given column index
	\*************************************************************************/
	func numberForColumn(withIndex idx:Int) -> NSNumber!
		{
		let stmt = self.preparedSql.stmt
		let indx = Int32(idx)
		
		switch (sqlite3_column_type(self.preparedSql.stmt, Int32(idx)))
			{
			case SQLITE_BLOB,
				 SQLITE_TEXT,
				 SQLITE_INTEGER:
					return NSNumber.init(value:sqlite3_column_int64(stmt, indx))
			
			case SQLITE_FLOAT:
					return NSNumber.init(value:sqlite3_column_double(stmt, indx))
				
			default:
					return nil
			}
		}

	/*************************************************************************\
	|* Return a number for a given column name
	\*************************************************************************/
	func numberForColumn(named name:String) -> NSNumber!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return self.numberForColumn(withIndex: Int(idx))
			}
		return nil
		}
		
	/*************************************************************************\
	|* Return a date for a given column name
	\*************************************************************************/
	func dateForColumn(named name:String) -> Date!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return self.dateForColumn(withIndex:Int(idx))
			}
		return nil
		}

	/*************************************************************************\
	|* Return a date for a given column index
	\*************************************************************************/
	func dateForColumn(withIndex idx:Int) -> Date!
		{
		var date:Date! = nil
		
		if (idx < 0) || (idx > self.preparedSql.columnCount())
			{
			return nil
			}
		let dt = self.doubleForColumn(withIndex: idx) as TimeInterval
		
		// Check to see this wasn't auto-converted from NULL for us...
		if (dt < 0.001) && (dt > -0.001)
			{
			if (sqlite3_column_type(self.preparedSql.stmt, Int32(idx)) != SQLITE_NULL)
				{
				date = Date(timeIntervalSince1970: dt)
				}
			}
		else
			{
			date = Date(timeIntervalSince1970: dt)
			}
			
		return date
		}


	/*************************************************************************\
	|* Return a data for a given column name
	\*************************************************************************/
	func dataForColumn(named name:String) -> Data!
		{
		if let idx = self.columnIndex(forName: name)
			{
			return self.dataForColumn(withIndex:Int(idx))
			}
		return nil
		}

	/*************************************************************************\
	|* Return a string for a given column index
	\*************************************************************************/
	func dataForColumn(withIndex idx:Int) -> Data!
		{
		if (idx < 0) || (idx > self.preparedSql.columnCount())
			{
			return nil
			}
		
		let indx:Int32 = Int32(idx)
		var data:Data! = nil
		let dataSize   = sqlite3_column_bytes(self.preparedSql.stmt, indx)
		if dataSize > 0
			{
			if let blob = sqlite3_column_blob(self.preparedSql.stmt, indx)
				{
				data = Data.init(bytes: blob, count: Int(dataSize))
				}
			}
		return data
		}

	// MARK: Private methods
	
	/*************************************************************************\
	|* Map columns to names
	\*************************************************************************/
	private func setupColumnNames()
		{
		let numCols = self.preparedSql.columnCount()
		
		for idx:Int in 0..<numCols
			{
			if let colName = self.preparedSql.columnName(forIndex: idx)
				{
				// Add column name, lowercase version and uppercase version
				self.columnNameToIndexMap[colName] = idx
				self.columnNameToIndexMap[colName.uppercased()] = idx
				self.columnNameToIndexMap[colName.lowercased()] = idx
				}
			}
		}
	}
