/*****************************************************************************\
|* QuickSilver :: DataModel :: Entity :: QSEntity
|*
|* A QSEntity is the handler class for a given table. The entity caches models
|* in RAM, and is the factory object for creation of models for that table.
|*
|* To be handled by a QsEntity, a table MUST HAVE:
|*	- a column named 'id'			which is an integer primary key
|*  - a column named 'uuid'			which is a string unique global identifier
|*  - a column named 'modified'		which is a date of last modification
|*  - a column named 'created'		which is a date of first creation
|*
|* Created by ThrudTheBarbarian on 4/8/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

//@_implementationOnly
import Foundation
import OSLog

/*****************************************************************************\
|* Class definition
\*****************************************************************************/
open class QSEntity : NSObject
	{
	/*************************************************************************\
	|* Interaction
	\*************************************************************************/
	public let engine: QSEngine							// Engine for the db
	public var isActive: Bool							// Are we active
	
	/*************************************************************************\
	|* Entity definition
	\*************************************************************************/
	public var tableName: String						// Name of table in DB
	public var columnNames: [String]					// Entity Column names
	public var columnTypes: [QSColumn]					// Types of each column
	public var columnTypesByName: [String : QSColumn]	// Column objects by name

	/*************************************************************************\
	|* Model handling
	\*************************************************************************/
	private var allModelsLoaded: Bool					// Models all loaded
	private var persistSql: QSPreparedSql!				// SQL to persist model
	private var modelIdLock: NSLock						// Lock, protect nextId
	
	/*************************************************************************\
	|* Caching
	\*************************************************************************/
	var modelsByUuid: NSMutableDictionary				// Cache: UUID of models
	var models : NSMutableSet							// Actual cache
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public init(withTableName tableName: String, inEngine engine: QSEngine)
		{
		// Store the instance variables and set to be active
		self.engine 			= engine
		self.isActive			= true
		
		// Table management
		self.tableName			= tableName
		self.modelIdLock		= NSLock()
		
		// Create storage for the column metadata
		self.columnNames		= [String]()
		self.columnTypes		= [QSColumn]()
		self.columnTypesByName	= [String : QSColumn]()
		
		// Create the cache
		self.modelsByUuid 		= NSMutableDictionary.init()
		self.models				= NSMutableSet.init()
		self.allModelsLoaded 	= false
		
		self.persistSql			= nil
		
		super.init()
		}
		
		
// MARK: Schema management

	/*************************************************************************\
	|* Schema management : Add a column
	\*************************************************************************/
	public
	func addColumn(name:String,
				   type:QSColumnType,
				options:QSColumnOptions = QSColumnOptions.none)
		{
		let lcName = name.lowercased()
		self.columnNames.append(lcName)
		
		let column = QSColumn.with(name:lcName, type:type, options:options)
		self.columnTypes.append(column)
		
		self.columnTypesByName[name] = column
		}
		
	/*************************************************************************\
	|* Schema management : return a column by name
	\*************************************************************************/
	public
	func column(forName name:String) -> QSColumn!
		{
		return self.columnTypesByName[name]
		}
		
	/*************************************************************************\
	|* Schema management : return a column-type by name
	\*************************************************************************/
	public
	func columnType(forName name:String) -> QSColumnType
		{
		let col = self.column(forName: name)
		return col?.type ?? QSColumnType.Varchar
		}

	/*************************************************************************\
	|* Schema management : Hang around waiting for any outstanding writes
	\*************************************************************************/
	public
	func waitForOutstandingWrites()
		{
		self.engine.io.bgWriteQueue.waitForOutstandingOperations()
		}
		
// MARK: Utility functions

	/*************************************************************************\
	|* Utility functions: make a list of srings, separated by commas
	\*************************************************************************/
	static func makeList(_ strings:[String]) -> String
		{
		let array = strings as NSArray
		return array.componentsJoined(by: ",")
		}

	/*************************************************************************\
	|* Utility functions: return the correct object for a given column type
	\*************************************************************************/
	public
	static func objectFrom(resultSet rs:QSResultSet,
						  withType type:QSColumnType,
					 forColumnIndex idx:Int) -> AnyObject!
		{
		var obj:AnyObject! = nil
		
		switch (type)
			{
			case QSColumnType.Varchar,
				 QSColumnType.VarcharPK:
					obj = rs.stringForColumn(withIndex: idx) as NSString
			
			case QSColumnType.Integer,
				 QSColumnType.IntegerPK,
				 QSColumnType.Bool:
					obj = rs.int64ForColumn(withIndex: idx) as NSNumber
			
			case QSColumnType.Decimal:
					obj = rs.doubleForColumn(withIndex: idx) as NSNumber
			
			case QSColumnType.Timestamp,
				 QSColumnType.TimestampAsTimeInterval:
					obj = rs.dateForColumn(withIndex: idx) as NSDate
			
			case QSColumnType.DataBlob:
					obj = rs.dataForColumn(withIndex: idx) as NSData

			default:
				Logger.quicksilver.log("Unknown object type \(String(describing:type)) in db")
					obj = rs.stringForColumn(withIndex: idx) as NSString
			}
		
		return obj
		}
		
	/*************************************************************************\
	|* Utility functions: Generate the SELECT part of a SQL query
	\*************************************************************************/
	func selectSqlStringForColumn(_ col:String) -> String
		{
		return col.isEmpty
			? ""
			: String(format: "SELECT %@ FROM %@", col, self.tableName)
		}
		
	/*************************************************************************\
	|* Utility functions: Generate the SELECT DISTINCT part of a SQL query
	\*************************************************************************/
	func selectDistinctSqlStringForColumn(_ col:String) -> String
		{
		return col.isEmpty
			? ""
			: String(format: "SELECT DISTINCT %@ FROM %@", col, self.tableName)
		}
		
	/*************************************************************************\
	|* Utility functions: Add object or NULL to argument list
	\*************************************************************************/
	static func add(object obj:Any!, to array:inout [AnyObject])
		{
		array.append(QSPreparedSql.convertArg(obj))
		}
		
// MARK: Low-level interface

	/*************************************************************************\
	|* Low-level : Generate a preparedSql object for a select operation
	\*************************************************************************/
	public
	func sqlForSelect(of cols:String, where condition:String) -> QSPreparedSql
		{
		let sql = condition.isEmpty
				? self.selectSqlStringForColumn(cols)
				: String(format:"%@ WHERE %@",
						 self.selectSqlStringForColumn(cols), condition)
	
		return QSPreparedSql.init(withIo: self.engine.io,
									 sql: sql,
								 prepare: false)
		}

	/*************************************************************************\
	|* Low-level : Generate a preparedSql object for a select operation
	\*************************************************************************/
	public
	func sqlForSelect(where condition:String = "") -> QSPreparedSql
		{
		return self.sqlForSelect(of:"*", where:condition)
		}

	/*************************************************************************\
	|* Low-level : Generate a preparedSql object for a DISTINCT select op
	\*************************************************************************/
	public
	func sqlForSelect(distinct cols:String, where condition:String) -> QSPreparedSql
		{
		let sql = condition.isEmpty
				? self.selectDistinctSqlStringForColumn(cols)
				: String(format:"%@ WHERE %@",
						 self.selectDistinctSqlStringForColumn(cols), condition)
	
		return QSPreparedSql.init(withIo: self.engine.io,
									 sql: sql,
								 prepare: false)
		}
	
	/*************************************************************************\
	|* Low-level : Generate a preparedSql object for a delete operation
	\*************************************************************************/
	public
	func sqlForDelete(where condition:String = "") -> QSPreparedSql
		{
		let sql = condition.isEmpty
				? String(format:"DELETE FROM %@", self.tableName)
				: String(format:"DELETE FROM %@ WHERE %@",
						 self.tableName, condition)
	
		return QSPreparedSql.init(withIo: self.engine.io,
									 sql: sql,
								 prepare: false)
		}

	/*************************************************************************\
	|* Low-level : Generate a preparedSql object for a modify operation
	\*************************************************************************/
	public
	func sqlForModify(of col:String, where condition:String) -> QSPreparedSql
		{
		let sql = condition.isEmpty
				? String(format: "UPDATE %@ SET %@=?, %@=?",
								 self.tableName,
								 col,
								 QSModel.modifiedDateColumn)
				: String(format: "UPDATE %@ SET %@=?, %@=? WHERE %@",
								 self.tableName,
								 col,
								 QSModel.modifiedDateColumn,
								 condition)
		
		return QSPreparedSql.init(withIo: self.engine.io,
										  sql: sql,
									  prepare: false)
		}

	/*************************************************************************\
	|* Low-level : Generate a preparedSql object for a modify by dbid
	\*************************************************************************/
	public
	func sqlForModifyByUuid(of col:String) -> QSPreparedSql
		{
		return self.sqlForModify(of:col, where:QSModel.uuidEquals)
		}
		
	/*************************************************************************\
	|* Low-level : Generate a preparedSql object for an update operation
	\*************************************************************************/
	public
	func sqlForUpdate(of col:String, where condition:String) -> QSPreparedSql
		{
		let sql = condition.isEmpty
				? String(format: "UPDATE %@ SET %@=?",
								 self.tableName, col)
				: String(format: "UPDATE %@ SET %@=? WHERE %@",
								 self.tableName, col, condition)
		
		return QSPreparedSql.init(withIo: self.engine.io,
										  sql: sql,
									  prepare: false)
		}
	
	public
	func sqlForUpdateByUuid(of cols:String) -> QSPreparedSql
		{
		return self.sqlForUpdate(of: cols, where: QSModel.uuidEquals)
		}

//	/*************************************************************************\
//	|* Low-level : Select columns with a WHERE clause, varargs version
//	\*************************************************************************/
//	func selectObjects(cols:[String], whereSql:String, _ args:AnyObject?...) -> [AnyObject]
//		{
//		let filtered = QSPreparedSql.filter(args:args, for:whereSql)
//		return self.selectObjects(cols:cols, whereSql:whereSql, args:filtered)
//		}
//	
//	/*************************************************************************\
//	|* Low-level : Select columns with a WHERE clause, array version
//	\*************************************************************************/
//	func selectObjects(cols:[String], whereSql:String, args:[AnyObject]) -> [AnyObject]
//		{
//		}
	
	/*************************************************************************\
	|* Low-level : return an array of objects of a given type for the query
	\*************************************************************************/
	static func arrayOf(type:QSColumnType, forQuery sql:QSPreparedSql, withArgs args:[AnyObject]) -> [AnyObject]
		{
		var results = [AnyObject]()
		
		if let rs = sql.query(with:args)
			{
			defer
				{
				rs.close()
				}
				
			while rs.next()
				{
				if let object = QSEntity.objectFrom(resultSet:rs,
													withType:type,
											  forColumnIndex:0)
					{
					results.append(object)
					}
				}
			}
		return results
		}
		
	/*************************************************************************\
	|* Low-level : Create a table if one doesn't already exist, using the
	|* column types registered with the entity
	\*************************************************************************/
	public
	func createTableIfNotExists() -> Bool
		{
		let prefix = "CREATE TABLE IF NOT EXISTS \(self.tableName)\n\t(\n"
		var cols   = [String]()
		
		for col in self.columnTypes
			{
			var fmt = ""
			
			switch col.type
				{
				case .Integer,.Bool:
					fmt = "\t%@\tINTEGER"
				
				case .IntegerPK:
					fmt = "\t%@\tINTEGER PRIMARY KEY"
				
				case .Decimal:
					fmt = "\t%@\tREAL"
				
				case .Varchar:
					fmt = "\t%@\tVARCHAR";

				case .VarcharPK:
					fmt = "\t%@\tVARCHAR PRIMARY KEY"
				
				case .Timestamp,.TimestampAsTimeInterval:
					fmt = "\t%@\tTIMESTAMP"
				
				case .DataBlob:
					fmt = "\t%@\tBLOB"
				
				default:
					Logger.quicksilver.error("Unknown column type in create table")
					return false
				}
			
			if !fmt.isEmpty
				{
				cols.append(String(format:fmt, col.name))
				}
			else
				{
				Logger.quicksilver.error("column \(col.name) is of unknown type in CREATE TABLE")
				}
			}
		
		let sql = String(format:"%@%@\n\t)\n",
											prefix,
											cols.joined(separator: ",\n"))
		let psql = QSPreparedSql.init(withIo: self.engine.io,
										 sql: sql,
									 prepare: false)
	
		let status = psql.update();
		return status;
		}
	
	// MARK: model retrieval
	
	/*************************************************************************\
	|* Model retrieval : return the model for a UUID in this entities table
	\*************************************************************************/
	public
	func modelWith(uuid:String) -> QSModel!
		{
		if let model = self.cachedModelWith(uuid: uuid)
			{
			return model
			}
		
		// Not in the cache..
		var model:QSModel! = nil
		let psql = self.sqlForSelect(where: "uuid=?")
		
		// Make sure we're self-serialised
		self.waitForOutstandingWrites()
		
		let args = [uuid as NSString]
		if let rs   = psql.query(with: args)
			{
			defer
				{
				rs.close()
				}
			
			let models = self.loadModelsFrom(resultSet: rs)
			if models.count > 0
				{
				model = models.first
				self.didCreate(model: model)
				}
			}
		else
			{
			Logger.quicksilver.log("Could not create model for uuid \(uuid)")
			}
			
		return model
		}
	
	/*************************************************************************\
	|* Model retrieval : return the models for UUIDs in this entities table
	\*************************************************************************/
	public
	func modelsWith(uuids:[String]) -> Set<QSModel>
		{
		var models = Set<QSModel>()
		if uuids.count > 0
			{
			let psql = self.sqlForSelect(where: "uuid IN (#?)")
			
			// Make sure we're self-serialised
			self.waitForOutstandingWrites()
			
			let args = [uuids as NSArray]
			if let rs   = psql.query(with: args)
				{
				defer
					{
					rs.close()
					}
				
				models = self.loadModelsFrom(resultSet: rs)
				if models.count > 0
					{
					for model in models
						{
						self.didCreate(model: model)
						}
					}
				}
			else
				{
				Logger.quicksilver.log("Could not create model for uuids \(uuids)")
				}
			}
			
		return models
		}
	
	/*************************************************************************\
	|* Model retrieval : return models based on a WHERE clause
	\*************************************************************************/
	public
	func models(where sql:String!, args:[Any?]) -> Set<QSModel>!
		{
		var result:Set<QSModel>!	= nil
		let psql 					= self.sqlForSelect(where: sql)
		let converted				= QSPreparedSql.filter(all:args)
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()
		
		if let rs = psql.query(with: converted)
			{
			defer
				{
				rs.close()
				}
				
			result = Set(self.loadModelsFrom(resultSet: rs))
			}
		
		if sql == nil
			{
			self.allModelsLoaded = true
			}
			
		for model in result
			{
			self.didCreate(model: model)
			}
			
		return result
		}
	
	/*************************************************************************\
	|* Model retrieval : return models based on a WHERE clause, varargs style
	\*************************************************************************/
	public
	func models(where sql:String!, _ args:Any?...) -> Set<QSModel>!
		{
		return self.models(where:sql, args:args)
		}
		
		
	/*************************************************************************\
	|* Model retrieval : return any model based on a WHERE clause
	\*************************************************************************/
	public
	func anyModel(where sql:String!, args:[Any?]) -> QSModel!
		{
		var result:Set<QSModel>!	= nil
		let psql 					= self.sqlForSelect(where: sql)
		let converted				= QSPreparedSql.filter(all:args)
		
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()
		
		if let rs = psql.query(with: converted)
			{
			defer
				{
				rs.close()
				}
				
			result = Set(self.loadModelsFrom(resultSet: rs, limit:1))
			}
		
		if result.count > 0
			{
			if let model = result.first
				{
				self.didCreate(model: model)
				return model
				}
			}
			
		return nil
		}
	
	/*************************************************************************\
	|* Model retrieval : return entities based on a WHERE clause, varargs style
	\*************************************************************************/
	public
	func anyModel(where sql:String!, _ args:Any?...) -> QSModel!
		{
		return self.anyModel(where:sql, args:args)
		}
	
	/*************************************************************************\
	|* Model retrieval : get the UUIDs for a WHERE clause
	\*************************************************************************/
	public
	func modelUuids(where sql:String!, args:[Any?]) -> Set<String>
		{
		var result:Set<String> = Set<String>()
		let converted			= QSPreparedSql.filter(all:args)
		let psql = self.sqlForSelect(distinct:QSModel.uuidColumn, where:sql)
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()
		
		let rows = QSEntity.arrayOf(type: QSColumnType.Varchar,
								forQuery: psql,
								withArgs: converted)
		
		for row in rows
			{
			if row is String
				{
				result.insert(row as! String)
				}
			else if row is NSString
				{
				result.insert(row as! String)
				}
			}
		return result
		}
		
	/*************************************************************************\
	|* Model retrieval : get the UUIDs for a WHERE clause, varargs style
	\*************************************************************************/
	public
	func modelUuids(where sql:String!, _ args:Any?...) -> Set<String>
		{
		return self.modelUuids(where:sql, args:args)
		}
	
	/*************************************************************************\
	|* Model retrieval : get the modelIds for a WHERE clause
	\*************************************************************************/
	public
	func uuids(where sql:String!, args:[Any?]) -> Set<String>
		{
		var result:Set<String> 		= Set<String>()
		let converted				= QSPreparedSql.filter(all:args)
		
		let psql = self.sqlForSelect(distinct:QSModel.uuidColumn, where:sql)
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()
		
		let rows = QSEntity.arrayOf(type: QSColumnType.Varchar,
							    forQuery: psql,
								withArgs: converted)
		
		for row in rows
			{
			if row is NSNumber
				{
				result.insert(row as! String)
				}
			}
		return result
		}
		
	/*************************************************************************\
	|* Model retrieval : get the modelIds for a WHERE clause, varargs style
	\*************************************************************************/
	public
	func modelIds(where sql:String!, _ args:Any?...) -> Set<String>
		{
		return self.uuids(where:sql, args:args)
		}
		
	/*************************************************************************\
	|* Model retrieval : count of models with a WHERE clause
	\*************************************************************************/
	public
	func countOfModels(where sql:String!, args:[Any?]) -> Int64
		{
		var result:Int64 = 0
		let psql = self.sqlForSelect(of: "COUNT(*)", where: sql)
		let converted = QSPreparedSql.filter(all:args)
		
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()

		if let rs = psql.query(with: converted)
			{
			defer
				{
				rs.close()
				}
			result = rs.int64ForColumn(withIndex: 0)
			}
		return result
		}

	/*************************************************************************\
	|* Model retrieval : count of models with a WHERE clause, varargs style
	\*************************************************************************/
	public
	func countOfModels(where sql:String!, _ args:Any?...) -> Int64
		{
		return self.countOfModels(where:sql, args:args)
		}
	
		
	/*************************************************************************\
	|* Model retrieval : check if model exists with a WHERE clause
	\*************************************************************************/
	public
	func modelExists(where sql:String, args:[Any?]) -> Bool
		{
		var exists 		= false
		let psql 		= self.sqlForSelect(of: "rowId", where: sql)
		let converted	= QSPreparedSql.filter(all:args)
		
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()

		if let rs = psql.query(with: converted)
			{
			defer
				{
				rs.close()
				}
			exists = rs.next()
			}
			
		return exists
		}

	/*************************************************************************\
	|* Model retrieval : check if model exists with a WHERE clause, varargs
	\*************************************************************************/
	public
	func modelExists(where sql:String, _ args:Any?...) -> Bool
		{
		return self.modelExists(where: sql, args: args)
		}

		
	/*************************************************************************\
	|* Model retrieval : get DISTINCT results using WHERE
	\*************************************************************************/
	func select(distinct colName:String, where sql:String!, args:[Any?]) -> [AnyObject]
		{
		var colType		= QSColumnType.Varchar
		let converted	= QSPreparedSql.filter(all:args)
		
		
		let psql 		= self.sqlForSelect(distinct:colName, where:sql)
		if let column	= self.column(forName:colName)
			{
			colType = column.type
			}
			
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()
		
		return QSEntity.arrayOf(type:colType,
							forQuery:psql,
							withArgs:converted)
		}

	/*************************************************************************\
	|* Model retrieval : get DISTINCT results using WHERE clause, varargs
	\*************************************************************************/
	public
	func select(distinct colName:String, where sql:String, _ args:Any?...) -> [AnyObject]
		{
		return self.select(distinct:colName, where:sql, args:args)
		}

	// MARK: model persistence

	/*************************************************************************\
	|* Model persistence : column update
	\*************************************************************************/
	public func addOperation(_ op:Operation)
		{
		self.engine.io.bgWriteQueue.addOperation(op)
		}
		
	/*************************************************************************\
	|* Model persistence : column update
	\*************************************************************************/
	public
	func update(column:String, to value:AnyObject!, where sql:String!, args:[Any?])
		{
		let psql 		= self.sqlForUpdate(of:column, where:sql)
		var mutableArgs = args
		
		
		mutableArgs.insert(value == nil ? NSNull.init() : value, at:0)
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()
		
		self.executeUpdate(sql:psql, withArgs:args)
		}

	/*************************************************************************\
	|* Model persistence : column update, varargs version
	\*************************************************************************/
	public
	func update(column:String, to value:AnyObject!, where sql:String!, _ args:Any?...)
		{
		return self.update(column:column, to:value, where:sql, args:args)
		}
		
	/*************************************************************************\
	|* Model persistence : Create the SQL to use
	\*************************************************************************/
	public
	func persistModelSql() -> QSPreparedSql
		{
		if let persistSql = self.persistSql
			{
			return persistSql
			}
		
		let sql = String(format:"INSERT INTO %@(%@) VALUES(%@)",
					self.tableName,
					QSEntity.makeList(self.columnNames),
					QSPreparedSql.questionMarkList(self.columnNames.count))
		
		objc_sync_enter(self)
		self.persistSql = QSPreparedSql.init(withIo: self.engine.io,
												sql: sql,
											prepare: self.isActive)
		objc_sync_exit(self)
		
		return self.persistSql
		}
	
	/*************************************************************************\
	|* Model persistence : Save the model to the DB. Entities need to override
	\*************************************************************************/
	open
	func persist(model:QSModel)
		{
		Logger.quicksilver.error("Class \(String(describing: self)) has not implemented persist(model:)")
		}
	
	/*************************************************************************\
	|* Model persistence : Update the model to the DB. Entities need to override
	\*************************************************************************/
	open
	func update(model:QSModel)
		{
		Logger.quicksilver.error("Class \(String(describing: self)) has not implemented update(model:)")
		}

	/*************************************************************************\
	|* Model persistence : Update the model using prepared SQL or string
	\*************************************************************************/
	public
	func executeUpdate(sql:Any, withArgs args:[Any?])
		{
		if (sql is String) || (sql is QSPreparedSql)
			{
			let queue = self.engine.io.bgWriteQueue;
			let converted = QSPreparedSql.filter(all:args)
		

			let op    = QSSqlOperation.init(with:engine.io,
											 sql:sql,
											args:converted)
			
			queue.add(operation:op)
			}
		}

	/*************************************************************************\
	|* Model persistence : Update the model, varargs version
	\*************************************************************************/
	public
	func executeUpdate(_ sql:Any, _ args:AnyObject?...)
		{
		if (sql is String)
			{
			let psql = QSPreparedSql.init(withIo:engine.io,
										     sql:sql as! String,
										 prepare:false)
			self.executeUpdate(sql:psql, withArgs:args)
			}
		else if (sql is QSPreparedSql)
			{
			let psql = sql as! QSPreparedSql
			self.executeUpdate(sql:psql, withArgs:args)
			}
		else
			{
			Logger.quicksilver.error("Attempt to pass sql as \(String(describing: sql)) in executeUpdate")
			}
		}
		
	/*************************************************************************\
	|* Model persistence : Override to implement behaviour in subclass
	\*************************************************************************/
	open
	func willSaveEngine()
		{}
	
	open
	func didSaveEngine()
		{}
		
		
	// MARK: model deletion
	
	/*************************************************************************\
	|* Delete : subclasses can override this to implement behaviour
	\*************************************************************************/
	open
	func willDelete(model:QSModel)
		{}
	
	/*************************************************************************\
	|* Delete : set the deleted flag and remove the model from the cache
	\*************************************************************************/
	public
	func deleteOfModelWith(uuid:String)
		{
		if let model = self.cachedModelWith(uuid:uuid)
			{
			model.isDeleted = true
			self.uncache(model: model)
			}
		}
		
	/*************************************************************************\
	|* Delete : Delete a set of models
	\*************************************************************************/
	public
	func deleteOfModelsWith(uuids:any Collection)
		{
		for uuid in uuids
			{
			if let uuid = uuid as? String
				{
				self.deleteOfModelWith(uuid:uuid)
				}
			}
		}
		
	/*************************************************************************\
	|* Delete : Get rid of a model
	\*************************************************************************/
	public
	func delete(model:QSModel)
		{
		/*********************************************************************\
		|* Mark the model object as deleted and remove it from the model cache 
		\*********************************************************************/
		self.willDelete(model: model)
		self.deleteOfModelWith(uuid:model.uuid)
		
		/*********************************************************************\
		|* Remove from the persistent store
		\*********************************************************************/
		let sql 	= self.sqlForDelete(where: QSModel.uuidEquals)
		self.executeUpdate(sql:sql, withArgs:[model.uuid])
		}
		
	/*************************************************************************\
	|* Delete : Get rid of a collection of models
	\*************************************************************************/
	public
	func delete(models:any Collection)
		{
		if (models.count > 0)
			{
			var uuids = [String]()
			
			for model in models
				{
				if let model = model as? QSModel
					{
					uuids.append(model.uuid)
					self.willDelete(model: model)
					}
				}
			self.delete(uuids:uuids)
			}
		}
		
	/*************************************************************************\
	|* Delete : Get rid of an array of modelIds
	\*************************************************************************/
	public
	func delete(uuids:[String])
		{
		if (uuids.count > 0)
			{
			self.deleteOfModelsWith(uuids:uuids)

			let sql 	= self.sqlForDelete(where:"uuid in (#?)")
			let args 	= [uuids]
			self.executeUpdate(sql:sql, withArgs:args)
			}
		}
		
	/*************************************************************************\
	|* Delete : Get rid of models using a WHERE clause
	\*************************************************************************/
	public
	func deleteModels(where sql:String, args:[Any?]) -> Bool
		{
		var deletedModels = false
		
		/*********************************************************************\
		|* Make sure we're self-serialised
		\*********************************************************************/
		self.waitForOutstandingWrites()
		
		let sql 	= self.sqlForSelect(distinct:QSModel.uuidColumn, where:sql)
		let objects = QSPreparedSql.filter(all: args)
		let dbUuids = QSEntity.arrayOf(type: QSColumnType.Integer,
								   forQuery: sql,
								   withArgs: objects)
		
		var uuids = [String]()
		for uuid in dbUuids
			{
			if let uuid = uuid as? String
				{
				uuids.append(uuid)
				}
			}
			
		if uuids.count > 0
			{
			self.delete(uuids:uuids)
			deletedModels = true
			}
		
		return deletedModels
		}
		
	/*************************************************************************\
	|* Delete : Get rid of models using a WHERE clause, varargs version
	\*************************************************************************/
	public
	func deleteModels(where sql:String, _ args:Any?...) -> Bool
		{
		return self.deleteModels(where:sql, args:args)
		}
		
		
	// MARK: model creation
	
	/*************************************************************************\
	|* Model creation : subclasses must override this to implement behaviour
	\*************************************************************************/
	open
	func loadModelFrom(resultSet results:QSResultSet) -> QSModel!
		{
		Logger.quicksilver.error("loadModelFrom(resultSet:) missing in \(self.className)")
		return nil
		}
		
	/*************************************************************************\
	|* Model creation : Create models from a result-set
	\*************************************************************************/
	public
	func loadModelsFrom(resultSet results:QSResultSet, limit:Int = 0) -> Set<QSModel>
		{
		var models = Set<QSModel>()
		
		while results.next()
			{
			if let uuid = results.stringForColumn(withIndex: 0)
				{
				if let model = self.cachedModelWith(uuid: uuid)
					{
					models.insert(model)
					}
				else
					{
					if let model = self.loadModelFrom(resultSet: results)
						{
						self.cacheModel(model)
						models.insert(model)
						}
					}
				}
			if (limit > 0) && models.count > limit
				{
				break
				}
			}
		
		return models
		}

	/*************************************************************************\
	|* Model creation : Create models from a result-set of dbids
	\*************************************************************************/
	public
	func loadModelsFrom(resultSetOfUuids rs:QSResultSet, limit:Int = 0) -> [QSModel]
		{
		var models 		= [QSModel]()
		var uncachedIds	= Set<String>()
		
		while rs.next()
			{
			if let uuid = rs.stringForColumn(withIndex: 0)
				{
				if let model = self.cachedModelWith(uuid:uuid)
					{
					models.append(model)
					if (limit > 0) && models.count > limit
						{
						break
						}
					}
				else
					{
					uncachedIds.insert(uuid)
					}
				}
			}
		
		if !uncachedIds.isEmpty
			{
			if (limit == 0) || (models.count < limit)
				{
				let additionalModels = self.modelsWith(uuids:Array(uncachedIds))
				models.append(contentsOf: additionalModels)
				}
			}
	
		return models
		}
		
	/*************************************************************************\
	|* Model creation : override the call to add functionality on db open
	\*************************************************************************/
	open
	func didOpenDatabase()
		{}
	
	/*************************************************************************\
	|* Model creation : override the call to add functionality on model create
	\*************************************************************************/
	open
	func didCreate(model:QSModel)
		{}
	
	// MARK: data access

	/*************************************************************************\
	|* Data access : Get a string for a model's column
	\*************************************************************************/
	public
	func stringFor(column name:String, forUuid uuid:String) -> String!
		{
		let sql = self.sqlForSelect(of: name, where: QSModel.uuidEquals)
		self.waitForOutstandingWrites()
		
		return self.engine.io.stringFor(sql, uuid as NSString)
		}

	/*************************************************************************\
	|* Data access : Get a number for a model's column
	\*************************************************************************/
	public
	func numberFor(column name:String, forUuid uuid:String) -> NSNumber!
		{
		let sql = self.sqlForSelect(of: name, where: QSModel.uuidEquals)
		self.waitForOutstandingWrites()
		
		return self.engine.io.numberFor(sql, uuid as NSString)
		}

	/*************************************************************************\
	|* Data access : Get a date for a model's column
	\*************************************************************************/
	public
	func dateFor(column name:String, forUuid uuid:String) -> Date!
		{
		let sql = self.sqlForSelect(of: name, where: QSModel.uuidEquals)
		self.waitForOutstandingWrites()
		
		return self.engine.io.dateFor(sql, uuid as NSString)
		}

	/*************************************************************************\
	|* Data access : Get a blob for a model's column
	\*************************************************************************/
	public
	func dataFor(column name:String, forUuid uuid:String) -> Data!
		{
		let sql = self.sqlForSelect(of: name, where: QSModel.uuidEquals)
		self.waitForOutstandingWrites()
		
		return self.engine.io.dataFor(sql, uuid as NSString)
		}

	/*************************************************************************\
	|* Data access : Get a bool for a model's column
	\*************************************************************************/
	public
	func boolFor(column name:String, forUuid uuid:String) -> Bool!
		{
		let sql = self.sqlForSelect(of: name, where: QSModel.uuidEquals)
		self.waitForOutstandingWrites()
		
		return self.engine.io.boolFor(sql, uuid as NSString)
		}

	/*************************************************************************\
	|* Data access : Get an int64 for a model's column
	\*************************************************************************/
	public
	func int64For(column name:String, forUuid uuid:String) -> Int64!
		{
		let sql = self.sqlForSelect(of: name, where: QSModel.uuidEquals)
		self.waitForOutstandingWrites()
		
		return self.engine.io.int64For(sql, uuid as NSString)
		}

	/*************************************************************************\
	|* Data access : Persist a column's value for a model, optionally with date
	\*************************************************************************/
	public
	func write(value:AnyObject,
		 toColumn name:String,
		 forUuid uuid:String,
		 withModDate date:Date!)
		{
		if let date = date
			{
			let sql 	= self.sqlForModifyByUuid(of:name)
			var args	= [AnyObject]()
			QSEntity.add(object:value, to:&args)
			args.append(date as NSDate)
			args.append(uuid as NSString)
			self.executeUpdate(sql: sql, withArgs: args)
			}
		else
			{
			let sql 	= self.sqlForUpdateByUuid(of:name)
			var args	= [AnyObject]()
			QSEntity.add(object:value, to:&args)
			args.append(uuid as NSString)
			self.executeUpdate(sql: sql, withArgs: args)
			}
		}
		
		
	// MARK: model cache
	
	/*************************************************************************\
	|* Model cache : cache a model
	\*************************************************************************/
	public
	func cacheModel(_ model:QSModel)
		{
		if model.uuid.isEmpty
			{
			Logger.quicksilver.log("Model uuid is empty, which is illegal")
			}
		if (model.isDeleted)
			{
			Logger.quicksilver.log("Trying to cache deleted model \(model.uuid)")
			}
		
		model.entity = self
		model.usedRecently = true
		
		self.modelsByUuid.setValue(model, forKey: model.uuid)
		self.models.add(model)
		}

	/*************************************************************************\
	|* Model cache : return the model via its uuid
	\*************************************************************************/
	public
	func cachedModelWith(uuid:String) -> QSModel!
		{
		if let model = self.modelsByUuid[uuid] as? QSModel
			{
			model.usedRecently = true
			return model
			}
		return nil
		}

	/*************************************************************************\
	|* Model cache : return a set of models via uuids
	\*************************************************************************/
	public
	func cachedModelsWith(uuids:any Collection<String>, notFound: inout Set<String>!) -> Set<QSModel>
		{
		var models:Set<QSModel> = Set<QSModel>()

		for uuid in uuids
			{
			if let model = self.modelsByUuid[uuid] as? QSModel
				{
				models.insert(model)
				model.usedRecently = true
				}
			else if notFound != nil
				{
				notFound.insert(uuid)
				}
			}
		return models
		}
		
	/*************************************************************************\
	|* Model cache : return the number of cached models
	\*************************************************************************/
	public
	func cachedModelCount() -> Int
		{
		self.models.count
		}
		
	/*************************************************************************\
	|* Model cache : return all the cached models
	\*************************************************************************/
	public
	func cachedModelsAsArray() -> [QSModel]
		{
		var models:Array<QSModel> = Array<QSModel>()
		for model in self.modelsByUuid.allValues
			{
			if let model = model as? QSModel
				{
				models.append(model)
				}
			}
		return models
		}
		
	/*************************************************************************\
	|* Model cache : return all the cached models
	\*************************************************************************/
	public
	func cachedModelsAsSet() -> Set<QSModel>
		{
		var models:Set<QSModel> = Set<QSModel>()
		for model in self.modelsByUuid.allValues
			{
			if let model = model as? QSModel
				{
				models.insert(model)
				}
			}
		return models
		}
		
	/*************************************************************************\
	|* Model cache : flush the model cache
	\*************************************************************************/
	public
	func flush()
		{
		for model in self.models
			{
			if let model = model as? QSModel
				{
				if (model.usedRecently)
					{
					model.usedRecently = false
					}
				else
					{
					_ = model.flush()
					}
				}
			}
		}
		
	/*************************************************************************\
	|* Model cache : flush a set of models
	\*************************************************************************/
	public
	func flush(models:any Collection<QSModel>)
		{
		for model in models
			{
			_ = model.flush()
			}
		}

	/*************************************************************************\
	|* Model cache : uncache the model
	\*************************************************************************/
	public
	func uncache(model:QSModel)
		{
		self.modelsByUuid.removeObject(forKey: model.uuid)
		model.entity = nil
		self.models.remove(model)
		}

	/*************************************************************************\
	|* Model cache : uncache a set of models
	\*************************************************************************/
	public
	func uncache(models:any Collection<QSModel>)
		{
		for model in models
			{
			self.uncache(model:model)
			}
		}

	/*************************************************************************\
	|* Model cache : uncache all the models
	\*************************************************************************/
	public
	func uncacheAllModels()
		{
		for model in self.models
			{
			if let model = model as? QSModel
				{
				model.entity = nil
				}
			}
		self.modelsByUuid.removeAllObjects()
		self.models.removeAllObjects()
		self.allModelsLoaded = false
		}
	}

