/*****************************************************************************\
|* QuickSilver :: Entity :: QSCounterEntity
|*
|* The QSCounterEntity is an internal property of the database that allows 
|* various named counters to be incremented and shared
|*
|* Created by ThrudTheBarbarian on 4/9/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation
import OSLog

/*****************************************************************************\
|* Class definition
\*****************************************************************************/
class QSCounterEntity : QSEntity
	{
	var counters : [String : QSCounterModel]		// list of counters
	var nextId : Int64								// next counter-id
		
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public override init(withTableName tableName: String,
								 inEngine engine: QSEngine)
		{
		/*********************************************************************\
		|* Set up the local state
		\*********************************************************************/
		counters = [String : QSCounterModel]()
		nextId   = 1
		
		/*********************************************************************\
		|* Initialise the parent
		\*********************************************************************/
		super.init(withTableName: tableName, inEngine: engine)

		/*********************************************************************\
		|* Create the table structure
		\*********************************************************************/
		self.addColumn(name: QSModel.uuidColumn, type: QSColumnType.VarcharPK)
		self.addColumn(name: QSModel.creationDateColumn, type: QSColumnType.Timestamp)
		self.addColumn(name: QSModel.modifiedDateColumn, type: QSColumnType.Timestamp)
		self.addColumn(name: QSCounterModel.tableColumn, type: QSColumnType.Varchar)
		self.addColumn(name: QSCounterModel.counterColumn, type: QSColumnType.Integer)
		if !self.createTableIfNotExists()
			{
			Logger.quicksilver.error("Failed to create table \(self.tableName)")
			}
		}

	// MARK: Model id management
	
	/*************************************************************************\
	|* Return the next model id for the given table. Use the cache if we have
	|* one otherwise fetch (and cache) the one from the table
	\*************************************************************************/
	func nextModelId(for table: String) -> Int64
		{
		if let model:QSCounterModel = self.modelForTable(table)
			{
			return model.counter
			}
		Logger.quicksilver.error("Cannot get next model for \(table)")
		return -1
		}
		
	/*************************************************************************\
	|* Set the next model id to be a particular value
	\*************************************************************************/
	func setNextModelId(to nextId:Int64, for table:String)
		{
		if let model:QSCounterModel = self.modelForTable(table)
			{
			model.set(counter: nextId)
			}
		else
			{
			Logger.quicksilver.error("Cannot set next model for \(table)")
			}
		}
		
	// MARK: Private methods

	/*************************************************************************\
	|* Used to synchronise a block
	\*************************************************************************/
	func synced(_ lock: Any, closure: () -> ())
		{
		objc_sync_enter(lock)
		closure()
		objc_sync_exit(lock)
		}

	/*************************************************************************\
	|* Return the model representing the counter for a table, create if needed
	\*************************************************************************/
	func modelForTable(_ table:String) -> QSCounterModel!
		{
		if let model:QSCounterModel = self.counters[table]
			{
			return model
			}
			
		var model:QSCounterModel! = nil
		objc_sync_enter(self)
		
		self.nextId += 1
		
		model = QSCounterModel.init(withEntity:self)

		model.cache(table: table)
		model.cache(counter: 1)
		
		self.counters[table] = model
		
		objc_sync_exit(self)
			
		model.persist()
		return model
		}
	
	
	// MARK: Required entity methods
	
	/*************************************************************************\
	|* Get the model information from a result set, and construct the class-
	|* specific model
	\*************************************************************************/
	override func loadModelFrom(resultSet rs:QSResultSet) -> QSModel!
		{
		var model:QSCounterModel! = nil
		if let uuid = rs.stringForColumn(named: QSModel.uuidColumn)
			{
			model = QSCounterModel.init(withUuid:uuid, forEntity: self)
			
			if let table = rs.stringForColumn(named: QSCounterModel.tableColumn)
				{
				model.set(table: table)
				}
			else
				{
				Logger.quicksilver.error("Cannot find counter-table name in result set")
				return nil
				}
			
			if let counter = rs.int64ForColumn(named: QSCounterModel.counterColumn)
				{
				model.set(counter: counter)
				}
			else
				{
				Logger.quicksilver.error("Cannot find counter-table counter in result set")
				return nil
				}
				
			if let uuid = rs.stringForColumn(named: QSModel.uuidColumn)
				{
				model.uuid = uuid
				}
			else
				{
				Logger.quicksilver.error("Cannot find counter-table uuid in result set")
				return nil
				}
			}
			
		return model
		}

	
	/*************************************************************************\
	|* Persist one of our models
	\*************************************************************************/
	override func persist(model: QSModel)
		{
		if model is QSCounterModel
			{
			let M = model as! QSCounterModel
			
			/*****************************************************************\
			|* Assemble the data for the columns in the table, as defined in
			|* the init(withTableName: inEngine:) method above
			\*****************************************************************/
			var args = [Any]()
			args.append(M.uuid)
			args.append(Date())
			args.append(Date())
			args.append(M.table)
			args.append(M.counter)
			
			self.executeUpdate(sql: self.persistModelSql(), withArgs: args)
			model.isPersisted = true
			}
		}
	}

