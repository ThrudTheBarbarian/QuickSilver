/*****************************************************************************\
|* QuickSilver :: DataModel :: Model :: QSCounterModel
|*
|* A QSCounterModel is the object that represents a single row in the table
|* managing the counters for other QSModels.
|*
|* Created by ThrudTheBarbarian on 4/8/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation

class QSCounterModel : QSModel
	{
	static let tableColumn = "tableName"			// name of 'table' column
	static let counterColumn = "counter"			// name of 'counter' col
	
	private(set) var table: String					// table identifier
	var counter: Int64								// Counter for table
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public override init(withUuid uuid: String!, forEntity entity: QSEntity)
		{
		counter = 0
		table = ""
		super.init(withUuid:uuid, forEntity:entity)
		}
	
	// MARK: CACHE-accessors. Do not cause a database-write
	
	/*************************************************************************\
	|* CACHE the counter
	\*************************************************************************/
	func cache(counter:Int64)
		{
		self.counter = counter
		}
	
	/*************************************************************************\
	|* CACHE the table
	\*************************************************************************/
	func cache(table:String)
		{
		self.table = table
		}
	
	// MARK: SET-accessors. Cause a database-write
	
	/*************************************************************************\
	|* SET the counter
	\*************************************************************************/
	func set(counter:Int64)
		{
		cache(counter:counter)
	
		if let ce = self.entity as? QSCounterEntity
			{
			let io 			= ce.engine.io
			let sql			= ce.sqlForUpdate(of:QSCounterModel.counterColumn,
									  where:QSModel.uuidEquals)
			let args	 	= [counter, self.uuid] as [Any]
			let op			= QSSqlOperation.init(with: io, sql: sql, args: args)
			io.bgWriteQueue.addOperation(op)
			}
		}
	
	/*************************************************************************\
	|* SET the table
	\*************************************************************************/
	func set(table:String)
		{
		cache(table: table)
	
		if let ce = self.entity as? QSCounterEntity
			{
			let io 			= ce.engine.io
			let sql			= ce.sqlForUpdate(of:QSCounterModel.counterColumn,
									  where:QSModel.uuidEquals)
			let args 		= [table, self.uuid]
			let op			= QSSqlOperation.init(with: io, sql: sql, args: args)
			io.bgWriteQueue.addOperation(op)
			}
		}

	/*************************************************************************\
	|* For debugging
	\*************************************************************************/
	public override var description:String
		{
		return String(format: "QSCounterModel [\(self.uuid)]: "
					+ "table:\(self.table), "
					+ "counter:\(self.counter)")
		}

	}
