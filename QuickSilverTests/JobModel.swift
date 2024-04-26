/*****************************************************************************\
|* QuickSilver :: QuickSilverTests :: Schema :: JobModel
|*
|* This is the corresponding model class for the tests target
|*
|* Created by ThrudTheBarbarian on 4/13/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation
import OSLog
@testable import QuickSilver

class JobModel : QSModel
	{
	static let titleColumn 		= "title"			// define the names of
	static let minSalaryColumn 	= "minSalary"		// the table columns in
	static let maxSalaryColumn	= "maxSalary"		// this test schema
	
	var title: String								// job title
	var minSalary: Int64							// minimum salary
	var maxSalary: Int64							// maximum salary
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public init(withUuid uuid: String!,
			 forEntity entity: QSEntity,
						title: String = "No title",
					minSalary: Int64 = 0,
					maxSalary: Int64 = 0)
		{
		self.title = title
		self.minSalary = minSalary
		self.maxSalary = maxSalary
		super.init(withUuid:uuid, forEntity:entity)
		}
		
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public convenience init(withEntity entity: QSEntity,
										title: String = "No title",
									minSalary: Int64 = 0,
									maxSalary: Int64 = 0)
		{
		self.init(withUuid:nil,
				 forEntity:entity,
				     title:title,
				 minSalary:minSalary,
				 maxSalary:maxSalary)
		}
	
	
	/*************************************************************************\
	|* For debugging
	\*************************************************************************/
	public override var description:String
		{
		return String(format: "JobModel [\(self.uuid)]: "
					+ "title:\(self.title), "
					+ "salary:\(self.minSalary) - \(self.maxSalary)")
		}
	
	// MARK: CACHE-accessors. Store the property and update the cache
		
	/*************************************************************************\
	|* CACHE the title
	\*************************************************************************/
	func cache(title:String)
		{
		self.title = title;
		self.entity.cacheModel(self)
		}
		
	/*************************************************************************\
	|* CACHE the min salary
	\*************************************************************************/
	func cache(minSalary:Int64)
		{
		self.minSalary = minSalary;
		self.entity.cacheModel(self)
		}
		
	/*************************************************************************\
	|* CACHE the title
	\*************************************************************************/
	func cache(maxSalary:Int64)
		{
		self.maxSalary = maxSalary;
		self.entity.cacheModel(self)
		}
	
	// MARK: SET-accessors. Cause a database-write on a background thread
	
	/*************************************************************************\
	|* SET the title
	\*************************************************************************/
	func set(title:String)
		{
		self.cache(title:title)
		
		if let entity = self.entity as? JobEntity
			{
			let sql			= entity.sqlForUpdate(of:JobModel.titleColumn,
									  where:QSModel.uuidEquals)
			let args	 	= [self.title, self.uuid] 
			let op			= QSSqlOperation.init(with: entity.engine.io,
												   sql: sql,
												  args: args)
			entity.engine.io.bgWriteQueue.addOperation(op)
			}
		}
	
	/*************************************************************************\
	|* SET the min salary
	\*************************************************************************/
	func set(minSalary:Int64)
		{
		self.cache(minSalary:minSalary)
		
		if let entity = self.entity as? JobEntity
			{
			let sql			= entity.sqlForUpdate(of:JobModel.minSalaryColumn,
									  where:QSModel.uuidEquals)
			let args	 	= [self.minSalary, self.uuid] as [Any]
			let op			= QSSqlOperation.init(with: entity.engine.io,
												   sql: sql,
												  args: args)
			entity.engine.io.bgWriteQueue.addOperation(op)
			}
		}
	
	/*************************************************************************\
	|* SET the max salary
	\*************************************************************************/
	func set(maxSalary:Int64)
		{
		self.cache(maxSalary:maxSalary)
		
		if let entity = self.entity as? JobEntity
			{
			let sql			= entity.sqlForUpdate(of:JobModel.maxSalaryColumn,
									  where:QSModel.uuidEquals)
			let args	 	= [self.minSalary, self.uuid] as [Any]
			let op			= QSSqlOperation.init(with: entity.engine.io,
												   sql: sql,
												  args: args)
			entity.addOperation(op)
			}
		}
	}
