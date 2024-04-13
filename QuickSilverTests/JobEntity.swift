import Foundation
import OSLog
@testable import QuickSilver

/*****************************************************************************\
|* QuickSilver :: SQLTests :: Schema :: TestEntity
|*
|* This is a simple entity class designed to be used to test out the database
|* functionality
|*
|* Created by ThrudTheBarbarian on 4/13/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

/*****************************************************************************\
|* Class definition
\*****************************************************************************/
class JobEntity : QSEntity
	{
		
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public override init(withTableName tableName: String,
								 inEngine engine: QSEngine)
		{
		/*********************************************************************\
		|* Initialise the parent
		\*********************************************************************/
		super.init(withTableName: tableName, inEngine: engine)

		/*********************************************************************\
		|* Create the table structure
		\*********************************************************************/
		self.addColumn(name: QSModel.uuidColumn, type: QSColumnType.Varchar)
		self.addColumn(name: QSModel.creationDateColumn, type: QSColumnType.Timestamp)
		self.addColumn(name: QSModel.modifiedDateColumn, type: QSColumnType.Timestamp)
		
		self.addColumn(name: JobModel.titleColumn, type: QSColumnType.Varchar)
		self.addColumn(name: JobModel.minSalaryColumn, type: QSColumnType.Integer)
		self.addColumn(name: JobModel.maxSalaryColumn, type: QSColumnType.Integer)
		
		if !self.createTableIfNotExists()
			{
			Logger.quicksilver.error("Failed to create table \(self.tableName)")
			}
		}

	// MARK: required entity methods
	
	/*************************************************************************\
	|* Model persistence : Save the model to the DB.
	\*************************************************************************/
	override func persist(model:QSModel)
		{
		if let model = model as? JobModel
			{
			var args = [Any]()
			args.append(model.uuid)
			args.append(model.created)
			args.append(Date())
			
			args.append(model.title)
			args.append(model.minSalary)
			args.append(model.maxSalary)
			
			self.executeUpdate(sql:self.persistModelSql(), withArgs:args)
			model.isPersisted = true
			}
		}
	
	/*************************************************************************\
	|* Model instantiation : Read the model from the DB.
	\*************************************************************************/
	override func loadModelFrom(resultSet rs:QSResultSet) -> QSModel!
		{
		var model:JobModel! = nil
		
		if let uuid = rs.stringForColumn(named: QSModel.uuidColumn)
			{
			var fields = 0
			model = JobModel.init(withUuid:uuid, forEntity: self)
			
			model.uuid = uuid
			fields += 1
				
			if let created = rs.dateForColumn(named: QSModel.creationDateColumn)
				{
				model.created = created
				fields += 1
				}
				
			if let modified = rs.dateForColumn(named: QSModel.creationDateColumn)
				{
				model.modified = modified
				fields += 1
				}
				
			if let title = rs.stringForColumn(named: JobModel.titleColumn)
				{
				model.title = title
				fields += 1
				}
				
			if let minSalary = rs.int64ForColumn(named: JobModel.minSalaryColumn)
				{
				model.minSalary = minSalary
				fields += 1
				}
				
			if let maxSalary = rs.int64ForColumn(named: JobModel.minSalaryColumn)
				{
				model.maxSalary = maxSalary
				fields += 1
				}
				
			if (fields != 6)
				{
				Logger.quicksilver.error("Cannot find job-model fields in results")
				return nil
				}
			}
		return model
		}

	}
