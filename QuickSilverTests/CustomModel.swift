//
//  CustomModel.swift
//  QuickSilverTests
//
//  Created by ThrudTheBarbarian on 4/13/24.
//

import OSLog
import XCTest
@testable import QuickSilver


final class CustomModel: XCTestCase
	{
	var engine:QSEngine!

	/*************************************************************************\
	|* Set up the test env
	\*************************************************************************/
    override func setUpWithError() throws
		{
		// Find documents directory
		let dirs: [String] = NSSearchPathForDirectoriesInDomains(
							FileManager.SearchPathDirectory.documentDirectory,
							FileManager.SearchPathDomainMask.allDomainsMask,
							true)
		
		var path = ""
		
		if dirs.count > 0
			{
			let dir = dirs[0] //documents directory
			path = dir.appendingFormat("/test.db")
			
			let fm = FileManager.default
			if (fm.fileExists(atPath: path))
				{
				do
					{
					try fm.removeItem(atPath: path)
					}
				catch
					{
					Logger.quicksilver.warning("Could not delete \(path)")
					}
				}
			}
		else
			{
			print("Could not find local directory to store file")
			return
			}

		engine = QSEngine.init(withPath:path)
		}
		
	/*************************************************************************\
	|* Tear down the test env
	\*************************************************************************/
    override func tearDownWithError() throws
		{
		engine.close()
		engine = nil
		}

	/*************************************************************************\
	|* Check to see if we can create a new Job
	\*************************************************************************/
    func testCreateJob() throws
		{
		/*********************************************************************\
		|* Create our dummy job
		\*********************************************************************/
		let entity 	= JobEntity.init(withTableName: "jobs", inEngine: self.engine)
		let job  	= JobModel.init(withEntity:entity,
										 title:"managing director",
									 minSalary: 100000,
									 maxSalary: 1000000)
		/*********************************************************************\
		|* And persist it
		\*********************************************************************/
		job.persist()
		
		/*********************************************************************\
		|* Check the model is in the DB
		\*********************************************************************/
		if let models = entity.models(where: "uuid != ''")
			{
			XCTAssert(models.count == 1, "Failed to create Job model")
			}
		else
			{
			XCTAssert(false, "Failed to fetch Job model")
			}
		}

	/*************************************************************************\
	|* Check to see if we can create a new Job
	\*************************************************************************/
    func testUpdateJobParameters() throws
		{
		/*********************************************************************\
		|* Create our dummy job
		\*********************************************************************/
		let entity 	= JobEntity.init(withTableName: "jobs", inEngine: self.engine)
		let job  	= JobModel.init(withEntity:entity,
										 title:"managing director",
									 minSalary: 100000,
									 maxSalary: 1000000)
		/*********************************************************************\
		|* And persist it
		\*********************************************************************/
		job.persist()

		/*********************************************************************\
		|* Check to see if it persisted ok
		\*********************************************************************/
		if let models = entity.models(where: "uuid != ''")
			{
			XCTAssert(models.count == 1, "Failed to create Job model")
			for model in models
				{
				if let model = model as? JobModel
					{
					XCTAssert(model.title == "managing director", "Failed to set Job title")
					}
				else
					{
					XCTAssert(false, "WTH? Somehow got a non-job object back!")
					}
				}
			}
		else
			{
			XCTAssert(false, "Failed to fetch Job model")
			}

		/*********************************************************************\
		|* Change the job title
		\*********************************************************************/
		job.set(title: "CEO")
		
		/*********************************************************************\
		|* Check to see that the change "took"
		\*********************************************************************/
		if let models = entity.models(where: "uuid != ''")
			{
			XCTAssert(models.count == 1, "Failed to create Job model")
			for model in models
				{
				if let model = model as? JobModel
					{
					print("uuid:\(model.uuid), title = \(model.title)")
					XCTAssert(model.title == "CEO", "Failed to update Job title")
					}
				else
					{
					XCTAssert(false, "WTH? Somehow got a non-job object back!")
					}
				}
			}
		else
			{
			XCTAssert(false, "Failed to fetch Job model")
			}
		}
	}
