//
//  QuickSilverTests.swift
//  QuickSilverTests
//
//  Created by ThrudTheBarbarian on 4/13/24.
//

import OSLog
import XCTest
@testable import QuickSilver


final class DatabaseAccess: XCTestCase
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

		self.engine = QSEngine.init(withPath:path)
		}
		
	/*************************************************************************\
	|* Tear down the test env
	\*************************************************************************/
    override func tearDownWithError() throws
		{
		self.engine.close()
		self.engine = nil
		}


	/*************************************************************************\
	|* Check to see if the db is active
	\*************************************************************************/
    func testIsActive() throws
		{
		XCTAssert(self.engine.io.isActive(), "Database is not active")
		}

	/*************************************************************************\
	|* Check to see what the library version is
	\*************************************************************************/
    func testLibraryVersion() throws
		{
		let version = self.engine.io.sqlLibraryVersion()
		XCTAssert(version == "3.43.2", "SQLite library version [\(version)] is not 3.43.2")
		}
		
	/*************************************************************************\
	|* Check to see if we can increment a counter object
	\*************************************************************************/
    func testIncrementCounter() throws
		{
		let entity = QSCounterEntity.init(withTableName: "test_table", inEngine: self.engine)
		var nextId:Int64 = 0
		
		for _ in 0..<5
			{
			nextId = entity.nextModelId(for:"test_table")
			entity.setNextModelId(to: nextId+1, for: "test_table")
			}
		XCTAssert(nextId == 5, "Counter increment failed counter:\(nextId) != 5")
		
		let model = entity.anyModel(where: "uuid != ''")
		XCTAssert(model != nil, "Failed to retrieve model")
		}

	/*************************************************************************\
	|* Check to see if we can fetch a counter object by id
	\*************************************************************************/
    func testFetchCounterById() throws
		{
		let entity = QSCounterEntity.init(withTableName: "test_table", inEngine: self.engine)
		let nextId:Int64 = entity.nextModelId(for:"test_table")
		XCTAssert(nextId == 1, "Counter increment failed counter:\(nextId) != 1")
		
		let model = entity.anyModel(where: "uuid != ''")
		XCTAssert(model != nil, "Failed to retrieve model")
		}

	/*************************************************************************\
	|* Check to see if we can fetch a counter object by uuid
	\*************************************************************************/
    func testFetchCounterByUUID() throws
		{
		let entity = QSCounterEntity.init(withTableName: "test_table", inEngine: self.engine)
		let nextId:Int64 = entity.nextModelId(for:"test_table")
		XCTAssert(nextId == 1, "Counter increment failed counter:\(nextId) != 1")
		
		let model = entity.anyModel(where: "uuid != ''")
		XCTAssert(model != nil, "Failed to retrieve model")
		if let model = model as? QSCounterModel
			{
			if let same = entity.modelWith(uuid: model.uuid) as? QSCounterModel
				{
				XCTAssert(same.uuid == model.uuid, "Could not fetch via uuid")
				XCTAssert(same.counter == model.counter, "Could not fetch via uuid")
				}
			else
				{
				XCTAssert(false, "could not fetch second model")
				}
			}
		else
			{
			XCTAssert(false, "could not fetch first model")
			}
		}

	/*************************************************************************\
	|* Check to see if we can have multiple models in a table
	\*************************************************************************/
    func testMultipleModels() throws
		{
		let entity = QSCounterEntity.init(withTableName: "test_table", inEngine: self.engine)
		let table1:Int64 = entity.nextModelId(for:"table1")
		var table2:Int64 = entity.nextModelId(for:"table2")
		entity.setNextModelId(to: table2+1, for: "table2")
		    table2 		 = entity.nextModelId(for:"table2")
		XCTAssert(table1 == 1, "Counter increment failed counter:\(table1) != 1")
		XCTAssert(table2 == 2, "Counter increment failed counter:\(table1) != 2")
		}

	/*************************************************************************\
	|* Check to see if we can fetch a counter object using where
	\*************************************************************************/
    func testFetchWithWhere() throws
		{
		let entity = QSCounterEntity.init(withTableName: "test_table", inEngine: self.engine)
		let table1:Int64 = entity.nextModelId(for:"table1")
		var table2:Int64 = entity.nextModelId(for:"table2")
		entity.setNextModelId(to: table2+1, for: "table2")
		table2			 = entity.nextModelId(for:"table2")

		XCTAssert(table1 == 1, "Counter increment failed counter:\(table1) != 1")
		XCTAssert(table2 == 2, "Counter increment failed counter:\(table1) != 2")

		if let models = entity.models(where: "uuid != ''")
			{
			XCTAssert(models.count == 2, "Failed to fetch both models")
			}
		else
			{
			XCTAssert(false, "could not fetch multiple models with WHERE")
			}
		}
		
	/*************************************************************************\
	|* Check to see if we can fetch a counter object using where
	\*************************************************************************/
    func testCreateSimpleIndex() throws
		{
		let entity = QSCounterEntity.init(withTableName: "test_table", inEngine: self.engine)
		let _ = entity.nextModelId(for:"table1")

		let ok = self.engine.io.createIndex(onTable: "test_table", column: "created")
		XCTAssert(ok, "Failed to create index on 'created'")
		}
		
	/*************************************************************************\
	|* Check to see if we can fetch a counter object using where
	\*************************************************************************/
    func testCreateMulticolIndex() throws
		{
		let entity = QSCounterEntity.init(withTableName: "test_table", inEngine: self.engine)
		let _ = entity.nextModelId(for:"table1")

		let ok = self.engine.io.createIndex(onTable: "test_table", columns: ["created","modified"])
		XCTAssert(ok, "Failed to create index on 'created & modified'")
		}
	}
