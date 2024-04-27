/*****************************************************************************\
|* QuickSilver :: Engine :: QSEngine
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

public class QSEngine : NSObject
	{
	var counterEntity: QSCounterEntity!			// Entity for counters table
	public var io: QSIO							// Access to the database file
	var dbPath: String							// Path to the DB
	
	var entitiesByClass: [String:QSEntity]		// lookup by class name
	var entitiesByTable: [String:QSEntity]		// lookup by table-name
	
	var isOpen: Bool							// Is the db open
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public init(withPath path: String)
		{
		/*********************************************************************\
		|* Create the local instance variables
		\*********************************************************************/
		self.isOpen 			= false
		self.dbPath 			= path
		self.entitiesByClass	= [String:QSEntity]()
		self.entitiesByTable	= [String:QSEntity]()
		self.io 				= QSIO(withPath:self.dbPath)
		
		/*********************************************************************\
		|* Initialise the parent
		\*********************************************************************/
		super.init()

		/*********************************************************************\
		|* Configure the database input/output interface
		\*********************************************************************/
		if !io.open(asReadOnly:false)
			{
			Logger.quicksilver.log("QSEngine Failed to open DB at path \(path)")
			}
		else
			{
			io.setSynchronicity(QSIO.Synchronicity.normal)
			if !io.beginTransaction()
				{
				Logger.quicksilver.error("QSEngine could not initialise transaction")
				}
			}
			
		/*********************************************************************\
		|* Create the counter entity
		\*********************************************************************/
		self.counterEntity 		= QSCounterEntity.init(withTableName:"counters",
															inEngine:self)
		}

	/*************************************************************************\
	|* Creation: for subclasses to override
	\*************************************************************************/
	public func open() -> Bool
		{
		self.didOpenEngine(self)
		return true
		}

	/*************************************************************************\
	|* Creation: for subclasses to override
	\*************************************************************************/
	open func didOpenEngine(_ engine:QSEngine)
		{
		}
		
	// MARK: Syncing
	
	/*************************************************************************\
	|* Syncing: save the current state
	\*************************************************************************/
	public func save()
		{
		self.willSaveEngine()
		if !self.commitAndBeginNewTransaction()
			{
			Logger.quicksilver.error("Failed to save the engine")
			}
		}

	/*************************************************************************\
	|* Syncing: Give the entities a chance to clean up before a save happens
	\*************************************************************************/
	open func willSaveEngine()
		{
		for entity in self.entitiesByTable.values
			{
			entity.willSaveEngine()
			}
		}
	
	/*************************************************************************\
	|* Syncing: flush the entities
	\*************************************************************************/
	public func flush()
		{
		for entity in self.entitiesByTable.values
			{
			entity.flush()
			}
		}

	/*************************************************************************\
	|* Syncing: shut down
	\*************************************************************************/
	public func close()
		{
		io.bgWriteQueue.waitUntilAllOperationsAreFinished()
		
		/*********************************************************************\
		|* Shut down all the entities 
		\*********************************************************************/
		for entity in self.entitiesByTable.values
			{
			entity.isActive = false
			entity.waitForOutstandingWrites()
			}
			
		/*********************************************************************\
		|* Finalise all the prepared statements
		\*********************************************************************/
		self.io.finalisePreparedStatements()
		
		/*********************************************************************\
		|* Get/Release the lock to ensure there's no ResultSets still in flight
		\*********************************************************************/
		self.io.lockDatabase()
		self.io.unlockDatabase()
		
		/*********************************************************************\
		|* Clear the cache 
		\*********************************************************************/
		for entity in self.entitiesByTable.values
			{
			entity.waitForOutstandingWrites()
			entity.uncacheAllModels()
			}
			
		/*********************************************************************\
		|* Close the existing transaction and close down the DB
		\*********************************************************************/
		self.commit()
		io.bgWriteQueue.waitUntilAllOperationsAreFinished()
		
		io.busyRetryTimeout = 10
		if !io.close()
			{
			Logger.quicksilver.log("Failed to shut down cleanly")
			}
		}
	
	// MARK: Transactions
	
	/*************************************************************************\
	|* Transactions: begin a transaction
	\*************************************************************************/
	public func beginTransaction() -> Bool
		{
		if !self.io.beginTransaction()
			{
			Logger.quicksilver.error("Failed to start initial transaction")
			return false
			}
		return true
		}
	
	/*************************************************************************\
	|* Transactions: begin a transaction
	\*************************************************************************/
	public func commit()
		{
		self.io.backgroundCommit()
		}
		
	/*************************************************************************\
	|* Transactions: commit the last transaction and start anew
	\*************************************************************************/
	public func commitAndBeginNewTransaction() -> Bool
		{
		if !self.io.commit(beginNewTransaction: true)
			{
			Logger.quicksilver.error("Failed to cycle transactions")
			return false
			}
		return true
		}
		
	/*************************************************************************\
	|* Transactions: (ok, not quite) wait for outstanding writes
	\*************************************************************************/
	public func waitForOutstandingWrites()
		{
		for entity in self.entitiesByTable.values
			{
			entity.waitForOutstandingWrites()
			}
		}
		
		
	// MARK: Entity management
	
	/*************************************************************************\
	|* Entity management : return the entity for a given class
	\*************************************************************************/
	public func entity(forClassNamed name:String) -> QSEntity!
		{
		let entity:QSEntity! = self.entitiesByClass[name]
		if (!entity.isActive)
			{
			return nil
			}
		return entity
		}
	
	/*************************************************************************\
	|* Entity management : return the entity for a given table
	\*************************************************************************/
	public func entity(forTableNamed table:String) -> QSEntity!
		{
		self.entitiesByTable[table]
		}
	
	/*************************************************************************\
	|* Entity management : register an entity for a given class
	\*************************************************************************/
	public func addEntity(_ entity:QSEntity!, forClass klass:QSEntity)
		{
		if let validEntity = entity
			{
			self.entitiesByClass[klass.className] = validEntity
			self.entitiesByTable[klass.tableName] = validEntity
			}
		}
	
	/*************************************************************************\
	|* Entity management : register an entity for a given table
	\*************************************************************************/
	public func addEntity(_ entity:QSEntity!, forTable table:String)
		{
		if let validEntity = entity
			{
			self.entitiesByTable[table] = validEntity
			}
		}
		
	}
