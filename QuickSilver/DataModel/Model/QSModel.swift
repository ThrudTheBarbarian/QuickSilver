/*****************************************************************************\
|* QuickSilver :: DataModel :: Model :: QSModel
|*
|* A QSModel is the object that represents a single row in a table governed by
|* a QSEntity. This is the base class, and it's expected that custom tables are
|* constructed by extending this class
|*
|* Created by ThrudTheBarbarian on 4/8/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation
import OSLog

open class QSModel : NSObject
	{
	/*************************************************************************\
	|* Static constants for table columns etc.
	\*************************************************************************/
	public static let uuidEquals = "uuid=?"				// must match uuid below
	public static let uuidColumn = "uuid"				// name of the UUID column
	public static let creationDateColumn = "created"	// name of the creation col
	public static let modifiedDateColumn = "modified"	// name of the modified col

	/*************************************************************************\
	|* Instance properties
	\*************************************************************************/
	public var isDeleted: Bool					// Marked as being deleted
	public var isPersisted: Bool				// Have we been persisted before
	public var notifyOnChange: Bool				// Whether to notify on change
	public var usedRecently: Bool				// For caching purposes
	
	public var uuid: String						// Value in uuid column in db
	public var created: Date					// When created, in GMT
	public var modified: Date					// When modified, in GMT

	public weak var entity : QSEntity!			// entity that manages us
	
	// MARK: Creation

	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public convenience init(withEntity entity: QSEntity)
		{
		self.init(withUuid: nil, forEntity: entity)
		}
		
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public init(withUuid uuid: String!, forEntity entity: QSEntity)
		{
		self.isDeleted			= false
		self.isPersisted		= false
		self.notifyOnChange		= true
		self.usedRecently		= false
		
		self.uuid				= uuid != nil ? uuid : UUID.init().uuidString
		
		self.created			= Date()
		self.modified			= Date()
		self.entity				= entity
		
		super.init()
		}
		
	/*************************************************************************\
	|* Creation: fetch the model for a given UUID via this model's entity
	\*************************************************************************/
	public static func modelWith(uuid:String, inEngine engine:QSEngine) -> QSModel!
		{
		if let entity = engine.entity(forClassNamed: self.className())
			{
			return entity.modelWith(uuid:uuid)
			}
		return nil
		}
		
	/*************************************************************************\
	|* Creation: fetch the models for given uuids via this model's entity
	\*************************************************************************/
	public static func modelsWith(uuids:[String], inEngine engine:QSEngine) -> Set<QSModel>!
		{
		if let entity = engine.entity(forClassNamed: self.className())
			{
			return entity.modelsWith(uuids:uuids)
			}
		return nil
		}

	/*************************************************************************\
	|* Creation: return any single model matching a WHERE clause
	\*************************************************************************/
	public static func anyModelWith(engine:QSEngine, where sql:String, args:[Any?]) -> QSModel!
		{
		if let entity = engine.entity(forClassNamed: self.className())
			{
			return entity.anyModel(where:sql, args:args)
			}
		return nil
		}

	/*************************************************************************\
	|* Creation: return any single model matching a WHERE clause, varargs style
	\*************************************************************************/
	public static func anyModelWith(engine:QSEngine, where sql:String, _ args:Any?...) -> QSModel
		{
		return self.anyModelWith(engine:engine, where:sql, args:args)
		}
		
	// MARK: Accessors
		
	/*************************************************************************\
	|* Accessors: Handle the model uuid
	\*************************************************************************/
	public func cache(uuid:String)
		{
		self.uuid = uuid
		}
	
	/*************************************************************************\
	|* Accessors: Handle the creation date
	\*************************************************************************/
	public func cache(creation:Date)
		{
		self.created = creation
		}
	
	/*************************************************************************\
	|* Accessors: Handle the modified date
	\*************************************************************************/
	public func cache(modified:Date)
		{
		self.modified = modified
		}
		
	// MARK: i/o
	
	/*************************************************************************\
	|* i/o: flush the model from the cache, if we can
	\*************************************************************************/
	public func flush() -> Bool
		{
		var didUncache: Bool = false
		if self.isPersisted
			{
			self.entity.uncache(model: self)
			didUncache = true
			}
		return didUncache
		}
	
	/*************************************************************************\
	|* i/o: persist ourselves
	\*************************************************************************/
	public func persist()
		{
		if !self.isPersisted
			{
			self.entity.persist(model:self)
			self.wasPersisted()
			}
		}
		
	/*************************************************************************\
	|* i/o: exists to allow subclasses to strut their funky stuff
	\*************************************************************************/
	open func wasPersisted()
		{}

	/*************************************************************************\
	|* i/o: exists to allow subclasses to override, must still call the static
	\*************************************************************************/
	public func write(value:AnyObject, toColumn column:String)
		{
		QSModel.write(value:value, toColumn:column, forModel:self)
		}
	
	/*************************************************************************\
	|* i/o: Persist a value to a column for a model
	\*************************************************************************/
	public static func write(value:AnyObject,
			toColumn column:String,
			 forModel model:QSModel,
			  withDate date:Date! = Date())
		{
		if (!model.isPersisted) && (!model.isDeleted)
			{
			if let entity = model.entity
				{
				if let date = date
					{
					model.cache(modified: date)
					}
					
				entity.write(value:value,
						  toColumn:column,
						   forUuid:model.uuid,
					   withModDate:date)
				}
			}
		}
		
	// MARK: data access
	
	/*************************************************************************\
	|* Access: return a string value for the column name
	\*************************************************************************/
	public func stringValue(forColumn name:String) -> String!
		{
		if (self.isPersisted) && (!self.isDeleted)
			{
			if let entity = self.entity
				{
				return entity.stringFor(column:name, forUuid:self.uuid)
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* Access: return a NSNumber value for the column name
	\*************************************************************************/
	public func numberValue(forColumn name:String) -> NSNumber!
		{
		if (self.isPersisted) && (!self.isDeleted)
			{
			if let entity = self.entity
				{
				return entity.numberFor(column:name, forUuid:self.uuid)
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* Access: return a Date value for the column name
	\*************************************************************************/
	public func dateValue(forColumn name:String) -> Date!
		{
		if (self.isPersisted) && (!self.isDeleted)
			{
			if let entity = self.entity
				{
				return entity.dateFor(column:name, forUuid:self.uuid)
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* Access: return a Data value for the column name
	\*************************************************************************/
	public func dataValue(forColumn name:String) -> Data!
		{
		if (self.isPersisted) && (!self.isDeleted)
			{
			if let entity = self.entity
				{
				return entity.dataFor(column:name, forUuid:self.uuid)
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* Access: return a Bool value for the column name
	\*************************************************************************/
	public func boolValue(forColumn name:String) -> Bool!
		{
		if (self.isPersisted) && (!self.isDeleted)
			{
			if let entity = self.entity
				{
				return entity.boolFor(column:name, forUuid:self.uuid)
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* Access: return an Int64 value for the column name
	\*************************************************************************/
	public func int64Value(forColumn name:String) -> Int64!
		{
		if (self.isPersisted) && (!self.isDeleted)
			{
			if let entity = self.entity
				{
				return entity.int64For(column:name, forUuid:self.uuid)
				}
			}
		return nil
		}
	
	// MARK: Deletion
	
	/*************************************************************************\
	|* Deletion : Return the first engine obtained from a list of models
	\*************************************************************************/
	public static func engineFor(models:any Collection) -> QSEngine!
		{
		for model in models
			{
			if let model = model as? QSModel
				{
				if let engine = model.entity?.engine
					{
					return engine
					}
				}
			}
		return nil
		}
	
	/*************************************************************************\
	|* Deletion : So subclasses can override
	\*************************************************************************/
	open func willBeDeleted()
		{}
	
	/*************************************************************************\
	|* Deletion : delete a model
	\*************************************************************************/
	public func deleteModel()
		{
		if self.isPersisted
			{
			self.willBeDeleted()
			self.entity?.delete(model:self)
			}
		self.isDeleted = true
		}
	
	/*************************************************************************\
	|* Deletion : delete a set/array of models
	\*************************************************************************/
	public static func delete(models:any Collection)
		{
		for model in models
			{
			if let model = model as? QSModel
				{
				model.willBeDeleted()
				}
			}
			
		if let engine = QSModel.engineFor(models: models)
			{
			if let entity = engine.entity(forClassNamed:self.className())
				{
				entity.delete(models:models)
				}
			}
		else
			{
			Logger.quicksilver.error("Failed to find engine to delete models")
			}
		}
	
	/*************************************************************************\
	|* Deletion : delete with a WHERE clause
	\*************************************************************************/
	public static func deleteFrom(engine:QSEngine!, where sql:String, args:[Any?])
		{
		if let entity = engine.entity(forClassNamed:self.className())
			{
			if !entity.deleteModels(where:sql, args:args)
				{
				Logger.quicksilver.error("Failed to delete models: \(sql)")
				}
			}
		}
	
	/*************************************************************************\
	|* Deletion : delete with a WHERE clause, varargs style
	\*************************************************************************/
	public static func deleteFrom(engine:QSEngine, where sql:String, _ args:Any?...)
		{
		self.deleteFrom(engine:engine, where:sql, args:args)
		}
	}
