/*****************************************************************************\
|* QuickSilver :: DataModel :: Columns :: QSColumn
|*
|* Entity properties are closely linked to columns in their table in the DB. 
|* The QSColumn class identifies properties specific to a column - basically
|* out-of-band information on an entity property.
|*
|* Created by ThrudTheBarbarian on 4/8/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation

/*****************************************************************************\
|* Define the options (currently all ignored) for creation of columns. Note
|* that these can be combined using something like
|*
|*   let combined = QSColumnOptions.none | QSColumnOptions.lazyLoad
|*
\*****************************************************************************/
public enum QSColumnOptions : Int
	{
	case none			= 0x000,
		 createIndex	= 0x001,
		 lazyLoad		= 0x002
	}

/*****************************************************************************\
|* Define the types of columns we support. This is used to determine what sort
|* of object or value to pass back and expect from methods handling the type
\*****************************************************************************/
public enum QSColumnType
	{
	case
		IntegerPK,
		Integer,
		
		Bool,

		Decimal,
		
		Varchar,
		VarcharPK,

		Timestamp,
		TimestampAsTimeInterval,

		DataBlob,
		
		Unknown // Really just so the default in a switch has the chance to
				// catch something unknown
	}

public class QSColumn : NSObject
	{
	var name 	: String!
	var type 	: QSColumnType
	var options : QSColumnOptions
	
	override public init()
		{
		self.type 		= QSColumnType.Varchar
		self.options	= QSColumnOptions.none
		super.init()
		}
	
	static func with(name:String, type:QSColumnType, options:QSColumnOptions) -> QSColumn
		{
		let col = QSColumn()
		col.name = name
		col.type = type
		col.options = options
		return col
		}
	}
