/*****************************************************************************\
|* QuickSilver :: Engine :: Operation :: QSSqlOperation
|*
|* Allows a sql write-operation to be appended to the queue of operations
|*
|* Created by ThrudTheBarbarian on 4/8/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation
import OSLog

/*****************************************************************************\
|* Class definition
\*****************************************************************************/
class QSSqlOperation : Operation
	{
	let io : QSIO!				// io object
	let sql : Any!				// Either a string or preparedSQL
	let args : [AnyObject]!		// List of args to the SQL
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public init(with io:QSIO, sql:Any, args:[Any])
		{
		self.io = io
		self.sql = sql
		self.args = QSPreparedSql.filter(all:args)
		
		super.init()
		}
	
	/*************************************************************************\
	|* Invocation
	\*************************************************************************/
	public func submit() -> Bool
		{
		if (io == nil) || (sql == nil) || (args == nil)
			{
			if (sql == nil)
				{
				Logger.quicksilver.error("nil SQL in SqlOperation")
				}
			else
				{
				switch self.sql
					{
					case let SQL as String:
						Logger.quicksilver.error("nil value in SqlOperation: \(SQL)")
					default:
						Logger.quicksilver.error("unknown SQL type in SqlOperation")
					}
				}
			return false
			}
		self.main()
		return true
		}

	/*************************************************************************\
	|* The actual operation
	\*************************************************************************/
	override public func main()
		{
		if (self.sql != nil)
			{
			var preparedSql : QSPreparedSql!
			
			switch (self.sql)
				{
				case let SQL as QSPreparedSql:
					preparedSql = SQL
				case let SQL as String:
					preparedSql = QSPreparedSql.init(withIo:self.io,
														sql:SQL,
													prepare:false)
				default:
					Logger.quicksilver.error("Unknown object in SqlOperation")
				}
				
			// We always want to unlock the DB at the end if we can
			defer
				{
				if (self.io != nil)
					{
					self.io?.unlockDatabase()
					}
				}
				
			do
				{
				if self.io == nil
					{
					throw QSException.noDatabase
					}
				else if preparedSql == nil
					{
					throw QSException.unknownSQLObject
					}
				self.io?.lockDatabase()
				if !preparedSql.update(with:self.args)
					{
					let msg = String(describing: self.sql)
					Logger.quicksilver.error("Failed to update SQL:\(msg)")
					}
				}
			catch
				{
				Logger.quicksilver.error("failed to prepare SQL")
				}
			}
		}
		
	/*************************************************************************\
	|* For debugging
	\*************************************************************************/
	public override var description:String
		{
		let sql = self.sql == nil ? "nil" : self.sql!
		return String(format: "QSIO SQL: \(sql)")
		}

	/*************************************************************************\
	|* Operation entry point
	\*************************************************************************/
	func run()
		{
		self.main()
		}
	}
