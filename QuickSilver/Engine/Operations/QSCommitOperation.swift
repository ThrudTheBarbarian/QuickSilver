/*****************************************************************************\
|* QuickSilver :: Engine :: Operation :: QSCommitOperation
|*
|* Terminates (and optionally re-opens) the transaction that is currently
|* active
|*
|* Created by ThrudTheBarbarian on 4/10/24
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation
import OSLog

/*****************************************************************************\
|* Class definition
\*****************************************************************************/
class QSCommitOperation : Operation
	{
	let io : QSIO!				// io object
	let reopen : Bool			// Begin a new transaction after closing this
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	public init(with io:QSIO, beginNewTransaction reopen: Bool = true)
		{
		self.io 	= io
		self.reopen = reopen
		
		super.init()
		}

	/*************************************************************************\
	|* For debugging
	\*************************************************************************/
	public override var description:String
		{
		let qsio = self.io == nil  ? "nil" : self.io.description
		return String(format: "commit of QSIO: \(qsio)")
		}

	/*************************************************************************\
	|* The actual operation
	\*************************************************************************/
	override public func main()
		{
		if let io = self.io
			{
			if !io.commit(beginNewTransaction:self.reopen)
				{
				Logger.quicksilver.error("Failed to commit transaction op")
				}
			}
		else
			{
			Logger.quicksilver.error("No QSIO in commit transaction op")
			}
		}
		
	/*************************************************************************\
	|* Operation entry point
	\*************************************************************************/
	func run()
		{
		self.main()
		}
	}
