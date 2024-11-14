/*****************************************************************************\
|* QuickSilver :: Engine :: QsOperationQueue
|*
|* This class implements the background processing of database writes. Since
|* model objects update themselves, post a write-op, then immediately return,
|* we need to queue up these changes and action them in sequence.
|*
|* Created by ThrudTheBarbarian on 4/8/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation

/*****************************************************************************\
|* Class definition
\*****************************************************************************/
class QSOperationQueue : OperationQueue, @unchecked Sendable
	{
	let addLock : NSLock!				// Lock while adding to the queue
	let waitLock : NSLock!				// Only one wait is allowed at a time
	var blockedAdds : [Operation]!		// List of ops queued while blocked
	
	/*************************************************************************\
	|* Creation
	\*************************************************************************/
	override public init()
		{
		self.addLock 		= NSLock()
		self.waitLock		= NSLock()
		self.blockedAdds	= nil
		
		super.init()
		}
		
	/*************************************************************************\
	|* Add an operation to the queue
	\*************************************************************************/
	func add(operation:Operation)
		{
		self.addLock.lock()
		defer
			{
			self.addLock.unlock()
			}
			
		if self.blockedAdds != nil
			{
			self.blockedAdds.append(operation)
			}
		else
			{
			super.addOperation(operation)
			}
		}
		
	/*************************************************************************\
	|* Return the number of blocked operations
	\*************************************************************************/
	func blockedOperationCount() -> Int
		{
		self.addLock.lock()
		defer
			{
			self.addLock.unlock()
			}
		return self.blockedAdds?.count ?? 0
		}
		
	/*************************************************************************\
	|* Block the queue until all operations before this have been processed
	\*************************************************************************/
	func waitForOutstandingOperations()
		{
		// Only allow one thread through this at a time
		self.waitLock.lock()
		defer
			{
			self.waitLock.unlock()
			}
		
		// Set up the pending queue
		self.addLock.lock()
		self.blockedAdds = [Operation]()
		self.addLock.unlock()

		// Wait until we're all done
		self.waitUntilAllOperationsAreFinished()
		
		// Process the pending queue
		self.addLock.lock()
		for op in self.blockedAdds
			{
			super.addOperation(op)
			}
		self.blockedAdds = nil
		self.addLock.unlock()
		}
	}
