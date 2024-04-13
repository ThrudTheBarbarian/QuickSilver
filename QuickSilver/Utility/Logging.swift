/*****************************************************************************\
|* QuickSilver :: Utility :: Logging
|*
|* Logging defines a category on the system log which can be used to log
|* Quicksilver-related errors/warnings etc.
|*
|* Created by ThrudTheBarbarian on 4/8/24.
|* Copyright 2010-2024 ThrudTheBarbarian. All rights reserved.
|*
\*****************************************************************************/

import Foundation
import os.log

extension Logger
	{
    private static var subsystem = Bundle.main.bundleIdentifier!
    static let quicksilver	 	 = Logger(subsystem:subsystem,
										   category:"quicksilver")
	}
