package org.eclipse.efbt.controller.component;

import org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel;
import org.eclipse.efbt.controller.model.platform_call.PlatformCall;
import org.eclipse.efbt.trl.component.translator.AttributeLineageUtil;

public class Controller {
	
	public static void executPlatfromCall (PlatformCall call)
	{
		if (call instanceof GetAttributeLineageModel)
		{
			AttributeLineageUtil.createAttributeLineageModel((GetAttributeLineageModel) call);
		}
	}

}
