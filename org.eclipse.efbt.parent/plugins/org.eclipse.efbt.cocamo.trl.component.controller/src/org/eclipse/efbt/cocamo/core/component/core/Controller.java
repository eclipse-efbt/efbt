package org.eclipse.efbt.cocamo.core.component.core;


import org.eclipse.efbt.cocamo.trl.component.translator.api.AttributeLineageUtil;
import org.eclipse.efbt.cocamo.trl.component.translator.impl.AttributeLineageUtilImpl;
import org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel;
import org.eclipse.efbt.controller.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;

public class Controller {
	
	public static void executPlatfromCall (PlatformCall call)
	{
		if (call instanceof GetAttributeLineageModel)
		{
			createAttributeLineageModel((GetAttributeLineageModel) call);
		}
	}
	
	/**
	 * Creates an attributeLineageModel according to the details of the GetAttributeLineageModel
	 * call, this is then set as the resultingModel field of the call.
	 * 
	 * @param call
	 */
	public static void createAttributeLineageModel(GetAttributeLineageModel call) {
		 call.eUnset(call.eClass().getEStructuralFeature(Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL));
		 
		 VersionedComponentsSet vcs = call.getTransformationContext();
		 AttributeLineageUtil alu = new AttributeLineageUtilImpl();
		 call.setResultingModel(alu.createAttributeLineageModel(vcs));
	}



}
