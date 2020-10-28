package org.eclipse.efbt.controller.core.component.core;


import org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel;
import org.eclipse.efbt.controller.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDToCocason;
import org.eclipse.efbt.language.trl.component.translator.api.AttributeLineageUtil;
import org.eclipse.efbt.language.trl.component.translator.impl.AttributeLineageUtilImpl;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.controller.core.model.platform_call.TranslateXMLToCocason;
import org.eclipse.efbt.cocason.component.export_smcubes.ImportAndExportSMCubesToNewCocason;

public class Controller {
	
	public static void executPlatfromCall (PlatformCall call)
	{
		if (call instanceof GetAttributeLineageModel)
		{
			createAttributeLineageModel((GetAttributeLineageModel) call);
		}
		if (call instanceof TranslateBIRDToCocason)
		{
			translateBIRDToCocason((TranslateBIRDToCocason) call);
		}
	}
	
	private static void translateBIRDToCocason(TranslateBIRDToCocason call) {
		// TODO Auto-generated method stub
		ImportAndExportSMCubesToNewCocason.importAndExportSMCubes2(call.getInputDirectory(), call.getOutputDirectory(), call.getTestDirectory());
	}



	


}
