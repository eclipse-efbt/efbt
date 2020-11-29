/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.controller.core.component.core;



import org.eclipse.efbt.controller.core.model.platform_call.ConvertSQLDeveloperModelToEcore;
import org.eclipse.efbt.controller.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason;
import org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason;
import org.eclipse.efbt.ldm.component.sqldevconvertor.SQLDevConverter;
import org.eclipse.efbt.cocason.component.export_smcubes.ImportAndExportSMCubesToNewCocason;

public class Controller {
	
	public static void executPlatfromCall (PlatformCall call)
	{
		
		if (call instanceof TranslateBIRDWithOldTestFormatToCocason)
		{
			translateBIRDToCocason((TranslateBIRDWithOldTestFormatToCocason) call);
		}
		if (call instanceof TranslateBIRDWithNewTestFormatToCocason)
		{
			translateBIRDWithNewTestFormatToCocason((TranslateBIRDWithNewTestFormatToCocason) call);
		}
		if (call instanceof ConvertSQLDeveloperModelToEcore)
		{
			convertSQLDeveloperModelToEcore((ConvertSQLDeveloperModelToEcore) call);
		}
		
	}
	
	private static void translateBIRDWithNewTestFormatToCocason(TranslateBIRDWithNewTestFormatToCocason call) {
		// TODO Auto-generated method stub
		ImportAndExportSMCubesToNewCocason.translateBIRDWithNewTestFormatToCocason(call.getInputDirectory(), call.getOutputDirectory(), call.getTestDirectory());
	}
	
	private static void translateBIRDToCocason(TranslateBIRDWithOldTestFormatToCocason call) {
		// TODO Auto-generated method stub
		ImportAndExportSMCubesToNewCocason.translateBIRDWithOldTestFormatToCocason(call.getInputDirectory(), call.getOutputDirectory(), call.getTestDirectory());
	}
	
	private static void convertSQLDeveloperModelToEcore(ConvertSQLDeveloperModelToEcore call) {
		// TODO Auto-generated method stub
		SQLDevConverter.convert(call.getInputDirectory());
	}
	
	



	


}
