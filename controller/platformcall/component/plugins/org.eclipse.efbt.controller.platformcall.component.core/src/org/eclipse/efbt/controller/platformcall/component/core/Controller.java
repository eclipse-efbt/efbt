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
package org.eclipse.efbt.controller.platformcall.component.core;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.*;
import org.eclipse.efbt.controller.ldm.component.testing.LogicalTransformationViews;
import org.eclipse.efbt.controller.smcubes.component.importexport.impl.ImportOfBIRDFromMSAccess;

public class Controller {
	
	public static void executPlatfromCall (PlatformCall call)
	{
		if (call instanceof ImportBIRDFromMSAccess)
		{
			ImportBIRDFromMSAccess((ImportBIRDFromMSAccess) call);
		}
		if (call instanceof CreateLogicalTransformationViewForScope)
		{
			createLogicalTransformationViewForScope((CreateLogicalTransformationViewForScope) call);
		}
		
	}

	private static void ImportBIRDFromMSAccess(ImportBIRDFromMSAccess call) {
		// TODO Auto-generated method stub
		ImportOfBIRDFromMSAccess.importBIRDFromMSAccess(call.getInputDirectory(), call.getOutputDirectory());
	}

	
	
	private static void createLogicalTransformationViewForScope(CreateLogicalTransformationViewForScope call) {
		// TODO Auto-generated method stub
		LogicalTransformationViews.createLogicalTransformationViewForScope(call.getLogicalTransformationModule(),
																			call.getTestScope(),
																			call);
	}
	


}
