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
 *  Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.component.attributelineagecreator.platformcalls;

import java.io.File;



import platform_call.CompareAttributeLineageModels;
import platform_call.ExecuteAttributeLineageModel;
import platform_call.GetAttributeLineageForOneReportCell;
import platform_call.GetAttributeLineageModel;
import platform_call.GetTestLogic;
import platform_call.PlatformCall;

import org.eclipse.efbt.component.common.util.AttributeLineageUtil;
import org.eclipse.efbt.component.common.util.ComparisonUtil;

/**
 * @author Neil Mackenzie
 *
 */
public class PlatformCalls {

  public static void executeCall(PlatformCall call) {

    if (call instanceof GetAttributeLineageModel)
    	AttributeLineageUtil.createAttributeLineageModel((GetAttributeLineageModel) call);
    
    if (call instanceof GetAttributeLineageForOneReportCell)
    	AttributeLineageUtil.createAttributeLineageModelForOneReportCell((GetAttributeLineageForOneReportCell) call);
  
    if (call instanceof CompareAttributeLineageModels)
        ComparisonUtil.compareAttributeLineageModels((CompareAttributeLineageModels) call);
    
    if (call instanceof GetTestLogic)
    	AttributeLineageUtil.getTestLogic((GetTestLogic) call);
    
   
  }

private static void deleteAllFilesFromOutputPathExceptBaseClasses(String outputPath) {
	
	File dir = new File(outputPath);
	String[] files = dir.list(); 
	for (int i = 0; i < files.length; i++) {
		if (files[i].equals("base_types.scala") || files[i].equals("customfuncs.scala") || files[i].equals("funcs.scala"))
		{
			//do nothing
		}
		else
		{
			File f= new File(outputPath + "\\" + files[i]);
			f.delete();
		}
	}
	
	
}

private static void deleteFilesNotForALM(String outputPath, String name) {
	
	File dir = new File(outputPath);
	String[] files = dir.list(); 
	for (int i = 0; i < files.length; i++) {
		if (files[i].equals("base_types.scala") || files[i].equals("customfuncs.scala") 
				|| files[i].equals("funcs.scala") || files[i].startsWith(name))
		{
			//do nothing
		}
		else
		{
			File f= new File(outputPath + "\\" + files[i]);
			f.delete();
		}
	}
}

}
