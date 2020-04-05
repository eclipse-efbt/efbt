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
package org.eclipse.efbt.component.datalineagecreator.platformcalls;

import java.io.File;

import org.eclipse.efbt.component.datalineagecreator.generator.scala.engine.GeneratorConfig;
import org.eclipse.efbt.component.datalineagecreator.generator.scala.engine.GeneratorStandaloneSetup;

import org.eclipse.emf.mwe.utils.DirectoryCleaner;
import org.eclipse.emf.mwe2.runtime.workflow.IWorkflowContext;
import org.eclipse.emf.mwe2.runtime.workflow.WorkflowContextImpl;
import org.eclipse.xtext.generator.GeneratorComponent;
import org.eclipse.xtext.generator.GeneratorComponent.Outlet;
import org.eclipse.xtext.mwe.Reader;
import org.eclipse.xtext.mwe.ResourceLoadingSlotEntry;


import platform_call.ExecuteAttributeLineageModel;

import platform_call.PlatformCall;

/**
 * @author Neil Mackenzie
 *
 */
public class DLMPlatformCalls {

  public static void executeCall(PlatformCall call) {

  
    
    if (call instanceof ExecuteAttributeLineageModel) {

      String inputPath = ((ExecuteAttributeLineageModel) call).getInputdir();
      String outputPath = ((ExecuteAttributeLineageModel) call).getOutputdir();
      deleteAllFilesFromOutputPathExceptBaseClasses(outputPath);
      GeneratorStandaloneSetup setup = new GeneratorStandaloneSetup();
      GeneratorConfig config = new org.eclipse.efbt.component.datalineagecreator.generator.scala.engine.GeneratorConfig();

      config.setJavaGenPath(inputPath);
      setup.setConfig(config);

      setup.setDoInit(true);

      Reader reader = new Reader();

      reader.addPath(inputPath);
      reader.addRegister(setup);
      ResourceLoadingSlotEntry rlse = new ResourceLoadingSlotEntry();
      rlse.setSlot("model");
      reader.addLoadResource(rlse);

      GeneratorComponent gen = new GeneratorComponent();
      gen.setRegister(setup);
      gen.addSlot("model");
      Outlet out = new Outlet();

      out.setPath(outputPath);
      gen.addOutlet(out);

      IWorkflowContext ctx = new WorkflowContextImpl();
      DirectoryCleaner dc = new DirectoryCleaner();
      reader.invoke(ctx);
      dc.invoke(ctx);
      gen.invoke(ctx);
      
      deleteFilesNotForALM(outputPath, ((ExecuteAttributeLineageModel) call).
    		  getName() +"_");

    }

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
