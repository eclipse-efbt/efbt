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
package org.eclipse.efbt.model.platformcalls;

import org.eclipse.efbt.generator.scala.engine.GeneratorConfig;
import org.eclipse.efbt.generator.scala.engine.GeneratorStandaloneSetup;
import org.eclipse.efbt.model.util.AttributeLineageUtil;
import org.eclipse.emf.mwe.utils.DirectoryCleaner;
import org.eclipse.emf.mwe2.runtime.workflow.IWorkflowContext;
import org.eclipse.emf.mwe2.runtime.workflow.WorkflowContextImpl;
import org.eclipse.xtext.generator.GeneratorComponent;
import org.eclipse.xtext.generator.GeneratorComponent.Outlet;
import org.eclipse.xtext.mwe.Reader;
import org.eclipse.xtext.mwe.ResourceLoadingSlotEntry;
import platform_call.ExecuteAttributeLineageModel;
import platform_call.GetAttributeLineageForOneReportCell;
import platform_call.GetAttributeLineageModel;

import platform_call.PlatformCall;

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
  
    
    if (call instanceof ExecuteAttributeLineageModel) {

      String inputPath = ((ExecuteAttributeLineageModel) call).getInputdir();
      String outputPath = ((ExecuteAttributeLineageModel) call).getOutputdir();
      GeneratorStandaloneSetup setup = new GeneratorStandaloneSetup();
      GeneratorConfig config = new org.eclipse.efbt.generator.scala.engine.GeneratorConfig();

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

    }

  }

}
