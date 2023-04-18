/**Copyright (c) 2020 Bird Software Solutions Ltd
 All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html

SPDX-License-Identifier: EPL-2.0 

 Contributors:
 Neil Mackenzie - initial API and implementation*/
package org.eclipse.efbt.openregspecs.dsl.generator

import org.eclipse.emf.ecore.resource.Resource
import org.eclipse.xtext.generator.AbstractGenerator
import org.eclipse.xtext.generator.IFileSystemAccess2
import org.eclipse.xtext.generator.IGeneratorContext
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XClass
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XPackage
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XAttribute
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XReference
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XEnum
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XDataType
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XOperation
import org.eclipse.efbt.openregspecs.model.open_reg_specs.UserTask
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ServiceTask
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Module
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ViewModule
import org.eclipse.efbt.openregspecs.model.open_reg_specs.WorkflowModule
import java.io.File

/**
 * Generates code from your model files on save.
 * 
 * See https://www.eclipse.org/Xtext/documentation/303_runtime_concepts.html#code-generation
 */
class OpenRegSpecsGenerator extends AbstractGenerator {

	override void doGenerate(Resource resource, IFileSystemAccess2 fsa, IGeneratorContext context) {
		System.out.println("resource = " + resource)
		        for (xpackage : resource.allContents.toIterable.filter(XPackage)) {
                System.out.println("package =  " + xpackage.name)

         }


}

}

 
	
 