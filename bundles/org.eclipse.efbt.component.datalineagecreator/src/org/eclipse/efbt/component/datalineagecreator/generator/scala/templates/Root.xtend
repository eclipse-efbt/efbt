/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Neil Mackenzie - initial API and implementation
 *******************************************************************************/
 package org.eclipse.efbt.component.datalineagecreator.generator.scala.templates

import javax.inject.Inject
import org.eclipse.emf.ecore.resource.Resource
import org.eclipse.xtext.generator.IFileSystemAccess
import org.eclipse.xtext.generator.IGenerator
import org.eclipse.efbt.component.datalineagecreator.generator.scala.templates.JavaBeanTemplate

class Root implements IGenerator { 
	@Inject JavaBeanTemplate javaBeanTemplate

	// add more templates here
	
	override doGenerate(Resource input, IFileSystemAccess fsa) {
		javaBeanTemplate.doGenerate(input,fsa)
	
	}
	
} 