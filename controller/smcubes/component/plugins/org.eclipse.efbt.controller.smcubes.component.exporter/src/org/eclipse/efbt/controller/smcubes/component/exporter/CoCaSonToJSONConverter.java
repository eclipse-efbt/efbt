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

package org.eclipse.efbt.controller.smcubes.component.exporter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
//import org.eclipse.xtext.resource.XtextResourceSet;

//import com.google.inject.Injector;


/**
 * CoCaSonToJSONConverter
 * 
 * @author Neil Mackenzie
 *
 *
 */
public class CoCaSonToJSONConverter {

	/**
	 * Convert from xml files on the outpath to new xtext files on the output path.
	 * For no we limit this to domains and input data structures
	 * 
	 * @param inputpath
	 * @param outputPath
	 * @throws IOException
	 */
	public void convertCoCaSonToJSON(String inputpath, String outputPath) throws IOException {
		// make a list of all the file names
		// for each file name load the resource
		// resolve all file names
		// for each filename get the resource and make a new json reource 
		//try this with domains and members first

		//EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/cocamo", CocamoPackage.eINSTANCE);
		/**CoCaSonStandaloneSetup setup = new CoCaSonStandaloneSetup();
		Injector i = setup.createInjectorAndDoEMFRegistration();
		XtextResourceSet xrs = i.getInstance(XtextResourceSet.class);
		URI srctURI = URI.createFileURI("C:/Users/Neil/output3/domains.dsl");

		URI srcURI2 = URI.
				createFileURI("C:/Users/Neil/output3/inputDataStructures.dsl");

		Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		

		
		
		//load the xml files into a resource, using the FreeBirdToolsResourceFactory
		ResourceSet resSet = i.getInstance(ResourceSet.class);

		
		URI domainsProgramURI = URI.createFileURI(
				inputpath + "domains.cocamo_program");
		URI inputDataStructuresProgramURI = URI.createFileURI(
				inputpath + "inputDataStructures.cocamo_program");

		Resource domainsResource = resSet.
				createResource(domainsProgramURI);
		Resource inputDataStructuresResource = resSet.
				createResource(inputDataStructuresProgramURI);

		domainsResource.load(Collections.EMPTY_MAP);
		inputDataStructuresResource.load(Collections.EMPTY_MAP);

		//make sure we resolve all proxies or we have problems.
		EcoreUtil.resolveAll(resSet);

		EObject domainsRoot = domainsResource.getContents().get(0);
		EObject inputDataStructuresRoot = 
				inputDataStructuresResource.getContents().get(0);

		// now create an xtext resource from the resource
		FreeBirdToolsResourceFactory factory = new FreeBirdToolsResourceFactory();
		Map<String, Object> m = reg.getExtensionToFactoryMap();
		m.put("cocamo", factory);
		

		

		URI trgtURI = null;
		Resource domainsResource2 = xrs.createResource(trgtURI);
		URI trgtURI2 = null;
		Resource inputDataStructuresResource2 = xrs.createResource(trgtURI2);

		domainsResource2.getContents().add(domainsRoot);

		inputDataStructuresResource2.getContents().
						add(inputDataStructuresRoot);

		EcoreUtil.resolveAll(xrs);
		
		//save the xtext representation of the model
		domainsResource2.save(Collections.EMPTY_MAP);
		inputDataStructuresResource2.save(Collections.EMPTY_MAP);*/

	}

	/**
	 * main method to retrieve the input and output file paths as argumanets
	 * and call the conversion.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		CoCaSonToJSONConverter converter = new CoCaSonToJSONConverter();
		try {
			
			converter.convertCoCaSonToJSON(args[0], args[1]);
		} catch (IOException e) {
		
			e.printStackTrace();
		}

	}

}
