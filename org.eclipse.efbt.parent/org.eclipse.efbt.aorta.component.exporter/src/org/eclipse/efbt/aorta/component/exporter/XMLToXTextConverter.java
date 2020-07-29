
package org.eclipse.efbt.aorta.component.exporter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.eclipse.efbt.aorta.AortaRuntimeModule;
import org.eclipse.efbt.aorta.AortaStandaloneSetup;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.mwe.utils.StandaloneSetup;
import org.eclipse.xtext.naming.IQualifiedNameConverter;
import org.eclipse.xtext.resource.IContainer;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.resource.IResourceDescription;
import org.eclipse.xtext.resource.IResourceDescription.Manager;
import org.eclipse.xtext.resource.IResourceDescriptions;
import org.eclipse.xtext.resource.IResourceServiceProvider;
import org.eclipse.xtext.resource.IResourceServiceProvider.Registry;
import org.eclipse.xtext.resource.XtextResourceSet;
import org.eclipse.xtext.resource.containers.DelegatingIAllContainerAdapter;
import org.eclipse.xtext.resource.containers.ResourceSetBasedAllContainersState;
import org.eclipse.xtext.resource.containers.ResourceSetBasedAllContainersStateProvider;
import org.eclipse.xtext.resource.impl.ResourceDescriptionsData;
import org.eclipse.xtext.resource.impl.ResourceDescriptionsProvider;
import org.eclipse.xtext.scoping.IScopeProvider;
import org.eclipse.xtext.serializer.impl.Serializer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

import org.eclipse.efbt.aorta.aorta_program.Aorta_programPackage;

/**
 * XMLToXTextConverter is responsible for taking an ecore model instance
 * representing a 'Program', which has been serialised to XML format, and
 * creating an XText representation of this according to the grammar defined in
 * the Aorta XText grammar.
 * 
 * Note that the Aorta XText grammar is generated from the Ecore model
 * representing 'Program' s.
 * 
 * @author Neil Mackenzie
 *
 *
 */
public class XMLToXTextConverter {

	/**
	 * Convert from xml files on the outpath to new xtext files on the output path.
	 * For no we limit this to domains and input data structures
	 * 
	 * @param inputpath
	 * @param outputPath
	 * @throws IOException
	 */
	public void convertXMLToXtext(String inputpath, String outputPath) throws IOException {
		// load the set of files into a resource set.
		// persist the files as a set of xtext files

		EPackage.Registry.INSTANCE.put("http://www.eclipse.org/aorta_program", Aorta_programPackage.eINSTANCE);
		FreeBirdToolsResourceFactory factory = new FreeBirdToolsResourceFactory();

		Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		Map<String, Object> m = reg.getExtensionToFactoryMap();
		m.put("aorta_program", factory);

		AortaStandaloneSetup setup = new AortaStandaloneSetup();
		Injector i = setup.createInjectorAndDoEMFRegistration();
		
		//load the xml files into a resource, using the FreeBirdToolsResourceFactory
		ResourceSet resSet = i.getInstance(ResourceSet.class);

		
		URI domainsProgramURI = URI.createFileURI(
				inputpath + "domains.aorta_program");
		URI inputDataStructuresProgramURI = URI.createFileURI(
				inputpath + "inputDataStructures.aorta_program");

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
		XtextResourceSet xrs = i.getInstance(XtextResourceSet.class);

		URI trgtURI = URI.createFileURI("C:/Users/Neil/output3/domains.dsl");

		URI trgtURI2 = URI.
				createFileURI("C:/Users/Neil/output3/inputDataStructures.dsl");

		Resource domainsResource2 = xrs.createResource(trgtURI);
		Resource inputDataStructuresResource2 = xrs.createResource(trgtURI2);

		domainsResource2.getContents().add(domainsRoot);

		inputDataStructuresResource2.getContents().
						add(inputDataStructuresRoot);

		EcoreUtil.resolveAll(xrs);
		
		//save the xtext representation of the model
		domainsResource2.save(Collections.EMPTY_MAP);
		inputDataStructuresResource2.save(Collections.EMPTY_MAP);

	}

	/**
	 * main method to retrieve the input and output file paths as argumanets
	 * and call the conversion.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		XMLToXTextConverter converter = new XMLToXTextConverter();
		try {
			
			converter.convertXMLToXtext(args[0], args[1]);
		} catch (IOException e) {
		
			e.printStackTrace();
		}

	}

}
