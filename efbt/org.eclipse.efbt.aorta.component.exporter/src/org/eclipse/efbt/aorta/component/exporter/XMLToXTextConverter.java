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

import aorta_program.Aorta_programPackage;

public class XMLToXTextConverter {



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
		ResourceSet resSet = i.getInstance(ResourceSet.class);

		// now pesist these as XText files

		URI domainsProgramURI = URI.createFileURI(inputpath + "domains.aorta_program");
		URI inputDataStructuresProgramURI = URI.createFileURI(inputpath + "inputDataStructures.aorta_program");


		Resource domainsResource = resSet.createResource(domainsProgramURI);
		Resource inputDataStructuresResource = resSet.createResource(inputDataStructuresProgramURI);

	

		domainsResource.load(Collections.EMPTY_MAP);
		inputDataStructuresResource.load(Collections.EMPTY_MAP);

		EcoreUtil.resolveAll(resSet);

		EObject domainsRoot = domainsResource.getContents().get(0);
		EObject inputDataStructuresRoot = inputDataStructuresResource.getContents().get(0);

		XtextResourceSet xrs = i.getInstance(XtextResourceSet.class);

		URI trgtURI = URI.createFileURI("C:/Users/Neil/output3/domains.dsl");

		URI trgtURI2 = URI.createFileURI("C:/Users/Neil/output3/inputDataStructures.dsl");

		Resource domainsResource2 = xrs.createResource(trgtURI);
		Resource inputDataStructuresResource2 = xrs.createResource(trgtURI2);

		domainsResource2.getContents().add(domainsRoot);

		inputDataStructuresResource2.getContents().add(inputDataStructuresRoot);

		EcoreUtil.resolveAll(xrs);

		/**IQualifiedNameConverter converter = i.getInstance(IQualifiedNameConverter.class);
		Registry rspr = IResourceServiceProvider.Registry.INSTANCE;
		IResourceServiceProvider resServiceProvider = rspr.getResourceServiceProvider(trgtURI);

		Manager manager = resServiceProvider.getResourceDescriptionManager();

		IResourceDescription description = manager.getResourceDescription(domainsResource2);
		Iterable<IEObjectDescription> eods = description.getExportedObjects();
		for (IEObjectDescription ieObjectDescription : eods) {
			System.out.println(converter.toString(ieObjectDescription.getQualifiedName()));
		}

		IContainer.Manager manager2 = i.getInstance(IContainer.Manager.class);
		System.out.println("manager2 = " + manager2);
		ResourceDescriptionsProvider rdp = i.getInstance(ResourceDescriptionsProvider.class);

		System.out.println("rdp = " + rdp);

		IResourceDescriptions rds1 = rdp.getResourceDescriptions(inputDataStructuresResource2);
		System.out.println("rds1 = " + rds1);

		IResourceDescription descr = rds1.getResourceDescription(trgtURI2);
		List<IContainer> visconts = manager2.getVisibleContainers(descr, rds1);
		for (IContainer iContainer : visconts) {
			Iterable<IResourceDescription> abcd = iContainer.getResourceDescriptions();
			for (IResourceDescription abc : abcd) {
				System.out.println("abc = " + abc.getURI());
			}

		}*/

//		Serializer serializer = i.getInstance(Serializer.class);
		
		domainsResource2.save(Collections.EMPTY_MAP);
		inputDataStructuresResource2.save(Collections.EMPTY_MAP);
		

	}

	

	public static void main(String[] args) {
		XMLToXTextConverter converter = new XMLToXTextConverter();
		try {
			//converter.convertXMLToXtext("C:/Users/Neil/output/", "C:/Users/Neil/output3/");
			converter.convertXMLToXtext(args[0],args[1]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
