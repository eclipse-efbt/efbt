package org.eclipse.efbt.controller.smcubes.component.importexport.impl;

import com.google.inject.Injector;
import java.io.IOException;
import java.util.Map;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.xtext.resource.XtextResourceSet;

public class XMIToTextConverter {
	
	public static void main(String[] args)
	{
		ResourceSet resourceSet = new ResourceSetImpl();
		String filepath = "C:\\Users\\Neil\\freebirdtools-develop-jun22\\git\\efbt\\openregspecs\\python\\results\\";
		URI theURI = URI.createFileURI(filepath + "ldm.open_reg_specs");

        // register UML
        Map packageRegistry = resourceSet.getPackageRegistry();
        packageRegistry.put(Open_reg_specsPackage.eNS_URI, Open_reg_specsPackage.eINSTANCE);

        // Register XML resource as UMLResource.Factory.Instance
        Map extensionFactoryMap = Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap();
        extensionFactoryMap.put("open_reg_specs", new XMIResourceFactoryImpl());

        Resource resource = (Resource) resourceSet.createResource(theURI);

    
        // try to load the file into resource
        try {
			resource.load(null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println( resource.getContents().get(0) );
        
        Injector injector = new org.eclipse.efbt.openregspecs.dsl.OpenRegSpecsStandaloneSetup().createInjectorAndDoEMFRegistration();
        XtextResourceSet resourceSet2 = injector.getInstance(XtextResourceSet.class);
        Resource resource2 = resourceSet2.createResource(URI.createFileURI(filepath + "frtb_text.open_reg_specs"));
        resource2.getContents().add(resource.getContents().get(0));
        try {
        	resource2.save(null);
        } catch (IOException e) {e.printStackTrace();}
	}

}
