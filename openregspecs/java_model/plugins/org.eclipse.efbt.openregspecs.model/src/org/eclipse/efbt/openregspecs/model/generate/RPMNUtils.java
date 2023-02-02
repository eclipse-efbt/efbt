package org.eclipse.efbt.openregspecs.model.generate;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

import F_01_01_REF_Output_Logic.F_01_01_REF_Output_LogicPackage;
import input_layer_enums.Year_domain;
import output_layer_entities.F_01_01_REF_OutputTable;
import output_layer_entities.Output_layer_entitiesFactory;
import output_layer_entities.Output_layer_entitiesPackage;

public class RPMNUtils {
	
	public static String fileDirectory = null;
	
	public static ResourceSet resSet =  null;
	
	
	public static void main(String[] args)
	{
		EPackage.Registry.INSTANCE.put(F_01_01_REF_Output_LogicPackage.eNS_URI, F_01_01_REF_Output_LogicPackage.eINSTANCE);
		EPackage.Registry.INSTANCE.put(Output_layer_entitiesPackage.eNS_URI, Output_layer_entitiesPackage.eINSTANCE);
		resSet = new ResourceSetImpl();
		F_01_01_REF_OutputTable table = Output_layer_entitiesFactory.eINSTANCE.createF_01_01_REF_OutputTable();
		Year_domain result = table.init();
		System.out.println("result =" + result);
	}
	public static void init(EObject theObject)
	{
		if (resSet == null) 
			resSet = theObject.eResource().getResourceSet();
		Path path = FileSystems.getDefault().getPath(".");
		fileDirectory = path.toString();
		System.out.println("fileDirectory =" + System.getProperty("user.dir"));

		EClass eclass = theObject.eClass();
		EList<EReference> references = eclass.getEAllReferences();
		for (Iterator iterator = references.iterator(); iterator.hasNext();) {
			EReference eReference = (EReference) iterator.next();
			if (!eReference.isContainment())
			{
				EObject newObject = findTableInXMIFile(eReference.getEType(), eclass);
				if(newObject != null)
					theObject.eSet(eReference, newObject);
				else
				{
					newObject = createObjectFromReferenceType(theObject, eReference);
					
					EClass newObjectsClass = newObject.eClass();
					EList<EOperation> operations = newObjectsClass.getEOperations();
					for (Iterator iterator2 = operations.iterator(); iterator2.hasNext();) {
						EOperation eOperation = (EOperation) iterator2.next();
						EList<EParameter> params = new BasicEList<EParameter>();
						if (eOperation.getName().equals("initialise"))
							try {
								newObject.eInvoke(eOperation, params);
							} catch (InvocationTargetException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
					}
					persistObject(newObject, eReference);
					System.out.println("theObject" + theObject);
					System.out.println("eReference" + eReference);
					System.out.println("eReference.etype" + eReference.getEType().getName());
					theObject.eSet(eReference, newObject);
				}
			}
			
			
		}
	}
	
	
	private static void persistObject(EObject theObject, EReference eReference) {
		// TODO Auto-generated method stub
		
		
	        try {
	        	theObject.eResource().save(null);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	}


	private static EObject createObjectFromReferenceType(EObject theObject, EReference eReference) {
		
		EClass theClass = (EClass) eReference.getEType();
		Resource resource1 = resSet.createResource(URI.createFileURI(theClass.getName() + ".xmi"));
		theObject.
		resource1.getContents().add(theObject);
		// TODO Auto-generated method stub
		EObject newObject = theClass.getEPackage().getEFactoryInstance().create(theClass);
		  
		return newObject;
		
	}

	
	
	private static EObject findTableInXMIFile(EClassifier eClassifier, EClass eclass) {
		// TODO Auto-generated method stub
		EObject returnVal = null;
		EPackage ePackage = eclass.getEPackage(); 
		//look n the filesystem..maybe there should be a param with the fileDirectory.
		
		 
		 
		 
		 Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		 Map<String, Object> m = reg.getExtensionToFactoryMap();
		 m.put("xmi", new XMIResourceFactoryImpl());

		
		try{
		Resource resource1  = resSet.getResource(URI.createURI(eClassifier.getName() +".xmi"), true);

		
		resource1.load(Collections.EMPTY_MAP);
		returnVal = resource1.getContents().get(0);
		}
		catch (Exception e) {
						// TODO Auto-generated catch block
						System.out.println(e);
					}
	
		return returnVal;
				
	}
	/**
	 * 
	 * public static void initilaise(EObject theObject)
	{
		//for each reference, check if we have a file, if not create the object and intialise it, then asign it
		EObject o = findTableInResource(tableName);
		if (o == null)
		{
			o = createAndInitialiseObject(tableName);
		}
	}
	private static EObject createAndInitialiseObject(String tableName) {
		// TODO Auto-generated method stub
		EObject o = findTableInXMIFile(tableName);
		if (o == null)
		{
			o = createResource(tableName);
		}
		resource.getContents().add(o);
		return o;
	}

	private static EObject createResource(String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	

	public static EObject findTableInResource (EClassifier eClassifier) {
		TreeIterator<EObject> contents = resource.getAllContents();
		EObject returnVal = null;
		while (contents.hasNext())
		{
			EObject object = contents.next();
			if(object instanceof EClass)
			{
				String name = ((EClass) object).getName();
				if(name.equals(eClassifier) )
				{
					returnVal = object;
				}
				
			}
			
		}
		
		return returnVal;
	}
*/
}
