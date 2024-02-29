/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDE-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/

package org.eclipse.efbt.regpot_desktop.orchestrator;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.Enumerator;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.efbt.regpot_desktop.csvconverter.EObjectLoader;
import org.eclipse.efbt.regpot_desktop.csvconverter.EObjectToCSVConverter;



public class Orchestration {
	
	//public static String fileDirectory = null;
	public static String resourceURI = "";
	
	public static boolean isSetUp = false;
	
	public static void main(String args[])
	{
		try {
			Class inputPackageClass  = Class.forName("input_tables.Input_tablesPackage");

			Field[] fields = inputPackageClass.getDeclaredFields();
			System.out.println("class = " + inputPackageClass.getName());
		    for (Field f : fields) {
		      System.out.println(f.getName());
		    }
		 
		    Field field = inputPackageClass.getDeclaredField("eNS_URI");
		    Field field2 = inputPackageClass.getDeclaredField("eINSTANCE");
			field.setAccessible(true);
			field2.setAccessible(true);
			Object nsuri = field.get(inputPackageClass);
			Object iPackage = field2.get(inputPackageClass);
			
			Class outputPackageClass  = Class.forName("output_tables.Output_tablesPackage");

			Field[] fields2 = outputPackageClass.getDeclaredFields();
			System.out.println("class = " + outputPackageClass.getName());
		    for (Field f2 : fields2) {
		      System.out.println(f2.getName());
		    }
		 
		    Field field3 = outputPackageClass.getDeclaredField("eNS_URI");
		    Field field4 = outputPackageClass.getDeclaredField("eINSTANCE");
		    field3.setAccessible(true);
			field4.setAccessible(true);
			Object nsuri2 = field3.get(outputPackageClass);
			Object iPackage2 = field4.get(outputPackageClass);
			
			Class reportcellsPackageClass  = Class.forName("report_cells.Report_cellsPackage");

			Field[] fields3 = reportcellsPackageClass.getDeclaredFields();
			System.out.println("class = " + reportcellsPackageClass.getName());
		    for (Field f3 : fields3) {
		      System.out.println(f3.getName());
		    }
		 
		    Field field5 = reportcellsPackageClass.getDeclaredField("eNS_URI");
		    Field field6 = reportcellsPackageClass.getDeclaredField("eINSTANCE");
		    field5.setAccessible(true);
			field6.setAccessible(true);
			Object nsuri3 = field5.get(reportcellsPackageClass);
			Object iPackage3 = field6.get(reportcellsPackageClass);

			
			EPackage.Registry.INSTANCE.put((String)nsuri,iPackage );
			EPackage.Registry.INSTANCE.put((String)nsuri2,iPackage2 );
			EPackage.Registry.INSTANCE.put((String)nsuri3,iPackage3 );

			} catch (ClassNotFoundException e) {

				e.printStackTrace();
			}
			 catch (SecurityException e) {

				e.printStackTrace();
			} catch (IllegalArgumentException e) {

				e.printStackTrace();
			}  catch (IllegalAccessException e) {

				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		
		String fullResourceURI = args[0];
		EObject theObject = EObjectLoader.loadObject(fullResourceURI);
		setup(theObject);
	}
	
	public static void setup(EObject theObject)
	{
		isSetUp = true;
		
		try {
			String platformURI = theObject.eResource().getURI().toFileString();
			String fileSeparator = FileSystems.getDefault().getSeparator();
			resourceURI = platformURI.substring(0,platformURI.lastIndexOf(fileSeparator) );
			EObjectToCSVConverter.resourceURI = resourceURI;
		} catch (Exception e) {
			e.printStackTrace();
		}

		Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		Map<String, Object> m = reg.getExtensionToFactoryMap();
		m.put("xmi", new XMIResourceFactoryImpl());

		EList<EOperation> operations = theObject.eClass().getEOperations();
		for (Iterator iterator2 = operations.iterator(); iterator2.hasNext();) {
			EOperation eOperation = (EOperation) iterator2.next();
			EList<EParameter> params = new BasicEList<EParameter>();
			if (eOperation.getName().equals("init"))
				try {
					theObject.eInvoke(eOperation, params);
				} catch (InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		}
		persistObject(theObject);
		EObjectToCSVConverter.persistObjectAsCSV(theObject,true);
		
		TreeIterator<EObject> contents = theObject.eAllContents();
		
		while (contents.hasNext())
		{
			EObject o = contents.next();
			operations = o.eClass().getEAllOperations();
			System.out.println(o.eClass().getName() +" {");
			for (Iterator iterator3 = operations.iterator(); iterator3.hasNext();) {
				EOperation eOperation = (EOperation) iterator3.next();
				EList<EParameter> params = new BasicEList<EParameter>();
				if (!eOperation.getName().equals("init") && !eOperation.getName().contains("_regscenario") && !eOperation.getName().contains("_private"))
					try {
						Object result = o.eInvoke(eOperation, params);
						System.out.println(result);
						
					} catch (InvocationTargetException e) {

						e.printStackTrace();
					}
					
			}
			System.out.println("}\n");
		}
	}

	public static void init(EObject theObject)
	{
		if (!isSetUp)
			setup(theObject);
		else
		{
			EClass eclass = theObject.eClass();
			EList<EReference> references = eclass.getEAllReferences();
			for (Iterator iterator = references.iterator(); iterator.hasNext();) {
				EReference eReference = (EReference) iterator.next();
				if (!eReference.isContainment() && eReference.getName().endsWith("Table"))
				{
					EObject newObject = EObjectLoader.findTableInXMIFile(eReference.getEType(), resourceURI);
					if(newObject != null) { 
						theObject.eSet(eReference, newObject);
						EObjectToCSVConverter.persistObjectAsCSV(newObject,true);
						
					}
					else
					{
						newObject = createObjectFromReferenceType( eReference);
						
						EClass newObjectsClass = newObject.eClass();
						EList<EOperation> operations = newObjectsClass.getEOperations();
						for (Iterator iterator2 = operations.iterator(); iterator2.hasNext();) {
							EOperation eOperation = (EOperation) iterator2.next();
							EList<EParameter> params = new BasicEList<EParameter>();
							if (eOperation.getName().equals("init"))
								try {
									newObject.eInvoke(eOperation, params);
								} catch (InvocationTargetException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								
						}
						persistObject(newObject);
						EObjectToCSVConverter.persistObjectAsCSV(newObject,true);						

						theObject.eSet(eReference, newObject);
					}
				}
				
			}
		}

	}

	private static void persistObject(EObject theObject) {
	        try {
	        	
	        	theObject.eResource().save(Collections.EMPTY_MAP);
			} catch (IOException e) {

				e.printStackTrace();
			} 
	}

	private static EObject createObjectFromReferenceType( EReference eReference) {
		
		EClass theClass = (EClass) eReference.getEType();
		
		Resource resource1 = EObjectLoader.resSet.createResource(URI.createFileURI( resourceURI + '/' +theClass.getName() + ".xmi"));
		
		EObject newObject = theClass.getEPackage().getEFactoryInstance().create(theClass);
		resource1.getContents().add(newObject);
		return newObject;
		
	}

}



