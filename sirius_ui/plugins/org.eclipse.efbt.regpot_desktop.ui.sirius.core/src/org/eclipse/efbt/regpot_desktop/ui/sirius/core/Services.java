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
package org.eclipse.efbt.regpot_desktop.ui.sirius.core;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EAnnotation;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.sirius.diagram.DDiagramElement;
import org.eclipse.sirius.diagram.DSemanticDiagram;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

import org.eclipse.efbt.regdna.model.regdna.*;
import org.eclipse.efbt.regdna.model.regdna.ELAttribute;
import org.eclipse.efbt.regdna.model.regdna.ELClass;
import org.eclipse.efbt.regdna.model.regdna.ELOperation;
import org.eclipse.efbt.regdna.model.regdna.SelectColumn;
import org.eclipse.efbt.regdna.model.regdna.SelectColumnAttributeAs;




/**
 * The services class used by VSM.
 */
public class Services {
    
	public static Path logFilePath = Path.of("logfile.txt");
	public static boolean debug = false;
    public String getInvokedOperation(EOperation self,DSemanticDiagram diagram,DDiagramElement view) {

    	DDiagramElement  container = (DDiagramElement) view.eContainer();
    	if(debug)
    	try {
			Files.writeString(logFilePath, " container " + container , StandardOpenOption.CREATE);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}

    	EObject containersTarget = container.getTarget();
    	if(debug)
    	try {
			Files.writeString(logFilePath, " container target " + containersTarget , StandardOpenOption.APPEND);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	
    	EObject viewTarget = view.getTarget(); 
    	if(debug)
    	try {
			Files.writeString(logFilePath, " viewTarget " + viewTarget , StandardOpenOption.APPEND);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	String result = "def";		
    	try {
    		result =  (containersTarget.eInvoke((EOperation)viewTarget, null)).toString();
		} catch (InvocationTargetException e) {
			if(debug)
		    try {
				Files.writeString(logFilePath, " InvocationTargetException e " + e.toString() , StandardOpenOption.APPEND);
				StringWriter writer = new StringWriter();
				PrintWriter printWriter = new PrintWriter( writer );
				e.printStackTrace( printWriter );
				printWriter.flush();

				String stackTrace = writer.toString();
				Files.writeString(logFilePath, " InvocationTargetException e " + stackTrace , StandardOpenOption.APPEND);
				
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
    	catch (Exception e1) {
    		if(debug)
    		try {
				Files.writeString(logFilePath, " Exception e " + e1.toString() , StandardOpenOption.APPEND);
				
			} catch (IOException e2) {
				e2.printStackTrace();
			}
		}
    	try {
			Files.writeString(logFilePath, " result " + result, StandardOpenOption.APPEND);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	return result;
     }
    
    public String getAttributeValue(EAttribute self,DSemanticDiagram diagram,DDiagramElement view) {

    	DDiagramElement  container = (DDiagramElement) view.eContainer();
    	System.out.println(" container " + container);
    	EObject containersTarget = container.getTarget();
    	System.out.println(" container target " + containersTarget);
    	EObject viewTarget = view.getTarget(); 
    	String result = "abc";		
    	result =  (containersTarget.eGet((EAttribute)viewTarget, true)).toString();
    	return result;
     }
    
    public EList<EObject> getReferencedObjects(EObject self)
    {	
		return getReferencedObjectsList(self.eCrossReferences());
    }
    
    public EList<EOperation> getPublicOperations(EObject self)
    {	
    	EList<EOperation> returnList = new BasicEList<EOperation>();
    	EList<EOperation> operations = self.eClass().getEOperations();
		for (EOperation eOperation : operations) {
			if (!eOperation.getName().endsWith("_private"))
			{
				returnList.add(eOperation);
			}
		}
		return returnList;
    }
    
    public EList<EObject> getReferencedObjectsList(EList<EObject> eObjects)
    {
    	EList<EObject> list = new BasicEList<EObject> ();
		for (EObject eObject : eObjects) {
			list.add(eObject);
			list.addAll(getReferencedObjects(eObject));
		}
		return list;
    	
    	
    }
    
    public EList<ELAttribute>  getUsedAttributes(ELClass self,DSemanticDiagram diagram)
    {
    	EObject target = diagram.getTarget(); 
    	EList<ELAttribute> returnList = new BasicEList<ELAttribute>();
    	TreeIterator<EObject> contents = target.eAllContents();
    	while (contents.hasNext()) {
    		EObject eObject = contents.next();
			if (eObject instanceof SelectColumnAttributeAs)
			{
				ELAttribute attr = ((SelectColumnAttributeAs) eObject).getAttribute();
				if (attr.eContainer().equals(self))
				{
					returnList.add(attr);
				}
				
			}
		}
    	return returnList;
    	
    }
    
    public EList<ELOperation>  getUsedOperations(ELClass self,DSemanticDiagram diagram)
    {
    	EObject target = diagram.getTarget(); 
    	EList<ELOperation> returnList = new BasicEList<ELOperation>();
    	TreeIterator<EObject> contents = target.eAllContents();
    	while (contents.hasNext()) {
    		EObject eObject = contents.next();
			if (eObject instanceof SelectDerivedColumnAs)
			{
				ELOperation oper = ((SelectDerivedColumnAs) eObject).getAttribute();
				if (oper.eContainer().equals(self))
				{
					returnList.add(oper);
				}
			}
		}
    	return returnList;
    	
    }
    
    public EList<EOperation> getUsedInvokedOperations(EObject self,DSemanticDiagram diagram,DDiagramElement view) {

    	EList<EOperation> usedOperations = new BasicEList<EOperation>();

    	EList<ClassAndFeaturePair> pairs = getListOfUsedClassAndFeaturePairs(diagram.getTarget());
    	EList<EOperation> operation = self.eClass().getEOperations();
    	for (EOperation eOperation : operation) {
			if(!eOperation.getName().endsWith("_private") && !eOperation.getName().contains("regScenario"))
			{
				boolean inList = false;
				for (ClassAndFeaturePair pair : pairs) {
					String theClassName = self.eClass().getName();
					String theSuperClassName = "NoSuperClass";
					EList<EClass> theSuperClasses = self.eClass().getESuperTypes();
					
					if (theSuperClasses.size() > 0)
						theSuperClassName = theSuperClasses.get(0).getName();
					
					if (eOperation.getName().equalsIgnoreCase(pair.attributeOrOperationName) 
							&& ((theClassName.equalsIgnoreCase(pair.className))
							|| (theSuperClassName.equalsIgnoreCase(pair.className))))
					{
						inList =true;
					}
				}
				if(inList)
					usedOperations.add(eOperation);
			}
		}
    	return usedOperations;
     }
    
    
    private EList<ClassAndFeaturePair>  getListOfUsedClassAndFeaturePairs(EObject root)
    {
    	EList<ClassAndFeaturePair> returnList = new BasicEList<ClassAndFeaturePair>();
    	EList<EObject> allObjects = getReferencedObjects(root);
    	allObjects.add(root);
    	if(debug)
        try {
			Files.writeString(logFilePath, "getListOfUsedClassAndFeaturePairs" + root.eClass().getName() , StandardOpenOption.APPEND);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	for (EObject eObject : allObjects) {
    		EList<EObject> containedObjects = eObject.eContents();
    		if(containedObjects.size() > 0)
    		{
    			
    			EClass objectsClass = containedObjects.get(0).eClass();
	    		EList<EOperation> operations = objectsClass.getEOperations();
	    		for (EOperation operation : operations) {
	    			if(debug)
	    		    try {
	    				Files.writeString(logFilePath, "operation = " + operation.getName() , StandardOpenOption.APPEND);
	    				
	    			} catch (IOException e1) {
	    				e1.printStackTrace();
	    			}
						EList<EAnnotation> annotations = operation.getEAnnotations();
						for (EAnnotation annotation : annotations) {
							if(debug)
						    try {
			    				Files.writeString(logFilePath, "annotation = " + annotation.toString() , StandardOpenOption.APPEND);
			    				
			    			} catch (IOException e1) {
			    				e1.printStackTrace();
			    			}
							if (annotation.getSource() == "dep")
							{
								if(debug)
							    try {
				    				Files.writeString(logFilePath, "annotation.getSource() = " + annotation.getSource() , StandardOpenOption.APPEND);
				    				
				    			} catch (IOException e1) {
				    				e1.printStackTrace();
				    			}
								EMap<String, String> details = annotation.getDetails();
								
								for (String key : details.keySet())
								{	
									if(debug)
								    try {
					    				Files.writeString(logFilePath, "key = " + key , StandardOpenOption.APPEND);
					    				
					    			} catch (IOException e1) {
					    				e1.printStackTrace();
					    			}
									if(debug)
								    try {
					    				Files.writeString(logFilePath, "details.get(key) = " + details.get(key) , StandardOpenOption.APPEND);
					    				
					    			} catch (IOException e1) {
					    				e1.printStackTrace();
					    			}
									catch (Exception e1) {
										if(debug)
									    try {
						    				Files.writeString(logFilePath, "error = " + e1.toString() , StandardOpenOption.APPEND);
						    				
						    			} catch (IOException e2) {
						    				e1.printStackTrace();
						    			}
					    			}
									String[] sections = details.get(key).split("\\.");
									if(debug)
								    try {
										Files.writeString(logFilePath, "sections.length = " + sections.length, StandardOpenOption.APPEND);
										Files.writeString(logFilePath, "sections[0] = " + sections[0], StandardOpenOption.APPEND);
					    				
					    				
					    			} 
									catch (Exception e1) {
										if(debug)
									    try {
						    				Files.writeString(logFilePath, "error = " + e1.toString() , StandardOpenOption.APPEND);
						    				
						    			} catch (IOException e2) {
						    				e1.printStackTrace();
						    			}
						    			
								}
									if(debug)
								    try {
					    				Files.writeString(logFilePath, "sections.length = " + sections.length , StandardOpenOption.APPEND);
					    				
					    			} catch (IOException e1) {
					    				e1.printStackTrace();
					    			}
									if (sections.length > 1)
									{
										ClassAndFeaturePair classAndFeaturePair = new ClassAndFeaturePair();
										if(debug)
									    try {
						    				Files.writeString(logFilePath, "(EClass)operation.eContainer() =" + (EClass)operation.eContainer() , StandardOpenOption.APPEND);
						    				
						    			} catch (IOException e1) {
						    				e1.printStackTrace();
						    			}
										String className = getEClassAssociatedWithReference((EClass)operation.eContainer(),sections[0]);
										if(debug)
									    try {
						    				Files.writeString(logFilePath, "className = " + className , StandardOpenOption.APPEND);
						    				
						    			} catch (IOException e1) {
						    				e1.printStackTrace();
						    			}
										classAndFeaturePair.className = className;
										classAndFeaturePair.attributeOrOperationName = sections[1];
										returnList.add(classAndFeaturePair);
									}
									else
									{
										System.out.println("couldnt split " + details.get(key));
										if(debug)
									    try {
						    				Files.writeString(logFilePath, "couldnt split " + details.get(key) , StandardOpenOption.APPEND);
						    				
						    			} catch (IOException e1) {
						    				e1.printStackTrace();
						    			}
									}
							}
						}
				}
    		}
		}
    	}
		return returnList;
    }

	private String getEClassAssociatedWithReference(EClass eClass, String refName) {
		// TODO Auto-generated method stub
		String returnValue = null;
		
		EList<EReference> references = eClass.getEReferences();
		
		if(debug)
	    try {
			Files.writeString(logFilePath, "references.size = " + references.size() , StandardOpenOption.APPEND);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (EReference eReference : references) {
			String referenceName = eReference.getName();
			if(debug)
		    try {
				Files.writeString(logFilePath, "referenceName = " + referenceName , StandardOpenOption.APPEND);
				
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			if(debug)
		    try {
				Files.writeString(logFilePath, "refName = " + refName , StandardOpenOption.APPEND);
				
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			if(referenceName.equalsIgnoreCase(refName))
			{
				returnValue = eReference.getEType().getName();
				if(debug)
			    try {
					Files.writeString(logFilePath, "returnValue = " + returnValue , StandardOpenOption.APPEND);
					
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
		
		return returnValue;
		
	}
	
	public EList<EAttribute> getUsedAttributeValues(EObject self,DSemanticDiagram diagram,DDiagramElement view) {

    	EList<EAttribute> usedAttributes = new BasicEList<EAttribute>();

    	EList<ClassAndFeaturePair> pairs = getListOfUsedClassAndFeaturePairs(diagram.getTarget());
    	EList<EAttribute> attributes = self.eClass().getEAttributes();
    	for (EAttribute eAttribute : attributes) {
			if(true)
			{
				boolean inList = false;
				String theClassName = self.eClass().getName();
				String theSuperClassName = "NoSuperClass";
				EList<EClass> theSuperClasses = self.eClass().getESuperTypes();
				if (theSuperClasses.size() > 0)
					theSuperClassName = theSuperClasses.get(0).getName();
				
				for (ClassAndFeaturePair pair : pairs) {
					if (eAttribute.getName().equalsIgnoreCase(pair.attributeOrOperationName) 
							&& ((theClassName.equalsIgnoreCase(pair.className))
							|| (theSuperClassName.equalsIgnoreCase(pair.className))))
					{
						inList =true;
					}
				}
				if(inList)
					usedAttributes.add(eAttribute);
			}
		}
    	return usedAttributes;
     }
	
	public EList<EObject> getUsedReferencedObjects(EObject self)
    {	
		EList<EObject> referencedObjects = getReferencedObjects(self);
		EList<EObject> usedReferencedObjects = new BasicEList<EObject>();
		EList<ClassAndFeaturePair> pairs = getListOfUsedClassAndFeaturePairs(self);
		for (EObject eObject :referencedObjects) {
			EList<EObject> containedObjects = eObject.eContents();
    		if(containedObjects.size() > 0)
    		{
    			
    			EClass objectsClass = containedObjects.get(0).eClass();
				EList<EAttribute> attributes = objectsClass.getEAttributes();
				int featureCount = 0;
		    	for (EAttribute eAttribute : attributes) {
					if(true)
					{
						String theClassName = objectsClass.getName();
						String theSuperClassName = "NoSuperClass";
						EList<EClass> theSuperClasses = objectsClass.getESuperTypes();
						if (theSuperClasses.size() > 0)
							theSuperClassName = theSuperClasses.get(0).getName();
						
						for (ClassAndFeaturePair pair : pairs) {
							if (eAttribute.getName().equalsIgnoreCase(pair.attributeOrOperationName) 
									&& ((theClassName.equalsIgnoreCase(pair.className))
									|| (theSuperClassName.equalsIgnoreCase(pair.className))))
							{
								featureCount++;
							}
						}
						
					}
				}
		    	EList<EOperation> operation = objectsClass.getEOperations();
		    	for (EOperation eOperation : operation) {
		    		String theClassName = objectsClass.getName();
					String theSuperClassName = "NoSuperClass";
					EList<EClass> theSuperClasses = objectsClass.getESuperTypes();
					if (theSuperClasses.size() > 0)
						theSuperClassName = theSuperClasses.get(0).getName();
					if(!eOperation.getName().endsWith("_private") && !eOperation.getName().contains("regScenario"))
					{
						for (ClassAndFeaturePair pair : pairs) {
							if (eOperation.getName().equalsIgnoreCase(pair.attributeOrOperationName) 
									&& ((theClassName.equalsIgnoreCase(pair.className))
									|| (theSuperClassName.equalsIgnoreCase(pair.className))))
							{
								featureCount++;
							}
						}
						
					}
				}
		    	if (featureCount>0)
		    		usedReferencedObjects.add(eObject);
			}
		}

		return usedReferencedObjects;

		
    }
		

}

