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
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.sirius.diagram.DDiagramElement;
import org.eclipse.sirius.diagram.DSemanticDiagram;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.eclipse.efbt.regdna.model.regdna.*;
import org.eclipse.efbt.regdna.model.regdna.ELAttribute;
import org.eclipse.efbt.regdna.model.regdna.ELClass;
import org.eclipse.efbt.regdna.model.regdna.ELOperation;
import org.eclipse.efbt.regdna.model.regdna.ELPublicOperation;
import org.eclipse.efbt.regdna.model.regdna.SelectColumn;
import org.eclipse.efbt.regdna.model.regdna.SelectColumnAttributeAs;




/**
 * The services class used by VSM.
 */
public class Services {
    

    public String getInvokedOperation(EOperation self,DSemanticDiagram diagram,DDiagramElement view) {

    	DDiagramElement  container = (DDiagramElement) view.eContainer();
    	System.out.println(" container " + container);
    	EObject containersTarget = container.getTarget();
    	System.out.println(" container target " + containersTarget);
    	EObject viewTarget = view.getTarget(); 
    	String result = "def";		
    	try {
    		result =  (containersTarget.eInvoke((EOperation)viewTarget, null)).toString();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				ELPublicOperation oper = ((SelectDerivedColumnAs) eObject).getAttribute();
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
    	
    	for (EObject eObject : allObjects) {
    		EList<EObject> containedObjects = eObject.eContents();
    		if(containedObjects.size() > 0)
    		{
    			
    			EClass objectsClass = containedObjects.get(0).eClass();
	    		EList<EOperation> operations = objectsClass.getEOperations();
	    		for (EOperation operation : operations) {
					if (operation.getName().endsWith("_private"))
					{
						EList<EParameter> params = operation.getEParameters();
						for (EParameter param : params) {
							String[] sections = param.getName().split("___");
							if (sections.length > 1)
							{
								ClassAndFeaturePair classAndFeaturePair = new ClassAndFeaturePair();
								
								String className = getEClassAssociatedWithReference((EClass)operation.eContainer(),sections[0]);
								classAndFeaturePair.className = className;
								classAndFeaturePair.attributeOrOperationName = sections[1];
								returnList.add(classAndFeaturePair);
							}
							else
								System.out.println("couldnt split " + param.getName());
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
		for (EReference eReference : references) {
			String referenceName = eReference.getName();
			
			if(referenceName.equalsIgnoreCase(refName))
			{
				returnValue = eReference.getEType().getName();
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

