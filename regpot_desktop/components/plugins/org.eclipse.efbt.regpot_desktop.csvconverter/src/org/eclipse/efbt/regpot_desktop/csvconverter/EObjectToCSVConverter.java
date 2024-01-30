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
package org.eclipse.efbt.regpot_desktop.csvconverter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.Enumerator;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

public class EObjectToCSVConverter {
	
	public static String resourceURI = "";
	
	
	public static void persistObjectAsCSV(EObject theObject,boolean useLongNames) {
		String csvString = createCSVStringForTable(theObject,useLongNames);
		
        try {
        	if (useLongNames)
        	{
        		Files.writeString(Path.of(resourceURI + '/' +theObject.eClass().getName() + "_longnames.csv"),csvString , StandardOpenOption.CREATE_NEW);
        	}
        	else
        	{
        		Files.writeString(Path.of(resourceURI + '/' +theObject.eClass().getName() + ".csv"),csvString , StandardOpenOption.CREATE_NEW);
        	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
}

	private static String createCSVStringForTable(EObject theObject, boolean useLongNames) {
		EClass clazz;
		String csvString = "";
		EClass eClass = theObject.eClass();
		EList<EStructuralFeature> sfs = eClass.getEAllStructuralFeatures();
		for (EStructuralFeature eStructuralFeature : sfs) {

			if (eStructuralFeature instanceof EReference) {
				boolean cont = ((EReference)eStructuralFeature).isContainment();
				if (cont) {
					boolean headerCreated = false;
					TreeIterator<EObject> objectContents = theObject.eAllContents();
					while (objectContents.hasNext())
					{
						EObject o = objectContents.next();
						if (!headerCreated)
						{
							csvString = csvString + createCSVHeaderStringForRow(o);
							headerCreated = true;
						}
						csvString = csvString + createCSVStringForRow(o, useLongNames);
							
					}
				}
					
				}
		}

		return csvString + "\n";
	}
	private static String createCSVStringForRow(EObject theObject,boolean useLongNames) {
		EClass clazz;
		String csvString = "";
		EClass eClass = theObject.eClass();
		EList<EStructuralFeature> sfs = eClass.getEAllStructuralFeatures();
		boolean firstItem = true;
		for (EStructuralFeature eStructuralFeature : sfs) {
			if (firstItem)
			{   
				Object referencedItem = theObject.eGet(eStructuralFeature, true);
				String referencedItemString = getReferencedItemString(eStructuralFeature, referencedItem,useLongNames);
				csvString = csvString + referencedItemString;
				firstItem = false;
			}
			else
			{
				Object referencedItem = theObject.eGet(eStructuralFeature, true);
				String referencedItemString = getReferencedItemString(eStructuralFeature, referencedItem,useLongNames);
				csvString = csvString + "," + referencedItemString;
				
			}
		}
		EList<EOperation> ops = eClass.getEAllOperations();
		for (EOperation eOperation : ops) {
			if (firstItem)
			{
				if (!eOperation.getName().contains("_regscenario_") && !eOperation.getName().contains("_private"))
				{
					try {
						Object result = theObject.eInvoke(eOperation, null);
						String resultString = "null";
						if(result != null)
							resultString = result.toString();
						
						csvString = csvString + resultString;
						firstItem = false;
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					}
				}
				
			}
			else
			{
				if (!eOperation.getName().contains("_regscenario_") && !eOperation.getName().contains("_private"))
				{
					try {
						Object result = theObject.eInvoke(eOperation, null);
						String resultString = "null";
						if(result != null)
							resultString = result.toString();
						csvString = csvString + "," + resultString;
						firstItem = false;
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return csvString + "\n";
		
	}
	
	private static String createCSVHeaderStringForRow(EObject theObject) {
		EClass clazz;
		String csvString = "";
		EClass eClass = theObject.eClass();
		EList<EStructuralFeature> sfs = eClass.getEAllStructuralFeatures();
		boolean firstItem = true;
		for (EStructuralFeature eStructuralFeature : sfs) {
			if (firstItem)
			{
				csvString = csvString + eStructuralFeature.getName();
				firstItem = false;
			}
			else
			{
				csvString = csvString + "," + eStructuralFeature.getName();
			}
		}
		EList<EOperation> ops = eClass.getEAllOperations();
		for (EOperation eOperation : ops) {
			if (firstItem)
			{
				if (!eOperation.getName().contains("_regscenario_") && !eOperation.getName().contains("_private"))
				{
					csvString = csvString + eOperation.getName();
					firstItem = false;
				}
				
			}
			else
			{
				if (!eOperation.getName().contains("_regscenario_") && !eOperation.getName().contains("_private"))
				{
					csvString = csvString + "," + eOperation.getName();
				}
			}
		}
		return csvString + "\n";
	}
	
	private static String getReferencedItemString(EStructuralFeature eStructuralFeature, Object referencedItem,boolean useLongNames) {
		String returnString  = null;
		if ( referencedItem == null)
			returnString = "null";
		else if (eStructuralFeature instanceof EReference)
		{
			
			if (eStructuralFeature.getUpperBound() == 1)
			{
				EClass eClass = ((EObject) referencedItem ).eClass();
				EAttribute idattr = eClass.getEIDAttribute();
				EList<EStructuralFeature> features = eClass.getEAllStructuralFeatures();
				for (EStructuralFeature eStructuralFeature2 : features) {
					if (eStructuralFeature2 == idattr)
					{
						Object attributeValue = ((EObject) referencedItem).eGet(eStructuralFeature2);
						if (attributeValue != null)
							returnString = attributeValue.toString();						
					}
					if (returnString == null)
						returnString = eStructuralFeature.getName();
					
				}
			}
			else
				returnString = "multiple_"+ eStructuralFeature.getName();
				
		}
		else if ((eStructuralFeature instanceof EAttribute) && ((EAttribute) eStructuralFeature).getEAttributeType() instanceof EEnum )
		{
			if (useLongNames)
			{
				returnString = ((Enumerator) referencedItem).getLiteral() + "_" +  ((Enumerator) referencedItem).getName();
			}
			else
			{
				returnString = ((Enumerator) referencedItem).getLiteral();
			}
		}
		else
		{
			returnString = referencedItem.toString();
		}
		return returnString;
	}
}
