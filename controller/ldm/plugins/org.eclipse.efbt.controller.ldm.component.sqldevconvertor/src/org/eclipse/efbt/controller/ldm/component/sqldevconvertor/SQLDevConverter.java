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
package org.eclipse.efbt.controller.ldm.component.sqldevconvertor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.impl.ResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * This class is responsible for taking logical models defined in the free
 * Oracle SQLDeveloper tool, which are exported by the tool into CSV files, and
 * transforming that logical datamodel into an equivalent Ecore model. Ecore is
 * the model definition format for model in the Eclipse Modelling Framework ,
 * and is an implementation of the EMOF (Essential Met Object Facility) standard
 * defined by the OMG standards group.
 * 
 * @author Neil Mackenzie
 *
 */
public class SQLDevConverter {

	/**
	 * Convert the csv files in the directory into and Ecore model
	 * 
	 * @param fileDirectory
	 */
	public static void convert(String fileDirectory) {

		// for each entity make an Ecore EClass
		

		EPackage birdpackage = EcoreFactory.eINSTANCE.createEPackage();
		birdpackage.setName("bird");
		birdpackage.setNsPrefix("bird");
		birdpackage.setNsURI("http://www.eclipse.org/bird");

		HashMap<String, EClass> classesMap = new HashMap<String, EClass>();

		// for each entity make an Eclass
		try {
			List<CSVRecord> rows = getCSVRowsFromFile(fileDirectory + "\\DM_Entities.csv");

			boolean headerSkipped = false;
			for (CSVRecord csvRow : rows) {

				// skip the first line which is the header.
				if (!headerSkipped)
					headerSkipped = true;
				else {
					String className = csvRow.get(0);

					String objectID = csvRow.get(1);

					String alteredClassName = replaceSpaceWithUnderscore(className);

					EClass eclass = EcoreFactory.eINSTANCE.createEClass();
					eclass.setName(alteredClassName);
					birdpackage.getEClassifiers().add(eclass);
					classesMap.put(objectID, eclass);

				}
			}

			// for each inheritance relationship, add the inheritence
			rows = getCSVRowsFromFile(fileDirectory + "\\DM_Entities.csv");

			headerSkipped = false;
			for (CSVRecord csvRow : rows) {
				// skip the first line which is the header.
				if (!headerSkipped)
					headerSkipped = true;
				else {
					// String className = csvRow.get(0);
					String classID = csvRow.get(1);
					String superclassID = csvRow.get(25);
					if (!(superclassID.trim().length() == 0)) {

						EClass theclass = (EClass) classesMap.get(classID);
						EClass superclass = (EClass) classesMap.get(superclassID);
						theclass.getESuperTypes().add(superclass);
					}

				}
			}

		} catch (IOException e) {

			e.printStackTrace();
		}

		/// for each domain make an enumeration

		HashMap<String, EEnum> enumMap = new HashMap<String, EEnum>();

		try {
			List<CSVRecord> rows = getCSVRowsFromFile(fileDirectory + "\\DM_Domains.csv");

			int counter = 0;
			boolean headerSkipped = false;

			for (CSVRecord csvRow : rows) {
				if (!headerSkipped)
					headerSkipped = true;
				else {
					counter++;

					String enumID = csvRow.get(0);
					String enumName = csvRow.get(1);
					String adaptedEnumName = replaceSpaceWithUnderscore(enumName);

					EEnum theEnumeration = EcoreFactory.eINSTANCE.createEEnum();
					birdpackage.getEClassifiers().add(theEnumeration);
					theEnumeration.setName(adaptedEnumName);
					enumMap.put(enumID, theEnumeration);

				}

			}
		} catch (IOException e) {

			e.printStackTrace();
		}

		// for each domain add the enum literals

		try {
			List<CSVRecord> rows = getCSVRowsFromFile(fileDirectory + "\\DM_Domain_AVT.csv");

			int counter = 0;
			boolean headerSkipped = false;
			for (CSVRecord csvRow : rows) {

				if (!headerSkipped)
					headerSkipped = true;
				else {
					counter++;
					String enumID = csvRow.get(0);
					String enumName = csvRow.get(5);
					String adaptedEnumName = replaceSpaceWithUnderscore(enumName);
					String value = csvRow.get(3);
					String adaptedValue = replaceSpaceWithUnderscore(value);
					EEnum theEnumeration = enumMap.get(enumID);
					if (theEnumeration == null) {
						System.out.println("missing domain: " + enumID);

					}
					EList<EEnumLiteral> literals = theEnumeration.getELiterals();
					// if the literal does not exist already, then add it
					if (!containsLiteral(literals, adaptedValue)) {
						EEnumLiteral literal = EcoreFactory.eINSTANCE.createEEnumLiteral();
						literal.setName(adaptedValue);
						literal.setValue(counter);
						literals.add(literal);
					}

				}
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

		/// for each logicalDatatype for orcle 12c, make a Datatype if we have an
		/// equivalent
		HashMap<String, EDataType> datatypeMap = new HashMap<String, EDataType>();
		EcorePackage ecorePackage = EcoreFactory.eINSTANCE.getEcorePackage();
		try {
			boolean headerSkipped = false;
			List<CSVRecord> rows = getCSVRowsFromFile(fileDirectory + "\\DM_Logical_To_Native.csv");
			for (CSVRecord csvRow : rows) {

				if (!headerSkipped)
					headerSkipped = true;
				else {
					String rdbms_Type = csvRow.get(3);
					String rdbms_Version = csvRow.get(4);
					String dataTypeID = csvRow.get(0);
					if (rdbms_Type.trim().equalsIgnoreCase("Oracle Database")
							&& rdbms_Version.trim().equalsIgnoreCase("12cR2")) {
						String native_type = csvRow.get(2);

						if (native_type.trim().equals("VARCHAR")) {

							datatypeMap.put(dataTypeID, ecorePackage.getEString());
						}
						if (native_type.trim().equals("VARCHAR2")) {

							datatypeMap.put(dataTypeID, ecorePackage.getEString());
						}
						if (native_type.trim().equals("INTEGER")) {

							datatypeMap.put(dataTypeID, ecorePackage.getEInt());
						}
						if (native_type.trim().equals("DATE")) {

							datatypeMap.put(dataTypeID, ecorePackage.getEDate());
						}
						if (native_type.trim().equals("NUMBER")) {

							datatypeMap.put(dataTypeID, ecorePackage.getEDouble());
						}
						if (native_type.trim().equals("UNKNOWN")) {

							datatypeMap.put(dataTypeID, ecorePackage.getEString());
						}

					}
				}
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

		// for each attribute add an attribute to the correct Entity
		// the attribute should have the correct type, which may be a specific
		// enumeration
		try {
			boolean headerSkipped = false;
			List<CSVRecord> rows = getCSVRowsFromFile(fileDirectory + "\\DM_Attributes.csv");
			for (CSVRecord csvRow : rows) {
				if (!headerSkipped)
					headerSkipped = true;
				else {
					String attributeName = csvRow.get(0);
					String amendedAttributeName = replaceSpaceWithUnderscore(attributeName);
					String attributeKind = csvRow.get(7).trim();
					EAttribute attribute = EcoreFactory.eINSTANCE.createEAttribute();
					attribute.setName(amendedAttributeName);

					if (attributeKind.equalsIgnoreCase("Domain")) {
						String domainID = csvRow.get(12);
						EEnum enumeration = enumMap.get(domainID);
						attribute.setEType(enumeration);
					}

					if (attributeKind.equalsIgnoreCase("Logical Type")) {
						String dataTypeID = csvRow.get(14);
						EDataType datatype = datatypeMap.get(dataTypeID);
						if (datatype != null) {
							attribute.setEType(datatype);
						} else {
							System.out.println("missing datatype: " + dataTypeID);
						}
					}

					String classID = csvRow.get(4);

					EClass theClass = classesMap.get(classID);
					if (theClass != null) {

						theClass.getEStructuralFeatures().add(attribute);
					} else {
						System.out.println("missing class: " + classID);
					}

				}
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
		// remove any attributes that already exist in superclass.

		Set<Entry<String, EClass>> classes = classesMap.entrySet();

		for (Iterator iterator = classes.iterator(); iterator.hasNext();) {
			Entry<String, EClass> entry = (Entry<String, EClass>) iterator.next();
			EClass theClass = entry.getValue();
			EList<EClass> superclasses = theClass.getEAllSuperTypes();
			if (superclasses.size() > 0) {
				EClass theSuperClass = superclasses.get(0);
				EList<EStructuralFeature> features = theClass.getEStructuralFeatures();
				List<EStructuralFeature> featuresToDelete = new ArrayList<EStructuralFeature>();
				for (Iterator iterator2 = features.iterator(); iterator2.hasNext();) {
					EStructuralFeature eStructuralFeature = (EStructuralFeature) iterator2.next();
					if (superclassContainsFeature(theSuperClass, eStructuralFeature))
						featuresToDelete.add(eStructuralFeature);

				}
				for (Iterator iterator2 = featuresToDelete.iterator(); iterator2.hasNext();) {
					EStructuralFeature eStructuralFeature = (EStructuralFeature) iterator2.next();
					features.remove(eStructuralFeature);
				}
			}
		}
		// for each relationship add a reference
		try {
			boolean headerSkipped = false;
			List<CSVRecord> rows = getCSVRowsFromFile(fileDirectory + "\\DM_Relations.csv");
			for (CSVRecord csvRow : rows) {
				if (!headerSkipped)
					headerSkipped = true;
				else {
					String sourceID = csvRow.get(16);
					String targetID = csvRow.get(18);
					String sourceTo_Target_Cardinality = csvRow.get(10);
					String targetTo_Source_Cardinality = csvRow.get(11);
					String targetClassName = csvRow.get(7);
					String source_Optional = csvRow.get(12);
					String target_Optional = csvRow.get(13);
					String attributeKind = csvRow.get(7).trim();
					String referenceID = csvRow.get(4).trim();
					EReference reference = EcoreFactory.eINSTANCE.createEReference();
					String referenceName = "the" + replaceSpaceWithUnderscore(targetClassName);
					if (target_Optional.trim().equals("Y"))
						reference.setLowerBound(0);
					else
						reference.setLowerBound(1);

					if (sourceTo_Target_Cardinality.trim().equals("*")) {
						reference.setUpperBound(-1);
						referenceName = referenceName + "s";
					} else
						reference.setUpperBound(1);
					// if the reference name already exists on this EClass
					// then add a number  the end like 2 if there is already 1 reference
					// or 3 if there already exists 2 other references to this class
					

					EClass theClass = classesMap.get(sourceID);
					EClass targetClass = classesMap.get(targetID);
					
					int numOfRelations = numberofRelationShipsToThisClass(theClass,targetClass);
					if(numOfRelations>0)
						referenceName = referenceName + numOfRelations;
					
					//referenceName = referenceName + referenceID;
					reference.setName(referenceName);
					
					
					if (theClass != null) {
						theClass.getEStructuralFeatures().add(reference);
					} else {
						System.out.println("missing class: " + sourceID);
					}
					if (targetClass != null) {

						reference.setEType(targetClass);
					} else {
						System.out.println("missing target class: " + sourceID);
					}

				}
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

		// create a resource and save it
		ResourceFactoryImpl resourceFactory = new XMIResourceFactoryImpl();
		URI modelURI = URI.createFileURI(fileDirectory + "\\ldm.ecore");

		Resource ldmResource = resourceFactory.createResource(modelURI);
		ldmResource.getContents().add(birdpackage);
		try {
			ldmResource.save(Collections.EMPTY_MAP);
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	private static List<CSVRecord> getCSVRowsFromFile(String fileName) throws IOException  {
		// TODO Auto-generated method stub
		File csvData = new File (fileName);
		 CSVParser parser = CSVParser.parse(csvData,StandardCharsets.UTF_8, CSVFormat.EXCEL);
		 List<CSVRecord> list = new ArrayList<CSVRecord>();
		 for (CSVRecord csvRecord : parser) {
			
			 list.add(csvRecord);
		 }
		 return list;
	}

	private static int numberofRelationShipsToThisClass(EClass sourceClass, EClass targetClass) {
		// TODO Auto-generated method stub
		EList<EStructuralFeature> features = sourceClass.getEStructuralFeatures();
		int counter = 0;
		
		for (EStructuralFeature eStructuralFeature : features) {
			EClassifier sourceFeatureType = eStructuralFeature.getEType();
			
			if (sourceFeatureType.equals(targetClass))
			{
				
				counter++;
			}
			else
			{
				
			}
			}
			
			
		
		return counter;
	}

	/**
	 * This method returns true if a the superclass provided contains a feature
	 * with the same name as the feature provided. Otherwise returns false.
	 * 
	 * @param theSuperClass
	 * @param eStructuralFeature
	 * @return
	 */
	private static boolean superclassContainsFeature(EClass theSuperClass, EStructuralFeature eStructuralFeature) {

		EList<EStructuralFeature> features = theSuperClass.getEAllStructuralFeatures();
		boolean contains = false;
		for (EStructuralFeature eStructuralFeature2 : features) {
			if (eStructuralFeature2.getName().equals(eStructuralFeature.getName()))
				contains = true;

		}
		return contains;
	}

	/**
	 * This method returns true if the set of Literals contains a Literal with the 
	 * a name equal to the String adaptedValue provided. Otherwise returns false.
	 * @param literals
	 * @param adaptedValue
	 * @return
	 */
	private static boolean containsLiteral(EList<EEnumLiteral> literals, String adaptedValue) {
		boolean contains = false;
		for (Iterator iterator = literals.iterator(); iterator.hasNext();) {
			EEnumLiteral eEnumLiteral = (EEnumLiteral) iterator.next();
			if (eEnumLiteral.getName().equals(adaptedValue)) {
				contains = true;
			}
		}

		return contains;

	}

	/**
	 * This method replaces spaces and the non-standard characters which are not allowed
	 * in Ecore classifier names  with underscores.
	 *  
	 * @param className
	 * @return
	 */
	private static String replaceSpaceWithUnderscore(String className) {
		
		return className.replace(' ', '_').replace((char) 65533, '_').replace(')', '_').replace('(', '_')
				.replace(',', '_').replace('\\', '_').replace('/', '_').replace('-', '_').replace(':', '_')
				.replace('+', '_').replace('.', '_').replace('?', '_').replace('\'', '_').replace('>', '_')
				.replace('&', '_').replace('%', '_').replace('[', '_').replace(']', '_').replace((char) 0x2019, '_')
				.replace((char) 0x2018, '_').replace((char) 0x0060, '_').replace((char) 0x00B4, '_');

	}

	/**
	 * A main method for testing, it takes the file location of the CSV files 
	 * as an argument.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		SQLDevConverter.convert(args[0]);
	}

}
