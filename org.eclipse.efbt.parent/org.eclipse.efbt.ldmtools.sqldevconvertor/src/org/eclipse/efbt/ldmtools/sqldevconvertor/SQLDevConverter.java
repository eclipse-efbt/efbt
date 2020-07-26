package org.eclipse.efbt.ldmtools.sqldevconvertor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.efbt.util.csv.CSVRow;
import org.eclipse.efbt.util.csv.CSVUtils;
import org.eclipse.efbt.util.csv.provider.CSVUtilsProvider;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
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

public class SQLDevConverter {
	
	public static void convert(String fileDirectory) {
		
		
		
		// for each entity make an Ecore EClass
		CSVUtils csvUtils = CSVUtilsProvider.getCSVUtils();
	
		EPackage birdpackage = EcoreFactory.eINSTANCE.createEPackage();
		birdpackage.setName("bird");
		birdpackage.setNsPrefix("bird");
		birdpackage.setNsURI("http://www.eclipse.org/bird");
		
		HashMap<String,EClass> classesMap = new HashMap<String,EClass>();
		
		
		try {
			List<CSVRow> rows = csvUtils.getCSVRowsFromFile(fileDirectory+"\\DM_Entities.csv");
			
			boolean headerSkipped = false;
			for (CSVRow csvRow : rows) {
				
				//skip the first line which is the header.
				if (!headerSkipped)
					headerSkipped = true;
				else
				{
					String className = csvRow.get(0);
					
					String objectID = csvRow.get(1);
					
					String alteredClassName = replaceSpaceWithUnderscore(className);
					
					EClass eclass = EcoreFactory.eINSTANCE.createEClass();
					eclass.setName(alteredClassName);
					birdpackage.getEClassifiers().add(eclass);
					classesMap.put(objectID,eclass);
					
				}
			}
			
			// for each inheritance relationship, add the inheritence 
			rows = csvUtils.getCSVRowsFromFile(fileDirectory+"\\DM_Entities.csv");
			
			headerSkipped = false;
			for (CSVRow csvRow : rows) {
				//skip the first line which is the header.
				if (!headerSkipped)
					headerSkipped = true;
				else
				{
					//String className = csvRow.get(0);
					String classID = csvRow.get(1);
					String superclassID = csvRow.get(25);
					if(!(superclassID.trim().length() == 0))
					{
						
						EClass theclass = (EClass) classesMap.get(classID);
						EClass superclass = (EClass) classesMap.get(superclassID);
						theclass.getESuperTypes().add(superclass);
					}

				}
			}
			
			
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		///for each domain make an enumeration
		
		HashMap<String,EEnum> enumMap = new HashMap<String,EEnum>();
		
		try {
			List<CSVRow> rows = csvUtils.getCSVRowsFromFile(fileDirectory+"\\DM_Domains.csv");
			
			int counter = 0;
			boolean headerSkipped = false;
			
			for (CSVRow csvRow : rows) {
				if (!headerSkipped)
					headerSkipped = true;
				else
				{
					counter ++;
			
				String enumID = csvRow.get(0);
				String enumName = csvRow.get(1);
				String adaptedEnumName = replaceSpaceWithUnderscore(enumName);
				
				
				EEnum theEnumeration=  EcoreFactory.eINSTANCE.createEEnum();
				birdpackage.getEClassifiers().add(theEnumeration);
				theEnumeration.setName(adaptedEnumName);
				enumMap.put(enumID, theEnumeration);
			
				
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		///for each domain add the enum literals

				
				try {
					List<CSVRow> rows = csvUtils.getCSVRowsFromFile(fileDirectory+"\\DM_Domain_AVT.csv");
					
					int counter = 0;
					boolean headerSkipped = false;
					for (CSVRow csvRow : rows) {
						
						if (!headerSkipped)
							headerSkipped = true;
						else
						{
							counter ++;
							String enumID = csvRow.get(0);
							String enumName = csvRow.get(5);
							String adaptedEnumName = replaceSpaceWithUnderscore(enumName);
							String value = csvRow.get(3);
							String adaptedValue =  replaceSpaceWithUnderscore(value);
							EEnum theEnumeration = enumMap.get(enumID);
							if(theEnumeration == null)
							{
								System.out.println("missing domain: " + enumID );
								
							}
							EList<EEnumLiteral> literals = theEnumeration.getELiterals();
							//if the literal does not exist already, then add it
							if(!containsLiteral(literals,adaptedValue))
							{
								EEnumLiteral literal = EcoreFactory.eINSTANCE.createEEnumLiteral();
								literal.setName(adaptedValue);
								literal.setValue(counter);
								literals.add(literal);
							}
						
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				///for each logicalDatatype for orcle 12c, make a Datatype if we have an equivalient
				HashMap<String,EDataType> datatypeMap = new HashMap<String,EDataType>();
				EcorePackage ecorePackage = EcoreFactory.eINSTANCE.getEcorePackage();
				try {
					boolean headerSkipped = false;
					List<CSVRow> rows = csvUtils.getCSVRowsFromFile(fileDirectory+"\\DM_Logical_To_Native.csv");
					for (CSVRow csvRow : rows) {
						
						if (!headerSkipped)
							headerSkipped = true;
						else
						{
							String rdbms_Type = csvRow.get(3);
							String rdbms_Version= csvRow.get(4);
							String dataTypeID= csvRow.get(0);
							if(rdbms_Type.trim().equalsIgnoreCase("Oracle Database") && rdbms_Version.trim().equalsIgnoreCase("12cR2"))
							{
								String native_type= csvRow.get(2);
					
								if(native_type.trim().equals("VARCHAR"))
								{
								
									datatypeMap.put(dataTypeID,ecorePackage.getEString());
								}
								if(native_type.trim().equals("VARCHAR2"))
								{
									
									datatypeMap.put(dataTypeID,ecorePackage.getEString());
								}
								if(native_type.trim().equals("INTEGER"))
								{
									
									datatypeMap.put(dataTypeID,ecorePackage.getEInt());
								}
								if(native_type.trim().equals("DATE"))
								{
									
									datatypeMap.put(dataTypeID,ecorePackage.getEDate());
								}if(native_type.trim().equals("NUMBER"))
								{
									
									datatypeMap.put(dataTypeID,ecorePackage.getEDouble());
								}
								if(native_type.trim().equals("UNKNOWN"))
								{
									
									datatypeMap.put(dataTypeID,ecorePackage.getEString());
								}
								
								
									
							}
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		// for each attribute add an attribute to the correct Entity
				// the attribute should have the correct type, which may be a specific enumeration
			try {
				boolean headerSkipped = false;
				List<CSVRow> rows = csvUtils.getCSVRowsFromFile(fileDirectory+"\\DM_Attributes.csv");
				for (CSVRow csvRow : rows) {
					if (!headerSkipped)
						headerSkipped = true;
					else
					{
						String attributeName = csvRow.get(0);
						String amendedAttributeName = replaceSpaceWithUnderscore(attributeName);
						String attributeKind = csvRow.get(7).trim();
						EAttribute attribute = EcoreFactory.eINSTANCE.createEAttribute();
						attribute.setName(amendedAttributeName);
						
							
					
						if(attributeKind.equalsIgnoreCase("Domain"))
						{
							String domainID = csvRow.get(12);
							EEnum enumeration  = enumMap.get(domainID);					
							attribute.setEType(enumeration);
						}
						
						if(attributeKind.equalsIgnoreCase("Logical Type"))
						{
							String dataTypeID = csvRow.get(14);
							EDataType datatype  = datatypeMap.get(dataTypeID);	
							if(datatype != null)
							{
								attribute.setEType(datatype);
							}
							else
							{
								System.out.println("missing datatype: " + dataTypeID );
							}
						}
						
						String classID = csvRow.get(4);
						
						EClass theClass = classesMap.get(classID);
						if(theClass != null)
						{
							
							theClass.getEStructuralFeatures().add(attribute);
						}
						else
						{
							System.out.println("missing class: " + classID );
						}
						
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// remove any attributes that already exist in superclass.
			
			Set<Entry<String, EClass>> classes = classesMap.entrySet();
			
			for (Iterator iterator = classes.iterator(); iterator.hasNext();) {
				Entry<String, EClass> entry = (Entry<String, EClass>) iterator.next();
				EClass theClass = entry.getValue();
				EList<EClass> superclasses = theClass.getEAllSuperTypes();
				if(superclasses.size()>0)
				{
					EClass theSuperClass = superclasses.get(0);
					EList<EStructuralFeature> features = theClass.getEStructuralFeatures();
					List<EStructuralFeature> featuresToDelete = new ArrayList<EStructuralFeature>();
					for (Iterator iterator2 = features.iterator(); iterator2.hasNext();) {
						EStructuralFeature eStructuralFeature = (EStructuralFeature) iterator2.next();
						if(superclassContainsFeature(theSuperClass,eStructuralFeature))
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
				List<CSVRow> rows = csvUtils.getCSVRowsFromFile(fileDirectory+"\\DM_Relations.csv");
				for (CSVRow csvRow : rows) {
					if (!headerSkipped)
						headerSkipped = true;
					else
					{
						String sourceID= csvRow.get(16);
						String targetID = csvRow.get(18);
						String sourceTo_Target_Cardinality = csvRow.get(10);
						String targetTo_Source_Cardinality = csvRow.get(11);
						String targetClassName = csvRow.get(7);
						String source_Optional = csvRow.get(12);
						String target_Optional  = csvRow.get(13);
						String attributeKind = csvRow.get(7).trim();
						String referenceID = csvRow.get(4).trim();
						EReference reference = EcoreFactory.eINSTANCE.createEReference();
						String referenceName = "the" +replaceSpaceWithUnderscore(targetClassName);
						if(target_Optional.trim().equals("Y"))
							reference.setLowerBound(0);
						else
							reference.setLowerBound(1);
						
						if(sourceTo_Target_Cardinality.trim().equals("*"))
						{
							reference.setUpperBound(-1);
							referenceName = referenceName + "s";
						}
						else 
							reference.setUpperBound(1);

						referenceName = referenceName + referenceID;
						reference.setName(referenceName);
					
						
						EClass theClass = classesMap.get(sourceID);
						EClass targetClass = classesMap.get(targetID);
						if(theClass != null)
						{
							theClass.getEStructuralFeatures().add(reference);
						}
						else
						{
							System.out.println("missing class: " + sourceID );
						}
						if(targetClass != null)
						{
							
							reference.setEType(targetClass);
						}
						else
						{
							System.out.println("missing target class: " + sourceID );
						}
						
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
		//create a resource and save it 
			ResourceFactoryImpl resourceFactory = new XMIResourceFactoryImpl();
			URI modelURI= URI.createFileURI(fileDirectory + "\\ldm.ecore");

			Resource ldmResource = resourceFactory.createResource(modelURI);
			ldmResource.getContents().add(birdpackage);
			try {
				ldmResource.save(Collections.EMPTY_MAP);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	}
	
	private static boolean superclassContainsFeature(EClass theSuperClass, EStructuralFeature eStructuralFeature) {
		// TODO Auto-generated method stub
		EList<EStructuralFeature> features = theSuperClass.getEAllStructuralFeatures();
		boolean contains = false;
		for (EStructuralFeature eStructuralFeature2 : features) {
			if(eStructuralFeature2.getName().equals(eStructuralFeature.getName()))
				contains = true;
			
		}
		return contains;
	}

	private static boolean containsLiteral(EList<EEnumLiteral> literals, String adaptedValue) {
		boolean contains = false;
		for  (Iterator iterator = literals.iterator(); iterator.hasNext();) {
			EEnumLiteral eEnumLiteral = (EEnumLiteral) iterator.next();
			if(eEnumLiteral.getName().equals(adaptedValue))
			{
				contains = true;
			}
		}
		
		return contains;
			
	}

	private static String replaceSpaceWithUnderscore(String className) {
		//replace spaces and apostrophes commas and brackets with underscores
		return className.replace(' ', '_').replace((char)65533, '_').replace(')', '_').
				replace('(', '_').replace(',', '_').replace('\\', '_').replace('/', '_')
				.replace('-', '_').replace(':', '_').replace('+', '_').replace('.', '_')
				.replace('?', '_').replace('\'', '_').replace('>', '_').replace('&', '_')
				.replace('%', '_').replace('[', '_').replace(']', '_').replace((char) 0x2019,'_' )
				.replace((char) 0x2018, '_').replace((char) 0x0060, '_').replace((char) 0x00B4, '_');
		

	}

	public static void main(String[] args)
	{
		SQLDevConverter.convert(args[0]);
	}

}
