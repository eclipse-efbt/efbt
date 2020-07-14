package org.eclipse.efbt.ldmtools.sqldevconvertor;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.eclipse.efbt.util.csv.CSVRow;
import org.eclipse.efbt.util.csv.CSVUtils;
import org.eclipse.efbt.util.csv.provider.CSVUtilsProvider;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcoreFactory;

public class SQLDevConverter {
	
	public static void convert(String fileDirectory) {
		// get the csv files
		//File attributesFile = new File(fileDirectory+"\\DM_Attributes.csv");
		//File entitiesFile = new File(fileDirectory+"\\DM_Entities.csv");
		//File relationshipsFile = new File(fileDirectory+"\\DM_Relations.csv");
		//File domainsFile = new File(fileDirectory+"\\DM_Domain_AVT.csv");
		
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
			//now set all the super classes
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
							EEnumLiteral literal = EcoreFactory.eINSTANCE.createEEnumLiteral();
							literal.setName(adaptedValue);
							literal.setValue(counter);
							literals.add(literal);
						
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				///for each logicalDatatype for orcle 12c, make a Datatype if we have an equivalient
				HashMap<String,EDataType> datatypeMap = new HashMap<String,EDataType>();
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
								EDataType datatype = EcoreFactory.eINSTANCE.createEDataType();
								if(native_type.trim().equals("VARCHAR"))
								{
									datatype.setName("EString");
									datatype.setInstanceClassName("String");
									datatype.setInstanceClass(String.class);
									datatypeMap.put(dataTypeID,datatype);
								}
								if(native_type.trim().equals("VARCHAR2"))
								{
									datatype.setName("EString");
									datatype.setInstanceClassName("String");
									datatype.setInstanceClass(String.class);
									datatypeMap.put(dataTypeID,datatype);
								}
								if(native_type.trim().equals("INTEGER"))
								{
									datatype.setName("EInt");
									datatype.setInstanceClassName("Integer");
									datatype.setInstanceClass(Integer.class);
									datatypeMap.put(dataTypeID,datatype);
								}
								if(native_type.trim().equals("DATE"))
								{
									datatype.setName("EDate");
									datatype.setInstanceClassName("Date");
									datatype.setInstanceClass(Date.class);
									datatypeMap.put(dataTypeID,datatype);
								}if(native_type.trim().equals("NUMBER"))
								{
									datatype.setName("EDouble");
									datatype.setInstanceClassName("Double");
									datatype.setInstanceClass(Double.class);
									datatypeMap.put(dataTypeID,datatype);
								}
								if(native_type.trim().equals("UNKNOWN"))
								{
									datatype.setName("EString");
									datatype.setInstanceClassName("String");
									datatype.setInstanceClass(Integer.class);
									datatypeMap.put(dataTypeID,datatype);
								}
								
								
									
							}
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		// for each attribute add an attribute to the correct Entity

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
						
							
						//check the data type, or a domain type
						//if data_type_kind is DOMAIN, set the correct enumeration type
						//if data_type_kind is DOMAIN, and DOMAIN type is unknown, set to special UNKNOWN domain
						//id ata type is Logical_type, get the Logical Type.
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
			
		
		// the attribute should have the correct type, which may be a specific enumeration
		// for each inheritance relationship, add the inheritence 
		// for each relationship, add a non-containment relationship with the correct cardinality.
		///for each domain make an enumeration
			System.out.println("birdPackage = " + birdpackage.toString());
		
		
	}
	
	private static String replaceSpaceWithUnderscore(String className) {
		// TODO Auto-generated method stub
		return className.replace(' ', '_');
	}

	public static void main(String[] args)
	{
		SQLDevConverter.convert(args[0]);
	}

}
