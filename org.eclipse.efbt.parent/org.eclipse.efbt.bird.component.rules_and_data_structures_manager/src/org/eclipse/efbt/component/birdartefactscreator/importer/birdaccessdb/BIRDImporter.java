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
 *  Neil Mackenzie - initial API and implementation
 *  */

package org.eclipse.efbt.component.birdartefactscreator.importer.birdaccessdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.component.birdartefactscreator.importer.FreeBirdToolsResourceFactory;
import org.eclipse.efbt.component.birdartefactscreator.importer.Importer;
import org.eclipse.efbt.util.access.provider.AccessUtilProvider;
import org.eclipse.efbt.util.accessdb.AccessRow;
import org.eclipse.efbt.util.accessdb.AccessUtils;
import org.eclipse.efbt.util.csv.CSVRow;
import org.eclipse.efbt.util.csv.CSVUtils;
import org.eclipse.efbt.util.csv.provider.CSVUtilsProvider;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;

import com.ibm.icu.text.UTF16;

import org.eclipse.efbt.aorta.aorta_program.Aorta_programFactory;

import org.eclipse.efbt.aorta.aorta_program.Program;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseCellWithEnumeratedValue;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseCellWithValue;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.aorta.base_column_structured_data.Base_column_structured_dataFactory;
import org.eclipse.efbt.aorta.column_structures.Column;
import org.eclipse.efbt.aorta.column_structures.ColumnDataType;
import org.eclipse.efbt.aorta.column_structures.ColumnDomain;
import org.eclipse.efbt.aorta.column_structures.ColumnDomainModule;
import org.eclipse.efbt.aorta.column_structures.ColumnStructureModule;
import org.eclipse.efbt.aorta.column_structures.ColumnStructuredEntity;
import org.eclipse.efbt.aorta.column_structures.Column_structuresFactory;
import org.eclipse.efbt.aorta.column_structures.EnumMember;
import org.eclipse.efbt.bird.core.CoreFactory;
import org.eclipse.efbt.bird.core.DOMAIN;
import org.eclipse.efbt.bird.core.FACET_VALUE_TYPE;
import org.eclipse.efbt.bird.core.MEMBER;
import org.eclipse.efbt.bird.core.VARIABLE;
import org.eclipse.efbt.bird.data_definition.COMBINATION;
import org.eclipse.efbt.bird.data_definition.COMBINATION_ITEM;
import org.eclipse.efbt.bird.data_definition.CUBE;
import org.eclipse.efbt.bird.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.bird.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.bird.data_definition.Data_definitionFactory;
import org.eclipse.efbt.bird.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.bird.efbt_data_definition.Efbt_data_definitionFactory;
import org.eclipse.efbt.aorta.functionality_module.FunctionalityModule;
import org.eclipse.efbt.aorta.functionality_module.FunctionalityModuleModule;
import org.eclipse.efbt.aorta.functionality_module.Functionality_moduleFactory;
import org.eclipse.efbt.bird.mapping.CUBE_MAPPING;
import org.eclipse.efbt.bird.mapping.MAPPING_DEFINITION;
import org.eclipse.efbt.bird.mapping.MAPPING_TO_CUBE;
import org.eclipse.efbt.bird.mapping.MEMBER_MAPPING;
import org.eclipse.efbt.bird.mapping.MappingFactory;
import org.eclipse.efbt.bird.mapping.VARIABLE_MAPPING;
import org.eclipse.efbt.aorta.test.E2ETest;
import org.eclipse.efbt.aorta.test.Test;
import org.eclipse.efbt.aorta.test.TestFactory;
import org.eclipse.efbt.aorta.test.TestModule;
import org.eclipse.efbt.aorta.test_definition.ClauseText;
import org.eclipse.efbt.aorta.test_definition.E2ETestDefinition;
import org.eclipse.efbt.aorta.test_definition.Given;
import org.eclipse.efbt.aorta.test_definition.Param;
import org.eclipse.efbt.aorta.test_definition.TestConstraintsModule;
import org.eclipse.efbt.aorta.test_definition.TestContraints;
import org.eclipse.efbt.aorta.test_definition.TestDefinition;
import org.eclipse.efbt.aorta.test_definition.TestDefinitionModule;
import org.eclipse.efbt.aorta.test_definition.TestTemplate;
import org.eclipse.efbt.aorta.test_definition.TestTemplateModule;
import org.eclipse.efbt.aorta.test_definition.Test_definitionFactory;
import org.eclipse.efbt.aorta.test_definition.Then;
import org.eclipse.efbt.aorta.test_definition.When;
import org.eclipse.efbt.aorta.test_input_data.TestColumnStructuredData;
import org.eclipse.efbt.aorta.test_input_data.TestInputData;
import org.eclipse.efbt.aorta.test_input_data.Test_input_dataFactory;
import org.eclipse.efbt.bird.vtl_transformation.TRANSFORMATION_SCHEME;
import org.eclipse.efbt.bird.vtl_transformation.Vtl_transformationFactory;

/**
 * This class is respnsable for structure data taking data from the BIRD
 * Access Database and also data in CSV defining Test data, and creates 
 * instances of the Aorta Program model.
 *  
 * @author Neil Mackenzie
 *
 */
public class BIRDImporter extends Importer {

	/**
	 * The file path of the access database
	 */
	public static String filepath;
	/**
	 * The file path of the input test data
	 */
	public static String testdatafilepath;

	/**
	 * A Program instance which contains just TestDefinitions
	 */
	public static Program testDefinitionProgram;
	/**
	 * A Program instance which contains just TestTemplates,
	 * note that the testTemplates can refer to TestDefintions 
	 * in testDefinitionProgram
	 */
	public static Program testTemplateProgram;
	/**
	 * A Program instance which contains just TestConstriants
	 */
	public static Program testConstraintsProgram;
	/**
	 * A Program instance which contains just input DataStructures
	 */
	public static Program iputDataStructuresProgram;
	/**
	 * A Program instance which contains just domains
	 */
	public static Program domainsProgram;
	/**
	 * A Program instance which contains just functionality Modules
	 */
	public static Program functionalityModulesProgram;
	/**
	 * A list of programs, each of which contains only Tests
	 */
	public static List<Program> testPrograms;

	/**
	 * Returns the file path of the access database
	 * @return
	 */
	public static String getFilepath() {
		return filepath;
	}
	/**
	 * Sets the file path of the access database
	 * 
	 */
	public static void setFilepath(String filepath) {
		BIRDImporter.filepath = filepath;
	}

	/** 
	 * create the TRANSFORMATION_SCHEME model instances from the BIRD Access Database
	 */
	public void createAllTransformationSchemes() {

		try {

			AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();
			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "TRANSFORMATION_SCHEME");

			for (AccessRow row : list) {
			
				TRANSFORMATION_SCHEME scheme = Vtl_transformationFactory.eINSTANCE.createTRANSFORMATION_SCHEME();
				scheme.setCode(row.getString("CODE"));
				scheme.setTransformation_scheme_id(row.getString("TRANSFORMATION_SCHEME_ID"));
				scheme.setDescription(row.getString("DESCRIPTION"));
				scheme.setMaintenance_agency_id(row.getString("MAINTENANCE_AGENCY_ID"));
				scheme.setName(row.getString("NAME"));
				scheme.setValid_to(row.getDate("VALID_TO"));
				scheme.setValid_from(row.getDate("VALID_FROM"));

				transformationSchemes.getSchemes().add(scheme);

			}

			/**
			 * Note that TRANSFORMATION is not currently neded in our model, and that
			 * Transmormation_Node was removed (perhaps temporarily) in BIRd 5.0 , we may
			 * wish to add this code back in at a later date.
			 * 
			 * 
			 * table = DatabaseBuilder.open(new File(filepath)).getTable("TRANSFORMATION");
			 * for (Row row : table) { System.out.println("Column 'TRANSFORMATION_ID' has
			 * value: " + row.getString("TRANSFORMATION_ID")); TRANSFORMATION transformation
			 * = Vtl_transformationFactory.eINSTANCE.createTRANSFORMATION();
			 * TRANSFORMATION_SCHEME scheme =
			 * getTransformationScheme(row.getString("TRANSFORMATION_SCHEME_ID"));
			 * 
			 * transformation.setCode(row.getString("CODE"));
			 * transformation.setTransformation_id(row.getString("TRANSFORMATION_ID"));
			 * transformation.setDescription(row.getString("DESCRIPTION"));
			 * transformation.setMaintenance_agency_id(row.getString("MAINTENANCE_AGENCY_ID"));
			 * transformation.setName(row.getString("NAME"));
			 * transformation.setExpression(row.getString("EXPRESSION"));
			 * scheme.getTranformations().add(transformation);
			 * System.out.println("transformation = " + transformation.toString());
			 * 
			 * }
			 * 
			 * table = DatabaseBuilder.open(new
			 * File(filepath)).getTable("TRANSFORMATION_NODE"); for (Row row : table) {
			 * System.out.println( "Column 'TRANSFORMATION_NODE_ID' has value: " +
			 * row.getString("TRANSFORMATION_NODE_ID")); TRANSFORMATION_NODE node =
			 * Vtl_transformationFactory.eINSTANCE.createTRANSFORMATION_NODE();
			 * EList<TRANSFORMATION_NODE> nodelist = new BasicEList<TRANSFORMATION_NODE>();
			 * 
			 * node.setLevel((row.getInt("LEVEL"))); node.setOrder((row.getInt("ORDER")));
			 * node.setTransformation_node_id((row.getString("TRANSFORMATION_NODE_ID")));
			 * node.setParent_node_id(row.getString("PARENT_NODE_ID")); String tableID =
			 * row.getString("TABLE_ID"); String elementID = row.getString("ELEMENT_ID");
			 * MEMBER member = null; VARIABLE variable = null; if (tableID != null) { if
			 * (tableID.equals("VARIABLE")) { variable = findVariableWithID(elementID);
			 * node.setVariable(variable); } if (tableID.equals("MEMBER")) { member =
			 * findMemberWithID(elementID); node.setMember(member); } }
			 * 
			 * node.setExpresssion(row.getString("EXPRESSION")); nodelist.add(node);
			 * System.out.println("node = " + node.toString());
			 * 
			 * }
			 */

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	/**
	 * get the MEMBER instance which corresponds to the String ID of the member
	 * @param elementID
	 * @return
	 */
	private static MEMBER findMemberWithID(String elementID) {
		EList<MEMBER> memberList = members.getMembers();
		MEMBER returnMember = null;
		for (Iterator iterator = memberList.iterator(); iterator.hasNext();) {
			MEMBER mem = (MEMBER) iterator.next();
			if (elementID.equals(mem.getMember_id()))
				returnMember = mem;
		}
		return returnMember;
	}


	/**
	 * get the VARIABLE instance which corresponds to the String ID of the 
	 * VARIABLE
	 * 
	 * @param elementID
	 * @return
	 */
	private static VARIABLE findVariableWithID(String elementID) {
		EList<VARIABLE> variableList = variables.getVariables();
		VARIABLE returnVariable = null;
		for (Iterator iterator = variableList.iterator(); iterator.hasNext();) {
			VARIABLE var = (VARIABLE) iterator.next();
			if (elementID.equals(var.getVariable_id()))
				returnVariable = var;
		}
		return returnVariable;
	}
	

	/**
	 * get the TRANSFORMATION_SCHEME instance which corresponds to the String ID 
	 * of the TRANSFORMATION_SCHEME
	 * 
	 * @param scheme_id
	 * @return
	 */
	private static TRANSFORMATION_SCHEME getTransformationScheme(String scheme_id) {
		
		EList<TRANSFORMATION_SCHEME> schemes = transformationSchemes.getSchemes();
		TRANSFORMATION_SCHEME returnScheme = null;
		for (Iterator iterator = schemes.iterator(); iterator.hasNext();) {
			TRANSFORMATION_SCHEME transformation_SCHEME = (TRANSFORMATION_SCHEME) iterator.next();
			if (scheme_id.equals(transformation_SCHEME.getTransformation_scheme_id()))
				returnScheme = transformation_SCHEME;
		}
		return returnScheme;

	}

	/** 
	 * create the MEMBER_MAPPING , VARIABLE_MAPPING ,  MAPPING_DEFINITION , CUBE_MAPPING
	 * and MAPPING_TO_CUBE model instances from the BIRD Access Database.
	 */
	public void createAllMappings() {

		// create all mapping defintions
		// attach all memebr mapping and variable maapings
		// create cubemapping module and link to created mapping defintions

		try {

			AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();
			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "TRANSFORMATION_SCHEME");

			for (AccessRow row : list) {

				MEMBER_MAPPING memberMapping = MappingFactory.eINSTANCE.createMEMBER_MAPPING();

				memberMapping.setMember_mapping_id(row.getString("MEMBER_MAPPING_ID"));
				memberMapping.setCode(row.getString("CODE"));
				memberMapping.setName(row.getString("NAME"));
				memberMapping.setMaintenance_agency_id(row.getString("MAINTENENCE_AGENCY_ID"));
				memberMappingModule.getMemberMappings().add(memberMapping);

			}
			list = accessUtils.getRowsForTable(filepath, "VARIABLE_MAPPING");

			for (AccessRow row : list) {

				VARIABLE_MAPPING variableMapping = MappingFactory.eINSTANCE.createVARIABLE_MAPPING();

				variableMapping.setVariable_mapping_id(row.getString("VARIABLE_MAPPING_ID"));
				variableMapping.setCode(row.getString("CODE"));
				variableMapping.setName(row.getString("NAME"));
				variableMapping.setMaintenance_agency_id(row.getString("MAINTENENCE_AGENCY_ID"));
				variableMappingModule.getVariableMappings().add(variableMapping);

			}

			list = accessUtils.getRowsForTable(filepath, "MAPPING_DEFINITION");

			for (AccessRow row : list) {

			
				MAPPING_DEFINITION mapping = MappingFactory.eINSTANCE.createMAPPING_DEFINITION();
				mapping.setCode(row.getString("CODE"));
				mapping.setMapping_id(row.getString("MAPPING_ID"));
				mapping.setName(row.getString("NAME"));
				mapping.setMapping_type((row.getString("MAPPING_TYPE")));
				String memberMappingString = (row.getString("MEMBER_MAPPING_ID"));

				if (memberMappingString != null) {
					mapping.setMemberMapping(findMemberMappingWithID(memberMappingString));
				}

				String variableMappingString = (row.getString("VARIABLE_MAPPING_ID"));
				if (variableMappingString != null) {
					mapping.setVariableMapping(findVariableMappingWithID(variableMappingString));
				}

				
				mappingDefinitionModule.getMappings().add(mapping);
			}

			// create the cubeMappings add them to the module
			// create the mapping to cube, link them by reference to the mapping defintions
			// then add them by containment to the cubeMapping

			list = accessUtils.getRowsForTable(filepath, "CUBE_MAPPING");

			for (AccessRow row : list) {

			
				CUBE_MAPPING mapping = MappingFactory.eINSTANCE.createCUBE_MAPPING();
				mapping.setCode(row.getString("CODE"));
				mapping.setCube_mapping_id(row.getString("CUBE_MAPPING_ID"));
				mapping.setName(row.getString("NAME"));
				mapping.setDescription(row.getString("DESCRIPTION"));
				mapping.setMaintenance_agency_id(row.getString("MAINTENENCE_AGENCY_ID"));
				String sourceCubeString = row.getString("SOURCE_CUBE_ID");
				String destinationCubeString = row.getString("DESTINATION_CUBE_ID");

				mapping.setSource_cube(findCubeWithID(sourceCubeString));
				mapping.setTarget_cube(findCubeWithID(destinationCubeString));
				cubeMappingModule.getCubeMappings().add(mapping);

				
			}

			list = accessUtils.getRowsForTable(filepath, "MAPPING_TO_CUBE");

			for (AccessRow row : list) {

				MAPPING_TO_CUBE mapping = MappingFactory.eINSTANCE.createMAPPING_TO_CUBE();

				String cubeMappingString = (row.getString("CUBE_MAPPING_ID"));
				String mappingString = (row.getString("MAPPING_ID"));
				mapping.setMapping(findMappingDefWithID(mappingString));

				CUBE_MAPPING cube_mapping = findCubeMappingWithID(cubeMappingString);
				cube_mapping.getMappingsToCube().add(mapping);

			
			}

		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}


	/**
	 * get the CUBE_MAPPING instance which corresponds to the String ID of the 
	 * CUBE_MAPPING
	 * 
	 * @param elementID
	 * @return
	 */
	private CUBE_MAPPING findCubeMappingWithID(String elementID) {
		EList<CUBE_MAPPING> cubeMappingList = cubeMappingModule.getCubeMappings();
		CUBE_MAPPING returnCubeMapping = null;
		for (Iterator iterator = cubeMappingList.iterator(); iterator.hasNext();) {
			CUBE_MAPPING cubeMapping = (CUBE_MAPPING) iterator.next();
			if (elementID.equals(cubeMapping.getCube_mapping_id()))
				returnCubeMapping = cubeMapping;
		}
		return returnCubeMapping;
	}

	/**
	 * get the MAPPING_DEFINITION instance which corresponds to the String ID of the 
	 * MAPPING_DEFINITION
	 * 
	 * @param elementID
	 * @return
	 */
	private MAPPING_DEFINITION findMappingDefWithID(String elementID) {
		EList<MAPPING_DEFINITION> mappingList = mappingDefinitionModule.getMappings();
		MAPPING_DEFINITION returnMapping = null;
		for (Iterator iterator = mappingList.iterator(); iterator.hasNext();) {
			MAPPING_DEFINITION mapping = (MAPPING_DEFINITION) iterator.next();
			if (elementID.equals(mapping.getMapping_id()))
				returnMapping = mapping;
		}
		return returnMapping;
	}

	/**
	 * get the CUBE instance which corresponds to the String ID of the 
	 * CUBE
	 * 
	 * @param elementID
	 * @return
	 */
	private CUBE findCubeWithID(String elementID) {
		EList<CUBE> cubeList = cubesModule.getCubes();
		CUBE returnCube = null;
		for (Iterator iterator = cubeList.iterator(); iterator.hasNext();) {
			CUBE cube = (CUBE) iterator.next();
			if (elementID.equals(cube.getCube_id()))
				returnCube = cube;
		}
		return returnCube;
	}

	/**
	 * get the VARIABLE_MAPPING instance which corresponds to the String ID of the 
	 * VARIABLE_MAPPING
	 * 
	 * @param elementID
	 * @return
	 */
	private VARIABLE_MAPPING findVariableMappingWithID(String elementID) {
		EList<VARIABLE_MAPPING> variableMappingList = variableMappingModule.getVariableMappings();
		VARIABLE_MAPPING returnVariableMapping = null;
		for (Iterator iterator = variableMappingList.iterator(); iterator.hasNext();) {
			VARIABLE_MAPPING varmapping = (VARIABLE_MAPPING) iterator.next();
			if (elementID.equals(varmapping.getVariable_mapping_id()))
				returnVariableMapping = varmapping;
		}
		return returnVariableMapping;
	}

	/**
	 * get the MEMBER_MAPPING instance which corresponds to the String ID of the 
	 * MEMBER_MAPPING
	 * 
	 * @param elementID
	 * @return
	 */
	private MEMBER_MAPPING findMemberMappingWithID(String elementID) {
		EList<MEMBER_MAPPING> memberMappingList = memberMappingModule.getMemberMappings();
		MEMBER_MAPPING returnMemberMapping = null;
		for (Iterator iterator = memberMappingList.iterator(); iterator.hasNext();) {
			MEMBER_MAPPING memMapping = (MEMBER_MAPPING) iterator.next();
			if (elementID.equals(memMapping.getMember_mapping_id()))
				returnMemberMapping = memMapping;
		}
		return returnMemberMapping;
	}

	/** 
	 * create the DOMAIN model instances from the BIRD Access Database
	 */
	public void createAllDomains() {
		AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();

		try {

			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "DOMAIN");
			for (AccessRow row : list) {
				// System.out.println("Column 'DOMAIN_ID' has value: " + row.get("DOMAIN_ID"));
				DOMAIN domain = CoreFactory.eINSTANCE.createDOMAIN();
				domain.setCode(row.getString("CODE"));
				domain.setDomain_id(row.getString("DOMAIN_ID"));
				domain.setDescription(row.getString("DESCRIPTION"));
				FACET_VALUE_TYPE valueType;
				String dataTypeString = row.getString("DATA_TYPE");
				if (dataTypeString != null) {
					if (dataTypeString.contains("tring")) {
						valueType = FACET_VALUE_TYPE.STRING;
						domain.setData_type(valueType);
					}
					if (dataTypeString.contains("nteger")) {
						valueType = FACET_VALUE_TYPE.BIG_INTEGER;
						domain.setData_type(valueType);
					}
					if (dataTypeString.contains("ate")) {
						valueType = FACET_VALUE_TYPE.DATE_TIME;
						domain.setData_type(valueType);
					}
					if (dataTypeString.contains("umber") || dataTypeString.contains("onetary")) {
						valueType = FACET_VALUE_TYPE.DECIMAL;
						domain.setData_type(valueType);
					}
					if (dataTypeString.contains("oolean")) {
						valueType = FACET_VALUE_TYPE.BOOLEAN;
						domain.setData_type(valueType);
					}
				}

				
				domain.setName(row.getString("NAME"));
				boolean is_enumerated = false;
				if (row.getBoolean("IS_ENUMERATED"))
					is_enumerated = true;
				domain.setIs_enumerated(is_enumerated);

				domains.getDomains().add(domain);
				
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}
	/** 
	 * create the MEMBER model instances from the BIRD Access Database
	 */
	public void createAllMembers() {

		AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();

		try {

			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "MEMBER");
			for (AccessRow row : list) {
			
				MEMBER member = CoreFactory.eINSTANCE.createMEMBER();
				member.setCode(row.getString("CODE"));
				member.setMember_id(row.getString("MEMBER_ID"));
				member.setDescription(row.getString("DESCRIPTION"));
				member.setName(row.getString("NAME"));
				String domainIDString = (row.getString("DOMAIN_ID"));
				DOMAIN domain = getDomainWithID(domainIDString);
				member.setDomain_id(domain);

				members.getMembers().add(member);
				
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	/**
	 * get the DOMAIN instance which corresponds to the String ID of the 
	 * DOMAIN
	 * 
	 * @param domainIDString
	 * @return
	 */
	private static DOMAIN getDomainWithID(String domainIDString) {

		EList<DOMAIN> domainList = domains.getDomains();
		DOMAIN returnDomain = null;
		for (Iterator iterator = domainList.iterator(); iterator.hasNext();) {
			DOMAIN dom = (DOMAIN) iterator.next();
			if (domainIDString.equals(dom.getDomain_id()))
				returnDomain = dom;
		}
		return returnDomain;
	}

	
	/** 
	 * create the VARIABLE model instances from the BIRD Access Database
	 */
	public void createAllVariables() {
		AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();

		try {

			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "VARIABLE");

			for (AccessRow row : list) {
			
				VARIABLE variable = CoreFactory.eINSTANCE.createVARIABLE();
				variable.setCode(row.getString("CODE"));
				variable.setVariable_id(row.getString("VARIABLE_ID"));
				variable.setDescription(row.getString("DESCRIPTION"));
				variable.setName(row.getString("NAME"));
				String domainIDString = (row.getString("DOMAIN_ID"));
				DOMAIN domain = getDomainWithID(domainIDString);
				variable.setDomain_id(domain);

				variables.getVariables().add(variable);
				
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	/** 
	 * create the CUBE_STRUCTURE, CUBE,  and CUBE_STRUCTURE_ITEM model instances 
	 * from the BIRD Access Database
	 */
	public void createAllCubes() {
		AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();
		try {
			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "CUBE_STRUCTURE");

			for (AccessRow row : list) {
				
				CUBE_STRUCTURE cube_structure = Data_definitionFactory.eINSTANCE.createCUBE_STRUCTURE();
				cube_structure.setCode(row.getString("CODE"));
				cube_structure.setCube_structure_id(row.getString("CUBE_STRUCTURE_ID"));
				cube_structure.setName(row.getString("NAME"));
				cube_structure.setDescription("DESCRIPTION");
				cube_structure.setValid_to(row.getDate("VALID_TO"));
				
				cubeStructuresModule.getCubeStructures().add(cube_structure);
			}
			list = accessUtils.getRowsForTable(filepath, "CUBE");
			for (AccessRow row : list) {
				
				CUBE cube = Data_definitionFactory.eINSTANCE.createCUBE();
				cube.setCode(row.getString("CODE"));
				cube.setCube_id(row.getString("CUBE_ID"));
				cube.setName(row.getString("NAME"));
				cube.setCube_type(row.getString("CUBE_TYPE"));
				cube.setDescription("DESCRIPTION");
				String cubeStructureString = row.getString("CUBE_STRUCTURE_ID");
				CUBE_STRUCTURE structure = findCubeStructureWithID(cubeStructureString);
				cube.setCube_structure_id(structure);

				cubesModule.getCubes().add(cube);
			}

			list = accessUtils.getRowsForTable(filepath, "CUBE");
			for (AccessRow row : list) {

				CUBE_STRUCTURE_ITEM item = Data_definitionFactory.eINSTANCE.createCUBE_STRUCTURE_ITEM();
				String structureString = (row.getString("CUBE_STRUCTURE_ID"));

				CUBE_STRUCTURE structure = findCubeStructureWithID(structureString);
				item.setCube_structure_id(structure);

				String variableString = (row.getString("VARIABLE_ID"));
				if (variableString != null) {
					VARIABLE variable = findVariableWithID(variableString);
					item.setVariable_id(variable);
				}
				String memberString = (row.getString("MEMBER_ID"));
				if (memberString != null) {
					MEMBER member = findMemberWithID(memberString);
					item.setMember_id(member);
				}
				item.setCube_variable_code(row.getString("CUBE_VARIABLE_CODE"));
				item.setDescription(row.getString("DESCRIPTION"));
				try {
					int order = row.getInt("ORDER");
					item.setOrder(order);
				} catch (NullPointerException e) {
					 System.out.println("null order");
				}
				String subdomain = (row.getString("SUBDOMAIN_ID"));
				if (subdomain != null && subdomain.contains("RSTRCTD") && subdomain.contains("ID"))
					item.setIsIdentifier(true);
				else
					item.setIsIdentifier(false);

				cubeStructureItemsModule.getCubeStructureItems().add(item);
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}

	/**
	 * get the CUBE_STRUCTURE instance which corresponds to the String ID of the 
	 * CUBE_STRUCTURE
	 * 
	 * @param elementID
	 * @return
	 */
	private static CUBE_STRUCTURE findCubeStructureWithID(String elementID) {
		EList<CUBE_STRUCTURE> structureList = cubeStructuresModule.getCubeStructures();
		CUBE_STRUCTURE returnStructure = null;
		for (Iterator iterator = structureList.iterator(); iterator.hasNext();) {
			CUBE_STRUCTURE structure = (CUBE_STRUCTURE) iterator.next();
			if (elementID.equals(structure.getCube_structure_id()))
				returnStructure = structure;
		}
		return returnStructure;
	}

	/** 
	 * create the COMBINATION model instances from the BIRD Access Database.
	 */
	public void createAllCombinations() {
		AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();
		try {
			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "COMBINATION");
			int counter = 0;
			CombinationModule combinationsModule = null;
			for (AccessRow row : list) {

				if (counter == 0) {
					combinationsModule = Efbt_data_definitionFactory.eINSTANCE.createCombinationModule();
					combinationsModules.add(combinationsModule);
					birdModel.getCombinations().add(combinationsModule);
				}

				counter++;
				if (counter == 100)
					counter = 0;

				
				COMBINATION comb = Data_definitionFactory.eINSTANCE.createCOMBINATION();
				comb.setCode(row.getString("CODE"));
				comb.setCombination_id(row.getString("COMBINATION_ID"));
				comb.setName(row.getString("NAME"));
				
				combinationsModule.getCombinations().add(comb);
			}

			list = accessUtils.getRowsForTable(filepath, "COMBINATION_ITEM");
			for (AccessRow row : list) {

				COMBINATION_ITEM item = Data_definitionFactory.eINSTANCE.createCOMBINATION_ITEM();
				String combinationString = (row.getString("COMBINATION_ID"));
				String variableString = (row.getString("VARIABLE_ID"));
				String memberString = (row.getString("MEMBER_ID"));
				COMBINATION com = findCombinationWithID(combinationString);
				if (memberString != null) {
					MEMBER mem = findMemberWithID(memberString);
					item.setMember_id(mem);
				}
				if (variableString != null) {
					VARIABLE variable = findVariableWithID(variableString);
					item.setVariable_id(variable);
				}
				
				com.getCombination_items().add(item);

				
			}
		} catch (IOException e) {
		
			e.printStackTrace();
		}

	}

	/**
	 * get the COMBINATION instance which corresponds to the String ID of the 
	 * COMBINATION
	 * 
	 * @param elementID
	 * @return
	 */
	private static COMBINATION findCombinationWithID(String elementID) {
		COMBINATION returnCombination = null;
		for (Iterator iterator = combinationsModules.iterator(); iterator.hasNext();) {
			CombinationModule combinationModule = (CombinationModule) iterator.next();
			EList<COMBINATION> combinationList = combinationModule.getCombinations();

			for (Iterator iterator2 = combinationList.iterator(); iterator2.hasNext();) {
				COMBINATION com = (COMBINATION) iterator2.next();
				if (elementID.equals(com.getCombination_id()))
					returnCombination = com;
			}

		}

		return returnCombination;
	}

	
	/**
	 * from Test data sored in CSV format , create all the model instances of
	 * Test and Store them in the list of  testPrograms. Note that to do this
	 * we will create instances of TestDefintions TestConstraints, 
	 * and TestTemplates which the Test instance will refer to.
	 */
	public void importTestData() {
		// get the csv file, use an open source csv library.
		// create 1 test definition
		// create mutliple tests, with that data.
		
		//create the TestDefintion Module to hold the different TestDefinitions
		TestDefinitionModule definitionModule = Test_definitionFactory.eINSTANCE.
				createTestDefinitionModule();
		definitionModule.setName("testDefinitionsModule");
		
		//create the TestTemplateModule to hold the different TestDefinitions
		TestTemplateModule testTemplateModule = Test_definitionFactory.eINSTANCE.
				createTestTemplateModule();
		testTemplateModule.setName("testTemplateModule");
		testTemplateProgram.setTestTemplates(testTemplateModule);
		testDefinitionProgram.setTestDefinitions(definitionModule);

		//create a TestTemplate and add it to the Test Template Module,
		// all our tests are going to use the same template
		TestTemplate testTemplate = 
				Test_definitionFactory.eINSTANCE.createTestTemplate();
		Param templateparam = Test_definitionFactory.eINSTANCE.createParam();
		templateparam.setParam(functionalityModulesProgram.
				getFunctionalityModules().getFunctionalityModules().get(0));
		testTemplate.getWhenParams().add(templateparam);
		testTemplateModule.getTemplates().add(testTemplate);

		//Set the given when then clauses of the Test Definition, all our 
		//tests are going to use the same TestDefintion
		ClauseText given_clauseText = 
				Test_definitionFactory.eINSTANCE.createClauseText();
		given_clauseText.setName("given_test_input_data");
		testTemplate.setGivenText(given_clauseText);
		ClauseText when_clauseText = 
				Test_definitionFactory.eINSTANCE.createClauseText();
		when_clauseText.setName("when_transformation_sheme_is_run");
		testTemplate.setWhenText(when_clauseText);
		ClauseText then_clauseText = 
				Test_definitionFactory.eINSTANCE.createClauseText();
		then_clauseText.setName("then_specific_output_data_is_expected");
		testTemplate.setGivenText(then_clauseText);
		testTemplateModule.getTemplates().add(testTemplate);
		//create a TestConstraint and add it to the Test Constrinats Module,
		// all our tests are going to use the same constriant
		TestContraints contraints = 
				Test_definitionFactory.eINSTANCE.createTestContraints();
		contraints.setName("constraints");
		// constraintsModule.getCoverageTestSets().add(contraints);
		testConstraintsProgram.setTestConstriants(contraints);
		
		
		Param contraintsparam = Test_definitionFactory.eINSTANCE.createParam();
		contraintsparam
				.setParam(functionalityModulesProgram.getFunctionalityModules().
						getFunctionalityModules().get(0));
		contraints.getWhenParams().add(contraintsparam);

		E2ETestDefinition definition =
					Test_definitionFactory.eINSTANCE.createE2ETestDefinition();
		definition.setName("standard_test");
		definitionModule.getTestDefinitions().add(definition);

		Param defparam = Test_definitionFactory.eINSTANCE.createParam();
		defparam.setParam(
				functionalityModulesProgram.getFunctionalityModules().
				getFunctionalityModules().get(1));
		
		When when = Test_definitionFactory.eINSTANCE.createWhen();
		Then then = Test_definitionFactory.eINSTANCE.createThen();
		Given given = Test_definitionFactory.eINSTANCE.createGiven();
		definition.setWhen(when);
		definition.setThen(then);
		definition.setGiven(given);
		definition.getWhen().getParams().add(defparam);

		definition.setTestContraints(contraints);
		testDefinitionProgram.setTestDefinitions(definitionModule);

		//create the Test model instances from the CSV test data
				
		FileReader csvData;
		try {

			csvData = new FileReader(new File(testdatafilepath));

			CSVUtils csvUtils = CSVUtilsProvider.getCSVUtils();
			List<CSVRow> list = csvUtils.getCSVRowsFromFile(testdatafilepath);

			HashMap<String, E2ETest> tests = new HashMap<String, E2ETest>();
			HashMap<String, BaseColumnStructuredData> tables = new HashMap<String, BaseColumnStructuredData>();
			HashMap<String, BaseRowData> rows = new HashMap<String, BaseRowData>();

			for (CSVRow csvRecord : list) {

				//get a row of data which wil relate to the value of one column
				//in a cube
				String id1 = csvRecord.get(0);
				String id2 = csvRecord.get(1);
				String record_no = csvRecord.get(2);
				String cube = csvRecord.get(3);
				String variable = csvRecord.get(4);
				String value = csvRecord.get(5);
				//check whhich test this value is for, and see 
				//if we already have created a Test for that test
				E2ETest test = tests.get(id1 + ":" + id2);
				if (test == null) {
					//if we dont have aTest for this item of Test data, then 
					//we create one and add it to the list of tests
					Program testProgram = Aorta_programFactory.eINSTANCE.createProgram();
					testPrograms.add(testProgram);
					TestModule testModule = TestFactory.eINSTANCE.createTestModule();
					testProgram.setTests(testModule);
					test = TestFactory.eINSTANCE.createE2ETest();
					test.setName(id1 + ":" + id2);
					test.setTestDefinition(definition);
					testModule.getTests().add(test);
					tests.put(id1 + ":" + id2, test);
					TestColumnStructuredData inputData2 = Test_input_dataFactory.eINSTANCE
							.createTestColumnStructuredData();
					test.setInputData(inputData2);
				}
				TestColumnStructuredData inputData = (TestColumnStructuredData) test.getInputData();

				// so we need a table for each table, then a row for each row, and a cell for
				// each cell.
				// so we make a hash map of sorts

				BaseColumnStructuredData table = tables.get(id1 + ":" + id2 + ":" + cube);
				if (table == null) {
					BaseColumnStructuredData structuredData = Base_column_structured_dataFactory.eINSTANCE
							.createBaseColumnStructuredData();
					structuredData.setCube(getCubeForCubeName(cube));
					tables.put(id1 + ":" + id2 + ":" + cube, structuredData);
					inputData.getSourceTableData().add(structuredData);
					table = structuredData;

				}

				BaseRowData rowData = rows.get(id1 + ":" + id2 + ":" + cube + ":" + record_no);
				if (rowData == null) {
					BaseRowData baseRow = Base_column_structured_dataFactory.eINSTANCE.createBaseRowData();
					baseRow.setRowID(record_no);
					rows.put(id1 + ":" + id2 + ":" + cube + ":" + record_no, baseRow);
					table.getRows().add(baseRow);
					rowData = baseRow;
				}

				// determine if the associated Variable is enumerated or not.
				// if so then get the correct enumerated value, if it does not exist, then put
				// this in the error report.
				
				boolean cubeExists = checkCubeExists(cube);
				if (cubeExists) {
					Column theColumn = getColumnFromCube(cube, variable);

					if (theColumn != null) {
						boolean isEnumeratedColumn = checkIsEnumeratedColumn(cube, variable);
						if (isEnumeratedColumn) {
							EnumMember member = getMember(cube, variable, value);
							if (member != null) {

								BaseCellWithEnumeratedValue enumcell = Base_column_structured_dataFactory.eINSTANCE
										.createBaseCellWithEnumeratedValue();
								enumcell.setColumn(theColumn);
								enumcell.setValue(member);
								enumcell.setCellID(theColumn.getName() + ":" + record_no);
								rowData.getCells().add(enumcell);

							} else {
								System.out.println("Cannot find member:" + id1 + ":" + id2 + ":" + cube + ":"
										+ record_no + ":" + variable + ":" + value);
							}

						} else {
							BaseCellWithValue cellWithValue = Base_column_structured_dataFactory.eINSTANCE
									.createBaseCellWithValue();
							cellWithValue.setValue(value);
							cellWithValue.setColumn(theColumn);
							cellWithValue.setCellID(theColumn.getName() + ":" + record_no);
							rowData.getCells().add(cellWithValue);
						}

					} else {
						System.out.println("Cannot find column:" + id1 + ":" + id2 + ":" + cube + ":" + record_no + ":"
								+ variable);
					}

				} else {
					System.out.println("Cannot find cube:" + id1 + ":" + id2 + ":" + cube);
				}

			}

		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}

	/**
	 * Get the ColumnStructuredEntity for a String cube ID.
	 * @param cube
	 * @return
	 */
	private ColumnStructuredEntity getCubeForCubeName(String cube) {
		
		EList<ColumnStructuredEntity> column_structured_entities =
				iputDataStructuresProgram.getInput_structures()
				.getColumnStructures();
		for (Iterator iterator = 
				column_structured_entities.iterator(); iterator.hasNext();) {
			ColumnStructuredEntity columnStructuredEntity =
					(ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube))
				return columnStructuredEntity;
		}
		return null;
	}
	/**
	 * Get the related EnumMeber
	 * 
	 * @param cube
	 * @param variable
	 * @param value
	 * @return
	 */
	private EnumMember getMember(String cube, String variable, String value) {
		
		EList<ColumnStructuredEntity> cubes = iputDataStructuresProgram.
				getInput_structures().getColumnStructures();
		EnumMember returnedMember = null;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) 
		{
			ColumnStructuredEntity columnStructuredEntity = 
					(ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube)) 
			{
				EList<Column> columns = columnStructuredEntity.getColumn();
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) 
				{
					Column column = (Column) iterator2.next();
					if (column.getName().equalsIgnoreCase(variable)) 
					{
						EList<EnumMember> theMembers = column.getDomain().getEnumMembers();
						for (Iterator iterator3 = theMembers.iterator(); iterator3.hasNext();) 
						{
							EnumMember enumMember = (EnumMember) iterator3.next();

							if (enumMember.getCode().equalsIgnoreCase(value))
								returnedMember = enumMember;

						}
					}
				}
			}

		}
		return returnedMember;
	}

	/**
	 * Return true if the variable for a cube is an enumberated varaible
	 * otherwise return false.
	 * 
	 * @param cube
	 * @param variable
	 * @return
	 */
	private boolean checkIsEnumeratedColumn(String cube, String variable) {
	
		EList<ColumnStructuredEntity> cubes = iputDataStructuresProgram.getInput_structures().getColumnStructures();
		boolean isEnumerated = false;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
			ColumnStructuredEntity columnStructuredEntity = (ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube)) {
				EList<Column> columns = columnStructuredEntity.getColumn();
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
					Column column = (Column) iterator2.next();
					if (column.getName().equalsIgnoreCase(variable))
						isEnumerated = column.getDomain().isIsEnumerated();
				}
			}

		}
		return isEnumerated;
	}

	/**
	 * Get the Column instance for a particular cube, 
	 * which is for a particular Varaible
	 * @param cube
	 * @param variable
	 * @return
	 */
	private Column getColumnFromCube(String cube, String variable) {
		
		EList<ColumnStructuredEntity> cubes = iputDataStructuresProgram.getInput_structures().getColumnStructures();
		Column returnedColumn = null;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
			ColumnStructuredEntity columnStructuredEntity = (ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube)) {
				EList<Column> columns = columnStructuredEntity.getColumn();
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
					Column column = (Column) iterator2.next();
					if (column.getName().equalsIgnoreCase(variable))
						returnedColumn = column;
				}
			}
		}

		return returnedColumn;
	}

	/**
	 * Return true if the cube exists, otherwise return false.
	 * 
	 * @param cube
	 * @return
	 */
	private boolean checkCubeExists(String cube) {
		
		EList<ColumnStructuredEntity> cubes = iputDataStructuresProgram.getInput_structures().getColumnStructures();
		boolean exists = false;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
			ColumnStructuredEntity columnStructuredEntity = (ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube))
				exists = true;

		}
		return exists;
	}

	/**
	 * Create the initial model instances of Aorta Programs from the BIRD 
	 * related models that were created from the BIRD Acces Database.
	 */
	public void createAortaFiles() {
		// create Aorta Program

		testDefinitionProgram = Aorta_programFactory.eINSTANCE.createProgram();
		testTemplateProgram = Aorta_programFactory.eINSTANCE.createProgram();
		testConstraintsProgram = Aorta_programFactory.eINSTANCE.createProgram();
		iputDataStructuresProgram = Aorta_programFactory.eINSTANCE.createProgram();
		domainsProgram = Aorta_programFactory.eINSTANCE.createProgram();
		functionalityModulesProgram = Aorta_programFactory.eINSTANCE.createProgram();
		testPrograms = new BasicEList<Program>();

		ColumnDomainModule domainModule = Column_structuresFactory.eINSTANCE.createColumnDomainModule();
		domainModule.setName("domainsModule");
		domainsProgram.setDomainModule(domainModule);
	
		EList<DOMAIN> domainList = domains.getDomains();
		for (Iterator iterator = domainList.iterator(); iterator.hasNext();) {
			DOMAIN domain = (DOMAIN) iterator.next();
			ColumnDomain columndomain = Column_structuresFactory.eINSTANCE.createColumnDomain();
			columndomain.setName(domain.getDomain_id());
			columndomain.setIsEnumerated(domain.isIs_enumerated());
			ColumnDataType datatype = Column_structuresFactory.eINSTANCE.createColumnDataType();
			datatype.setName(domain.getData_type().getName());
			columndomain.setDataType(datatype);
			domainModule.getColumnDomains().add(columndomain);
		}
		EList<MEMBER> memberList = members.getMembers();
		for (Iterator iterator = memberList.iterator(); iterator.hasNext();) {
			MEMBER member = (MEMBER) iterator.next();
			EnumMember enumMember = Column_structuresFactory.eINSTANCE.createEnumMember();
			enumMember.setName(member.getMember_id());
			enumMember.setCode(member.getCode());
			addEnumMeberToCorrectColumnDomain(enumMember, member.getDomain_id().getDomain_id(), domainModule);

		}

		ColumnStructureModule columnStructureModule = Column_structuresFactory.eINSTANCE.createColumnStructureModule();
		columnStructureModule.setName("columnStructureModule");
		iputDataStructuresProgram.setInput_structures(columnStructureModule);

		// create a columnstructure entity for each columnstructure
		EList<CUBE_STRUCTURE> cubeStructures = cubeStructuresModule.getCubeStructures();
		for (Iterator iterator = cubeStructures.iterator(); iterator.hasNext();) {
			CUBE_STRUCTURE cube_STRUCTURE = (CUBE_STRUCTURE) iterator.next();
			Date validToDate = cube_STRUCTURE.getValid_to();
			Date when = new Date(3000, 1, 1);
			if (validToDate.after(when)) {
				ColumnStructuredEntity columnStructuredEntity = Column_structuresFactory.eINSTANCE
						.createColumnStructuredEntity();
				columnStructuredEntity.setName(cube_STRUCTURE.getCode());
				columnStructureModule.getColumnStructures().add(columnStructuredEntity);

			}
		}

		EList<CUBE_STRUCTURE_ITEM> cubeStructureItems = cubeStructureItemsModule.getCubeStructureItems();
		for (Iterator iterator = cubeStructureItems.iterator(); iterator.hasNext();) {
			CUBE_STRUCTURE_ITEM cube_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM) iterator.next();
			Column column = Column_structuresFactory.eINSTANCE.createColumn();
			column.setName(cube_STRUCTURE_ITEM.getCube_variable_code());
			column.setIsMandatory(cube_STRUCTURE_ITEM.isIs_mandatory());
			column.setIsIdentifier(cube_STRUCTURE_ITEM.isIsIdentifier());
			setColumnDomain(column, domainModule, cube_STRUCTURE_ITEM.getVariable_id().getDomain_id().getDomain_id());
			addColumntoColumnStructuredEntity(column, columnStructureModule,
					cube_STRUCTURE_ITEM.getCube_structure_id().getCode());
		}

		FunctionalityModuleModule functionalityModuleModule = Functionality_moduleFactory.eINSTANCE
				.createFunctionalityModuleModule();
		functionalityModuleModule.setName("functionalityModule");

		functionalityModulesProgram.setFunctionalityModules(functionalityModuleModule);

		EList<TRANSFORMATION_SCHEME> schemes = transformationSchemes.getSchemes();
		for (Iterator iterator = schemes.iterator(); iterator.hasNext();) {
			TRANSFORMATION_SCHEME transformation_SCHEME = (TRANSFORMATION_SCHEME) iterator.next();
			FunctionalityModule functionalityModule = Functionality_moduleFactory.eINSTANCE
					.createLeafFunctionalityModule();
			functionalityModule.setName(transformation_SCHEME.getTransformation_scheme_id());
			functionalityModuleModule.getFunctionalityModules().add(functionalityModule);

		}

	}

	/**
	 * Persist the Aorta model instances as XML files.
	 */
	public void persistAortaFiles() {

		try {

			FreeBirdToolsResourceFactory factory = new FreeBirdToolsResourceFactory();

			URI testDefinitionProgramURI = URI.createFileURI(outputFilepath + "testDefinition.aorta_program");
			URI testTemplateProgramURI = URI.createFileURI(outputFilepath + "testTemplate.aorta_program");
			URI testConstraintsProgramURI = URI.createFileURI(outputFilepath + "testConstraints.aorta_program");
			URI inputDataStructuresProgramURI = URI.createFileURI(outputFilepath + "inputDataStructures.aorta_program");
			URI domainsProgramURI = URI.createFileURI(outputFilepath + "domains.aorta_program");
			URI functionalityModulesProgramURI = URI
					.createFileURI(outputFilepath + "functionalityModules.aorta_program");

			Resource testDefintionResource = factory.createResource(testDefinitionProgramURI);
			Resource testTemplateResource = factory.createResource(testTemplateProgramURI);
			Resource testConstraintsResource = factory.createResource(testConstraintsProgramURI);
			Resource inputDataStructuresResource = factory.createResource(inputDataStructuresProgramURI);
			Resource domainsResource = factory.createResource(domainsProgramURI);
			Resource functionalityModulesResource = factory.createResource(functionalityModulesProgramURI);

			testDefintionResource.getContents().add(testDefinitionProgram);
			testTemplateResource.getContents().add(testTemplateProgram);
			testConstraintsResource.getContents().add(testConstraintsProgram);
			inputDataStructuresResource.getContents().add(iputDataStructuresProgram);
			domainsResource.getContents().add(domainsProgram);
			functionalityModulesResource.getContents().add(functionalityModulesProgram);

			testDefintionResource.save(Collections.EMPTY_MAP);
			testTemplateResource.save(Collections.EMPTY_MAP);
			testConstraintsResource.save(Collections.EMPTY_MAP);
			inputDataStructuresResource.save(Collections.EMPTY_MAP);
			domainsResource.save(Collections.EMPTY_MAP);
			functionalityModulesResource.save(Collections.EMPTY_MAP);

			int counter = 0;
			for (Iterator iterator = testPrograms.iterator(); iterator.hasNext();) {
				counter++;
				URI testProgramURI = URI.createFileURI(outputFilepath + "test" + counter + ".aorta_program");
				Program testProgram = (Program) iterator.next();
				Resource testProgramResource = factory.createResource(testProgramURI);
				testProgramResource.getContents().add(testProgram);
				testProgramResource.save(Collections.EMPTY_MAP);

			}

		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	/**
	 * Adds a Column the the correct ColumnStructuredEntity in a 
	 * ColumnStructureModule depending upon the string cube_structure_id 
	 * matching the name of a  ColumnStructuredEntity.
	 * 
	 * @param column
	 * @param columnStructureModule
	 * @param cube_structure_id
	 */
	private void addColumntoColumnStructuredEntity(Column column, ColumnStructureModule columnStructureModule,
			String cube_structure_id) {

		EList<ColumnStructuredEntity> columnStructures = columnStructureModule.getColumnStructures();
		for (Iterator iterator = columnStructures.iterator(); iterator.hasNext();) {
			ColumnStructuredEntity columnStructuredEntity = (ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube_structure_id))
				columnStructuredEntity.getColumn().add(column);

		}
	}

	/**
	 * Sets the domain od a Column correctly depending upon the String domainID matching
	 * the name of a Domain in the domainModule.
	 * 
	 * @param column
	 * @param domainModule
	 * @param domainID
	 */
	private void setColumnDomain(Column column, ColumnDomainModule domainModule, String domainID) {
		
		EList<ColumnDomain> columnDomains = domainModule.getColumnDomains();
		for (Iterator iterator = columnDomains.iterator(); iterator.hasNext();) {
			ColumnDomain columnDomain = (ColumnDomain) iterator.next();

			if (columnDomain.getName().equalsIgnoreCase(domainID))
				column.setDomain(columnDomain);
		}
	}

	/**
	 * Adds a member to the correct domain depending upon the String domainID matching
	 * the name of a Domain in the domainModule.
	 * @param enumMember
	 * @param domainID
	 * @param domainModule
	 */
	private void addEnumMeberToCorrectColumnDomain(EnumMember enumMember, String domainID,
			ColumnDomainModule domainModule) {

		EList<ColumnDomain> columnDomains = domainModule.getColumnDomains();
		for (Iterator iterator = columnDomains.iterator(); iterator.hasNext();) {
			ColumnDomain columnDomain = (ColumnDomain) iterator.next();

			if (columnDomain.getName().equalsIgnoreCase(domainID))
				columnDomain.getEnumMembers().add(enumMember);

		}
	}
	
	/**
	 * A main method for initial test.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		BIRDImporter importer = new BIRDImporter();
		importer.filepath = args[0];
		importer.outputFilepath = args[1];
		testdatafilepath = args[2];
		
		importer.doImport();
		importer.createAortaFiles();
		importer.importTestData();

		importer.persistAortaFiles();
	}

}
