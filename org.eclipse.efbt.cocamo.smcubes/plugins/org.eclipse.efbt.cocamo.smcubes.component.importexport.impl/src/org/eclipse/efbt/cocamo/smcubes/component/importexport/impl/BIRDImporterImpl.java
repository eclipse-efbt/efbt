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
package org.eclipse.efbt.cocamo.smcubes.component.importexport.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.efbt.cocamo.core.model.test.E2ETest;
import org.eclipse.efbt.cocamo.core.model.test.TestFactory;
import org.eclipse.efbt.cocamo.core.model.test.TestModule;
import org.eclipse.efbt.cocamo.core.model.test_definition.ClauseText;
import org.eclipse.efbt.cocamo.core.model.test_definition.E2ETestDefinition;
import org.eclipse.efbt.cocamo.core.model.test_definition.Given;
import org.eclipse.efbt.cocamo.core.model.test_definition.Param;
import org.eclipse.efbt.cocamo.core.model.test_definition.TestContraints;
import org.eclipse.efbt.cocamo.core.model.test_definition.TestDefinitionModule;
import org.eclipse.efbt.cocamo.core.model.test_definition.TestTemplate;
import org.eclipse.efbt.cocamo.core.model.test_definition.TestTemplateModule;
import org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionFactory;
import org.eclipse.efbt.cocamo.core.model.test_definition.Then;
import org.eclipse.efbt.cocamo.core.model.test_definition.When;
import org.eclipse.efbt.cocamo.core.model.test_input_data.TestInputData;
import org.eclipse.efbt.cocamo.core.model.test_input_data.Test_input_dataFactory;
import org.eclipse.efbt.cocamo.smcubes.component.access.api.AccessRow;
import org.eclipse.efbt.cocamo.smcubes.component.access.api.AccessUtils;
import org.eclipse.efbt.cocamo.smcubes.component.access.provider.AccessUtilProvider;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithValue;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataFactory;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.CocamoFactory;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTest;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestInputData;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestModule;
import org.eclipse.efbt.cocamo.smcubes.model.core.CoreFactory;
import org.eclipse.efbt.cocamo.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.cocamo.smcubes.model.core.FACET_VALUE_TYPE;
import org.eclipse.efbt.cocamo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocamo.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.COMBINATION;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.COMBINATION_ITEM;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.Data_definitionFactory;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.Efbt_data_definitionFactory;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.CUBE_MAPPING;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.MAPPING_DEFINITION;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.MAPPING_TO_CUBE;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.MEMBER_MAPPING;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.MappingFactory;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.VARIABLE_MAPPING;
import org.eclipse.efbt.cocamo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME;
import org.eclipse.efbt.cocamo.smcubes.model.vtl_transformation.Vtl_transformationFactory;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * This class is respnsable for structure data taking data from the BIRD Access
 * Database and also data in CSV defining Test data, and creates instances of
 * the cocamo SMCubesStaticModel model.
 * 
 * @author Neil Mackenzie
 *
 */
public class BIRDImporterImpl extends Importer {

	public BIRDImporterImpl() {
		super();
	}

	public void doImport(String theFilepath, String theOutputFilepath, String theTestdatafilepath) {

		super.doImport(theFilepath, theOutputFilepath, theTestdatafilepath);
	}

	/**
	 * Returns the file path of the access database
	 * 
	 * @return
	 */
	public String getFilepath() {
		return filepath;
	}

	/**
	 * Sets the file path of the access database
	 * 
	 */
	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}

	/**
	 * create the TRANSFORMATION_SCHEME model instances from the BIRD Access
	 * Database
	 */
	public void createAllTransformationSchemes() {

		try {

			AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();
			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "TRANSFORMATION_SCHEME");

			for (AccessRow row : list) {

				TRANSFORMATION_SCHEME scheme = Vtl_transformationFactory.eINSTANCE.createTRANSFORMATION_SCHEME();
				scheme.setCode(row.getString("CODE"));
				scheme.setTransformation_scheme_id(replaceDots(row.getString("TRANSFORMATION_SCHEME_ID")));
				scheme.setName(replaceDots(row.getString("TRANSFORMATION_SCHEME_ID")));
				scheme.setDescription(row.getString("DESCRIPTION"));
				scheme.setDisplayName(row.getString("NAME"));
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

	private String replaceDots(String string) {
		// TODO Auto-generated method stub
		return string.replace('.','_');
	}

	/**
	 * get the MEMBER instance which corresponds to the String ID of the member
	 * 
	 * @param elementID
	 * @return
	 */
	private MEMBER findMemberWithID(String elementID) {
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
	 * get the VARIABLE instance which corresponds to the String ID of the VARIABLE
	 * 
	 * @param elementID
	 * @return
	 */
	private VARIABLE findVariableWithID(String elementID) {
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
	 * get the TRANSFORMATION_SCHEME instance which corresponds to the String ID of
	 * the TRANSFORMATION_SCHEME
	 * 
	 * @param scheme_id
	 * @return
	 */
	private TRANSFORMATION_SCHEME getTransformationScheme(String scheme_id) {

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
	 * create the MEMBER_MAPPING , VARIABLE_MAPPING , MAPPING_DEFINITION ,
	 * CUBE_MAPPING and MAPPING_TO_CUBE model instances from the BIRD Access
	 * Database.
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

				memberMapping.setMember_mapping_id(replaceDots(row.getString("MEMBER_MAPPING_ID")));
				memberMapping.setCode(row.getString("CODE"));
				memberMapping.setName(replaceDots(row.getString("NAME")));
				memberMapping.setMaintenance_agency_id(row.getString("MAINTENENCE_AGENCY_ID"));
				memberMappingModule.getMemberMappings().add(memberMapping);

			}
			list = accessUtils.getRowsForTable(filepath, "VARIABLE_MAPPING");

			for (AccessRow row : list) {

				VARIABLE_MAPPING variableMapping = MappingFactory.eINSTANCE.createVARIABLE_MAPPING();

				variableMapping.setVariable_mapping_id(replaceDots(row.getString("VARIABLE_MAPPING_ID")));
				variableMapping.setCode(row.getString("CODE"));
				variableMapping.setName(replaceDots(row.getString("NAME")));
				variableMapping.setMaintenance_agency_id(row.getString("MAINTENENCE_AGENCY_ID"));
				variableMappingModule.getVariableMappings().add(variableMapping);

			}

			list = accessUtils.getRowsForTable(filepath, "MAPPING_DEFINITION");

			for (AccessRow row : list) {

				MAPPING_DEFINITION mapping = MappingFactory.eINSTANCE.createMAPPING_DEFINITION();
				mapping.setCode(row.getString("CODE"));
				mapping.setMapping_id(replaceDots(row.getString("MAPPING_ID")));
				mapping.setName(replaceDots(row.getString("NAME")));
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
				mapping.setCube_mapping_id(replaceDots(row.getString("CUBE_MAPPING_ID")));
				mapping.setName(replaceDots(row.getString("NAME")));
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
	 * get the CUBE instance which corresponds to the String ID of the CUBE
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
				
				DOMAIN domain = CoreFactory.eINSTANCE.createDOMAIN();
				domain.setCode(row.getString("CODE"));
				domain.setDomain_id(replaceDots(row.getString("DOMAIN_ID")));
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

				domain.setName(replaceDots(row.getString("DOMAIN_ID")));
				domain.setDisplayName(row.getString("NAME"));
				boolean is_enumerated = false;
				if (row.getBoolean("IS_ENUMERATED").booleanValue())
					is_enumerated = true;
				domain.setIs_enumerated(is_enumerated);
				
				boolean is_referenced = false;
				if (row.getBoolean("IS_REFERENCE").booleanValue())
					is_referenced = true;
				domain.setIs_reference(is_referenced);

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
				member.setMember_id(replaceDots(row.getString("MEMBER_ID")));
				member.setDescription(row.getString("DESCRIPTION"));
				member.setName(replaceDots(row.getString("MEMBER_ID")));
				member.setDisplayName(row.getString("NAME"));
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
	 * get the DOMAIN instance which corresponds to the String ID of the DOMAIN
	 * 
	 * @param domainIDString
	 * @return
	 */
	private DOMAIN getDomainWithID(String domainIDString) {

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
				variable.setVariable_id(replaceDots(row.getString("VARIABLE_ID")));
				variable.setDescription(row.getString("DESCRIPTION"));
				variable.setName(replaceDots(row.getString("VARIABLE_ID")));
				variable.setDisplayName(row.getString("NAME"));
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
	 * create the CUBE_STRUCTURE, CUBE, and CUBE_STRUCTURE_ITEM model instances from
	 * the BIRD Access Database
	 */
	public void createAllCubes() {
		AccessUtils accessUtils = AccessUtilProvider.getAccessUtils();
		try {
			List<AccessRow> list = accessUtils.getRowsForTable(filepath, "CUBE_STRUCTURE");

			for (AccessRow row : list) {

				CUBE_STRUCTURE cube_structure = Data_definitionFactory.eINSTANCE.createCUBE_STRUCTURE();
				cube_structure.setCode(row.getString("CODE"));
				cube_structure.setCube_structure_id(replaceDots(row.getString("CUBE_STRUCTURE_ID")));
				cube_structure.setName(replaceDots(row.getString("CUBE_STRUCTURE_ID")));
				cube_structure.setDisplayName(row.getString("NAME"));
				cube_structure.setDescription("DESCRIPTION");
				cube_structure.setValid_to(row.getDate("VALID_TO"));

				cubeStructuresModule.getCubeStructures().add(cube_structure);
			}
			list = accessUtils.getRowsForTable(filepath, "CUBE");
			for (AccessRow row : list) {

				CUBE cube = Data_definitionFactory.eINSTANCE.createCUBE();
				cube.setCode(row.getString("CODE"));
				cube.setCube_id(replaceDots(row.getString("CUBE_ID")));
				cube.setName(replaceDots(row.getString("CUBE_ID")));
				cube.setDisplayName(row.getString("NAME"));
				cube.setCube_type(row.getString("CUBE_TYPE"));
				cube.setDescription("DESCRIPTION");
				String cubeStructureString = row.getString("CUBE_STRUCTURE_ID");
				CUBE_STRUCTURE structure = findCubeStructureWithID(cubeStructureString);
				cube.setCube_structure_id(structure);

				cubesModule.getCubes().add(cube);
			}

			list = accessUtils.getRowsForTable(filepath, "CUBE_STRUCTURE_ITEM");
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
	private CUBE_STRUCTURE findCubeStructureWithID(String elementID) {
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
				comb.setCombination_id(replaceDots(row.getString("COMBINATION_ID")));
				comb.setName(replaceDots(row.getString("NAME")));

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
	private COMBINATION findCombinationWithID(String elementID) {
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
	 * Get the ColumnStructuredEntity for a String cube ID.
	 * 
	 * @param cube
	 * @return
	 */
	/**
	 * private ColumnStructuredEntity getCubeForCubeName(String cube) {
	 * 
	 * EList<ColumnStructuredEntity> column_structured_entities =
	 * iputDataStructuresProgram.getInput_structures() .getColumnStructures(); for
	 * (Iterator iterator = column_structured_entities.iterator();
	 * iterator.hasNext();) { ColumnStructuredEntity columnStructuredEntity =
	 * (ColumnStructuredEntity) iterator.next(); if
	 * (columnStructuredEntity.getName().equalsIgnoreCase(cube)) return
	 * columnStructuredEntity; } return null; }
	 */
	/**
	 * Get the related EnumMeber
	 * 
	 * @param cube
	 * @param variable
	 * @param value
	 * @return
	 */
	/**
	 * private EnumMember getMember(String cube, String variable, String value) {
	 * 
	 * EList<ColumnStructuredEntity> cubes = iputDataStructuresProgram.
	 * getInput_structures().getColumnStructures(); EnumMember returnedMember =
	 * null; for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
	 * ColumnStructuredEntity columnStructuredEntity = (ColumnStructuredEntity)
	 * iterator.next(); if (columnStructuredEntity.getName().equalsIgnoreCase(cube))
	 * { EList<Column> columns = columnStructuredEntity.getColumn(); for (Iterator
	 * iterator2 = columns.iterator(); iterator2.hasNext();) { Column column =
	 * (Column) iterator2.next(); if (column.getName().equalsIgnoreCase(variable)) {
	 * EList<EnumMember> theMembers = column.getDomain().getEnumMembers(); for
	 * (Iterator iterator3 = theMembers.iterator(); iterator3.hasNext();) {
	 * EnumMember enumMember = (EnumMember) iterator3.next();
	 * 
	 * if (enumMember.getCode().equalsIgnoreCase(value)) returnedMember =
	 * enumMember;
	 * 
	 * } } } }
	 * 
	 * } return returnedMember; }
	 */

	/**
	 * Return true if the variable for a cube is an enumberated varaible otherwise
	 * return false.
	 * 
	 * @param cube
	 * @param variable
	 * @return
	 */
	/**
	 * private boolean checkIsEnumeratedColumn(String cube, String variable) {
	 * 
	 * EList<ColumnStructuredEntity> cubes =
	 * iputDataStructuresProgram.getInput_structures().getColumnStructures();
	 * boolean isEnumerated = false; for (Iterator iterator = cubes.iterator();
	 * iterator.hasNext();) { ColumnStructuredEntity columnStructuredEntity =
	 * (ColumnStructuredEntity) iterator.next(); if
	 * (columnStructuredEntity.getName().equalsIgnoreCase(cube)) { EList<Column>
	 * columns = columnStructuredEntity.getColumn(); for (Iterator iterator2 =
	 * columns.iterator(); iterator2.hasNext();) { Column column = (Column)
	 * iterator2.next(); if (column.getName().equalsIgnoreCase(variable))
	 * isEnumerated = column.getDomain().isIsEnumerated(); } }
	 * 
	 * } return isEnumerated; }
	 */

	/**
	 * Get the Column instance for a particular cube, which is for a particular
	 * Varaible
	 * 
	 * @param cube
	 * @param variable
	 * @return
	 */
	/**
	 * private Column getColumnFromCube(String cube, String variable) {
	 * 
	 * EList<ColumnStructuredEntity> cubes =
	 * iputDataStructuresProgram.getInput_structures().getColumnStructures(); Column
	 * returnedColumn = null; for (Iterator iterator = cubes.iterator();
	 * iterator.hasNext();) { ColumnStructuredEntity columnStructuredEntity =
	 * (ColumnStructuredEntity) iterator.next(); if
	 * (columnStructuredEntity.getName().equalsIgnoreCase(cube)) { EList<Column>
	 * columns = columnStructuredEntity.getColumn(); for (Iterator iterator2 =
	 * columns.iterator(); iterator2.hasNext();) { Column column = (Column)
	 * iterator2.next(); if (column.getName().equalsIgnoreCase(variable))
	 * returnedColumn = column; } } }
	 * 
	 * return returnedColumn; }
	 */

	/**
	 * Return true if the cube exists, otherwise return false.
	 * 
	 * @param cube
	 * @return
	 */
	/**
	 * private boolean checkCubeExists(String cube) {
	 * 
	 * EList<ColumnStructuredEntity> cubes =
	 * iputDataStructuresProgram.getInput_structures().getColumnStructures();
	 * boolean exists = false; for (Iterator iterator = cubes.iterator();
	 * iterator.hasNext();) { ColumnStructuredEntity columnStructuredEntity =
	 * (ColumnStructuredEntity) iterator.next(); if
	 * (columnStructuredEntity.getName().equalsIgnoreCase(cube)) exists = true;
	 * 
	 * } return exists; }
	 */

	/**
	 * Adds a Column the the correct ColumnStructuredEntity in a
	 * ColumnStructureModule depending upon the string cube_structure_id matching
	 * the name of a ColumnStructuredEntity.
	 * 
	 * @param column
	 * @param columnStructureModule
	 * @param cube_structure_id
	 */
	/**
	 * private void addColumntoColumnStructuredEntity(Column column,
	 * ColumnStructureModule columnStructureModule, String cube_structure_id) {
	 * 
	 * EList<ColumnStructuredEntity> columnStructures =
	 * columnStructureModule.getColumnStructures(); for (Iterator iterator =
	 * columnStructures.iterator(); iterator.hasNext();) { ColumnStructuredEntity
	 * columnStructuredEntity = (ColumnStructuredEntity) iterator.next(); if
	 * (columnStructuredEntity.getName().equalsIgnoreCase(cube_structure_id))
	 * columnStructuredEntity.getColumn().add(column);
	 * 
	 * } }
	 */

	/**
	 * Sets the domain od a Column correctly depending upon the String domainID
	 * matching the name of a Domain in the domainModule.
	 * 
	 * @param column
	 * @param domainModule
	 * @param domainID
	 */
	/**
	 * private void setColumnDomain(Column column, ColumnDomainModule domainModule,
	 * String domainID) {
	 * 
	 * EList<ColumnDomain> columnDomains = domainModule.getColumnDomains(); for
	 * (Iterator iterator = columnDomains.iterator(); iterator.hasNext();) {
	 * ColumnDomain columnDomain = (ColumnDomain) iterator.next();
	 * 
	 * if (columnDomain.getName().equalsIgnoreCase(domainID))
	 * column.setDomain(columnDomain); } }
	 */

	/**
	 * Adds a member to the correct domain depending upon the String domainID
	 * matching the name of a Domain in the domainModule.
	 * 
	 * @param enumMember
	 * @param domainID
	 * @param domainModule
	 */
	/**
	 * private void addEnumMeberToCorrectColumnDomain(EnumMember enumMember, String
	 * domainID, ColumnDomainModule domainModule) {
	 * 
	 * EList<ColumnDomain> columnDomains = domainModule.getColumnDomains(); for
	 * (Iterator iterator = columnDomains.iterator(); iterator.hasNext();) {
	 * ColumnDomain columnDomain = (ColumnDomain) iterator.next();
	 * 
	 * if (columnDomain.getName().equalsIgnoreCase(domainID))
	 * columnDomain.getEnumMembers().add(enumMember);
	 * 
	 * } }
	 */

	
	/**
	 * from Test data sored in CSV format , create all the model instances of
	 * Test and Store them in the list of  testPrograms. Note that to do this
	 * we will create instances of TestDefintions TestConstraints, 
	 * and TestTemplates which the Test instance will refer to.
	 */
	@Override
	public void importTestDataWithOldTestFormat(String fileLocation) {
		
		prepareTestData();
		
		FileReader csvData;
		try {

			csvData = new FileReader(new File(fileLocation));

			
			
			List<CSVRecord> list = getCSVRowsFromFile(fileLocation);

			HashMap<String, SMCubesTest> tests = new HashMap<String, SMCubesTest>();
			HashMap<String, BaseColumnStructuredData> tables = new HashMap<String, BaseColumnStructuredData>();
			HashMap<String, BaseRowData> rows = new HashMap<String, BaseRowData>();
			int nameCounter = 0;
			for (CSVRecord csvRecord : list) {
				nameCounter++;
				//get a row of data which wil relate to the value of one column
				//in a cube
				String id1 = csvRecord.get(0).trim();
				String id2 = csvRecord.get(1).trim();
				String record_no = csvRecord.get(2);
				String cube = csvRecord.get(3);
				String variable = csvRecord.get(4);
				String value = csvRecord.get(5);
				//check whhich test this value is for, and see 
				//if we already have created a Test for that test
				SMCubesTest test = tests.get(id1 + ":" + id2);
				if (test == null) {
					//if we dont have aTest for this item of Test data, then 
					//we create one and add it to the list of tests
					SMCubesStaticModel testProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
					testPrograms.add(testProgram);
					SMCubesTestModule testModule = CocamoFactory.eINSTANCE.createSMCubesTestModule();
					testModule.setName("TestModule" + nameCounter);
					testProgram.setTests(testModule);
					test = CocamoFactory.eINSTANCE.createSMCubesTest();
					test.setName(id1 + ":" + id2);
					test.setTestDefinition((E2ETestDefinition) testDefinitionProgram.getTestDefinitions().getTestDefinitions().get(0)); 
					testModule.getTests().add(test);
					tests.put(id1 + ":" + id2, test);
					SMCubesTestInputData inputData2 = CocamoFactory.eINSTANCE
							.createSMCubesTestInputData();
					inputData2.setName(id1 + ":" + id2 + ":Data");
					test.setInputData(inputData2);
				}
				SMCubesTestInputData inputData = (SMCubesTestInputData) test.getInputData();
				

				// so we need a table for each table, then a row for each row, and a cell for
				// each cell.
				// so we make a hash map of sorts

				BaseColumnStructuredData table = tables.get(id1 + ":" + id2 + ":" + cube);
				if (table == null) {
					BaseColumnStructuredData structuredData = Base_column_structured_dataFactory.eINSTANCE
							.createBaseColumnStructuredData();
					structuredData.setName(id1 + ":" + id2 + ":" + cube +":CubeData" );
					structuredData.setCube(getCubeForCubeName(cube));
					tables.put(id1 + ":" + id2 + ":" + cube, structuredData);
					inputData.getSmcubes_inputdata().add(structuredData);
					table = structuredData;

				}

				BaseRowData rowData = rows.get(id1 + ":" + id2 + ":" + cube + ":" + record_no);
				if (rowData == null) {
					BaseRowData baseRow = Base_column_structured_dataFactory.eINSTANCE.createBaseRowData();
					baseRow.setRowID(id1 + ":" + id2 + ":" + cube + ":" + record_no);
					rows.put(id1 + ":" + id2 + ":" + cube + ":" + record_no, baseRow);
					table.getRows().add(baseRow);
					rowData = baseRow;
				}

				// determine if the associated Variable is enumerated or not.
				// if so then get the correct enumerated value, if it does not exist, then put
				// this in the error report.
				
				boolean cubeExists = checkCubeExists(cube);
				if (cubeExists) {
					VARIABLE theColumn = getColumnFromCube(cube, variable);

					if (theColumn != null) {
						boolean isEnumeratedColumn = checkIsEnumeratedColumn(cube, variable);
						if (isEnumeratedColumn) {
							MEMBER member = getMember(cube, variable, value);
							if (member != null) {

								BaseCellWithEnumeratedValue enumcell = Base_column_structured_dataFactory.eINSTANCE
										.createBaseCellWithEnumeratedValue();
								enumcell.setColumn(theColumn);
								enumcell.setValue(member);
								enumcell.setCellID(id1 + ":" + id2 + ":" + cube + ":"
										+ record_no + ":" +  variable);
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
							cellWithValue.setCellID(id1 + ":" + id2 + ":" + cube + ":"
									+ record_no + ":" +  variable);
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
	
	public void prepareTestData()
	{
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
		testTemplate.setName("testTemplate1");
		Param templateparam = Test_definitionFactory.eINSTANCE.createParam();
		templateparam.setParam(transformationSchemes.getSchemes().get(0)
				);
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
		
		contraints.setTemplate(testTemplate);
		// constraintsModule.getCoverageTestSets().add(contraints);
		testConstraintsProgram.setTestConstriants(contraints);
		
		
		Param contraintsparam = Test_definitionFactory.eINSTANCE.createParam();
		contraintsparam
				.setParam(transformationSchemes);
		contraints.getWhenParams().add(contraintsparam);

		E2ETestDefinition definition =
					Test_definitionFactory.eINSTANCE.createE2ETestDefinition();
		definition.setName("standard_test");
		definitionModule.getTestDefinitions().add(definition);

		Param defparam = Test_definitionFactory.eINSTANCE.createParam();
		defparam.setParam(
				transformationSchemes.getSchemes().get(1));
		
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
				
	}
	/**
	 * from Test data sored in CSV format , create all the model instances of
	 * Test and Store them in the list of  testPrograms. Note that to do this
	 * we will create instances of TestDefintions TestConstraints, 
	 * and TestTemplates which the Test instance will refer to.
	 */
	@Override
	public void importTestDataWithNewTestFormat() {
		
		prepareTestData();
		//for each file. run through it, and we should create the old style tests data, and then load it in.
		try {
			convertTestDataFromNewFormatToOldFormat();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		importTestDataWithOldTestFormat(outputFilepath + "/testsInOldFormat.csv");
		
		
	}
	
	private void convertTestDataFromNewFormatToOldFormat() throws IOException {
		
		//create initial file
		FileWriter testsInOldFormat = new FileWriter(outputFilepath + "/testsInOldFormat.csv",true);
		
		//for each import file, get its name and the columns, and export if there is a value.
		File dir = new File(testdatafilepath);
		  File[] directoryListing = dir.listFiles();
		  if (directoryListing != null) {
		    for (File child : directoryListing) {
		    	
		    	 StringBuffer sb = new StringBuffer();
				 CSVParser parser = CSVParser.parse(child,StandardCharsets.UTF_8, CSVFormat.EXCEL.withFirstRecordAsHeader());
				 boolean isHeader = true;
				 for (CSVRecord csvRecord : parser) {
					 if(!isHeader)
					 {
						 String testCaseID = csvRecord.get(0);
					 
						 String rowdID = csvRecord.get(1);
						 Map<String, String> map = csvRecord.toMap();
						 Set<Entry<String, String>> entrySet = map.entrySet();
						 
						 for (Iterator iterator = entrySet.iterator(); iterator.hasNext();) {
							Entry<String, String> entry = (Entry<String, String>) iterator.next();
							if(entry.getValue() != null && !entry.getValue().trim().equals("")
									&& !entry.getKey().equals("TestCaseID") && !entry.getKey().equals("RowdID"))
							{
								//System.out.println(testCaseID + "," + rowdID  +"," + child.getName().substring(0,child.getName().length()-4) + "," + entry.getKey() + "," + entry.getValue() +'\n');
								sb = sb.append( testCaseID.substring(0,testCaseID.indexOf(':')) + "," + testCaseID.substring(testCaseID.indexOf(':') +1,testCaseID.length()) + "," + rowdID  +"," + child.getName().substring(0,child.getName().length()-4) + "," + entry.getKey() + "," + entry.getValue() +'\n');

							}
						}
					 }
					 else
						 isHeader = false;
					
				 }
				 testsInOldFormat.append(sb).flush();

		    }
		  } else {
		    // log an error if it is not a directory.
		  }
	}

	/**
	 * Get the related EnumMeber
	 * 
	 * @param cube
	 * @param variable
	 * @param value
	 * @return
	 */
	private MEMBER getMember(String cube, String variable, String value) {
		
		EList<CUBE> cubes = cubesModule.getCubes();
			
		MEMBER returnedMember = null;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) 
		{
			CUBE columnStructuredEntity = 
					(CUBE) iterator.next();
			if (columnStructuredEntity.getCode().equalsIgnoreCase(cube)) 
			{
				
				
				EList<CUBE_STRUCTURE_ITEM> columns = getColumnsForCube(columnStructuredEntity);
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) 
				{
					CUBE_STRUCTURE_ITEM column = (CUBE_STRUCTURE_ITEM) iterator2.next();
					if (column.getCube_variable_code().equalsIgnoreCase(variable)) 
					{
						EList<MEMBER> theMembers = getDomainMembers(column.getVariable_id().getDomain_id());
						for (Iterator iterator3 = theMembers.iterator(); iterator3.hasNext();) 
						{
							MEMBER enumMember = (MEMBER) iterator3.next();

							if (enumMember.getCode().equalsIgnoreCase(value))
								returnedMember = enumMember;

						}
					}
				}
			}

		}
		return returnedMember;
	}

	
	private EList<MEMBER> getDomainMembers(DOMAIN domain) {

		EList<MEMBER> returnMembers = new BasicEList<MEMBER>();	
		EList<MEMBER> allMembers = members.getMembers();
		for (MEMBER member : allMembers) {
			if(member.getDomain_id().equals(domain))
			returnMembers.add(member);
		}
		return returnMembers;
	}

	private EList<CUBE_STRUCTURE_ITEM> getColumnsForCube(CUBE columnStructuredEntity) {
		// TODO Auto-generated method stub
		EList<CUBE_STRUCTURE_ITEM> returnItems = new BasicEList<CUBE_STRUCTURE_ITEM>();
		CUBE_STRUCTURE cs = columnStructuredEntity.getCube_structure_id();
		EList<CUBE_STRUCTURE_ITEM> csi = cubeStructureItemsModule.getCubeStructureItems();
		for (Iterator iterator = csi.iterator(); iterator.hasNext();) {
			CUBE_STRUCTURE_ITEM cube_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM) iterator.next();
			
			if(cube_STRUCTURE_ITEM.getCube_structure_id().equals(cs))
			{
				returnItems.add(cube_STRUCTURE_ITEM);
			}
		}
		return returnItems;
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
	
		EList<CUBE> cubes = cubesModule.getCubes();
		boolean isEnumerated = false;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
			CUBE columnStructuredEntity = (CUBE) iterator.next();
			if (columnStructuredEntity.getCode().equalsIgnoreCase(cube)) {
				
				 EList<CUBE_STRUCTURE_ITEM> columns = getColumnsForCube(columnStructuredEntity);
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
					CUBE_STRUCTURE_ITEM column = (CUBE_STRUCTURE_ITEM) iterator2.next();
					if (column.getCube_variable_code().equalsIgnoreCase(variable))
						isEnumerated = column.getVariable_id().getDomain_id().isIs_enumerated();
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
	private VARIABLE getColumnFromCube(String cube, String variable) {
		
		EList<CUBE> cubes = cubesModule.getCubes();
		VARIABLE returnedColumn = null;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
			CUBE columnStructuredEntity = (CUBE) iterator.next();
			if (columnStructuredEntity.getCode().equalsIgnoreCase(cube) ) {
				
				 EList<CUBE_STRUCTURE_ITEM> columns = getColumnsForCube(columnStructuredEntity);
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
					CUBE_STRUCTURE_ITEM column = (CUBE_STRUCTURE_ITEM) iterator2.next();
					if (column.getCube_variable_code().equalsIgnoreCase(variable))
						returnedColumn = column.getVariable_id();
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
		
		EList<CUBE> cubes = cubesModule.getCubes();
		boolean exists = false;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
			CUBE columnStructuredEntity = (CUBE) iterator.next();
			if (columnStructuredEntity.getCode().equalsIgnoreCase(cube))
				exists = true;

		}
		return exists;
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
	
	/**
	 * Get the ColumnStructuredEntity for a String cube ID.
	 * @param cube
	 * @return
	 */
	private CUBE getCubeForCubeName(String cube) {
		
		EList<CUBE> column_structured_entities =
				cubesModule.getCubes();
		for (Iterator iterator = 
				column_structured_entities.iterator(); iterator.hasNext();) {
			CUBE columnStructuredEntity =
					(CUBE) iterator.next();
			if (columnStructuredEntity.getCode().equalsIgnoreCase(cube))
				return columnStructuredEntity;
		}
		return null;
	}
	
	/**
	 * A main method for initial test.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		BIRDImporterImpl importer = new BIRDImporterImpl();

		String filepath = args[0];
		String outputFilepath = args[1];
		String testdatafilepath = args[2];
		importer.doImport(filepath, outputFilepath, testdatafilepath);

	}

}
