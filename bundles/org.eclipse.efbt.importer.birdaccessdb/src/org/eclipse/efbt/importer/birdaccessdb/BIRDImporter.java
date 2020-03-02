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

package org.eclipse.efbt.importer.birdaccessdb;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Member;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import org.eclipse.efbt.importer.Importer;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import core.CoreFactory;
import core.DOMAIN;
import core.FACET_VALUE_TYPE;
import core.MAINTENANCE_AGENCY;
import core.MEMBER;
import core.VARIABLE;
import data_definition.COMBINATION;
import data_definition.COMBINATION_ITEM;
import data_definition.CUBE;
import data_definition.CUBE_STRUCTURE;
import data_definition.CUBE_STRUCTURE_ITEM;
import data_definition.Data_definitionFactory;
import efbt_data_definition.CombinationModule;
import efbt_data_definition.CubeModule;
import efbt_data_definition.DomainModule;
import efbt_data_definition.Efbt_data_definitionFactory;
import efbt_data_definition.MemberModule;
import efbt_data_definition.VariableModule;
import efbt_vtl_transformation.*;
import mapping.CUBE_MAPPING;
import mapping.MAPPING_DEFINITION;
import mapping.MAPPING_TO_CUBE;
import mapping.MEMBER_MAPPING;
import mapping.MappingFactory;
import mapping.VARIABLE_MAPPING;
import vtl_transformation.TRANSFORMATION;
import vtl_transformation.TRANSFORMATION_NODE;
import vtl_transformation.TRANSFORMATION_SCHEME;
import vtl_transformation.Vtl_transformationFactory;

/**
 * @author Neil Mackenzie
 *
 */
public class BIRDImporter extends Importer {
	
	/**
	 * The file path of the access database
	 */
	private static String filepath = "C:\\Users\\neil\\BIRD_release_5.0\\BIRD_release_5.0.accdb";
	
	public static String getFilepath() {
		return filepath;
	}

	public static void setFilepath(String filepath) {
		BIRDImporter.filepath = filepath;
	}

	public void createAllTransformationSchemes() {
		Table table;
		try {
			table = DatabaseBuilder.open(new File(filepath)).getTable("TRANSFORMATION_SCHEME");
			for (Row row : table) {
				System.out
						.println("Column 'TRANSFORMATION_SCHEME_ID' has value: " + row.get("TRANSFORMATION_SCHEME_ID"));
				TRANSFORMATION_SCHEME scheme = Vtl_transformationFactory.eINSTANCE.createTRANSFORMATION_SCHEME();
				scheme.setCode(row.getString("CODE"));
				scheme.setTransformation_scheme_id(row.getString("TRANSFORMATION_SCHEME_ID"));
				scheme.setDescription(row.getString("DESCRIPTION"));
				scheme.setMaintenance_agency_id(row.getString("MAINTENANCE_AGENCY_ID"));
				scheme.setName(row.getString("NAME"));
				scheme.setType(row.getString("TYPE"));
				transformationSchemes.getSchemes().add(scheme);
				System.out.println("scheme = " + scheme.toString());

			}

			table = DatabaseBuilder.open(new File(filepath)).getTable("TRANSFORMATION");
			for (Row row : table) {
				System.out.println("Column 'TRANSFORMATION_ID' has value: " + row.getString("TRANSFORMATION_ID"));
				TRANSFORMATION transformation = Vtl_transformationFactory.eINSTANCE.createTRANSFORMATION();
				TRANSFORMATION_SCHEME scheme = getTransformationScheme(row.getString("TRANSFORMATION_SCHEME_ID"));

				transformation.setCode(row.getString("CODE"));
				transformation.setTransformation_id(row.getString("TRANSFORMATION_ID"));
				transformation.setDescription(row.getString("DESCRIPTION"));
				transformation.setMaintenance_agency_id(row.getString("MAINTENANCE_AGENCY_ID"));
				transformation.setName(row.getString("NAME"));
				transformation.setExpression(row.getString("EXPRESSION"));
				scheme.getTranformations().add(transformation);
				System.out.println("transformation = " + transformation.toString());

			}

			table = DatabaseBuilder.open(new File(filepath)).getTable("TRANSFORMATION_NODE");
			for (Row row : table) {
				System.out.println(
						"Column 'TRANSFORMATION_NODE_ID' has value: " + row.getString("TRANSFORMATION_NODE_ID"));
				TRANSFORMATION_NODE node = Vtl_transformationFactory.eINSTANCE.createTRANSFORMATION_NODE();
				EList<TRANSFORMATION_NODE> nodelist = new BasicEList<TRANSFORMATION_NODE>();

				node.setLevel((row.getInt("LEVEL")));
				node.setOrder((row.getInt("ORDER")));
				node.setTransformation_node_id((row.getString("TRANSFORMATION_NODE_ID")));
				node.setParent_node_id(row.getString("PARENT_NODE_ID"));
				String tableID = row.getString("TABLE_ID");
				String elementID = row.getString("ELEMENT_ID");
				MEMBER member = null;
				VARIABLE variable = null;
				if (tableID != null) {
					if (tableID.equals("VARIABLE")) {
						variable = findVariableWithID(elementID);
						node.setVariable(variable);
					}
					if (tableID.equals("MEMBER")) {
						member = findMemberWithID(elementID);
						node.setMember(member);
					}
				}

				node.setExpresssion(row.getString("EXPRESSION"));
				nodelist.add(node);
				System.out.println("node = " + node.toString());

			}

			// get all nodes in a list, add all level 1 nodes, add them in order.

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

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

	private static TRANSFORMATION_SCHEME getTransformationScheme(String scheme_id) {
		// TODO Auto-generated method stub
		EList<TRANSFORMATION_SCHEME> schemes = transformationSchemes.getSchemes();
		TRANSFORMATION_SCHEME returnScheme = null;
		for (Iterator iterator = schemes.iterator(); iterator.hasNext();) {
			TRANSFORMATION_SCHEME transformation_SCHEME = (TRANSFORMATION_SCHEME) iterator.next();
			if (scheme_id.equals(transformation_SCHEME.getTransformation_scheme_id()))
				returnScheme = transformation_SCHEME;
		}
		return returnScheme;

	}

	public void createAllMappings() {
		
		//creat4e all mapping defintions
		//attach all memebr mapping and variable maapings
		//create cubemapping module and link to created mapping defintions
		Table table;
		try {
		

		table = DatabaseBuilder.open(new File(filepath)).getTable("MEMBER_MAPPING");
		for (Row row : table) {
			System.out.println("Column2 'MEMBER_MAPPING_ID' has value: " + row.get("MEMBER_MAPPING_ID"));
			MEMBER_MAPPING memberMapping = MappingFactory.eINSTANCE.createMEMBER_MAPPING();			
			
			memberMapping.setMember_mapping_id(row.getString("MEMBER_MAPPING_ID"));
			memberMapping.setCode(row.getString("CODE"));
			memberMapping.setName(row.getString("NAME"));
			memberMapping.setMaintenance_agency_id(row.getString("MAINTENENCE_AGENCY_ID"));
			memberMappingModule.getMemberMappings().add(memberMapping);
	
					

			//combinationItemsModule.getCombination_items().add(item);
		}
		table = DatabaseBuilder.open(new File(filepath)).getTable("VARIABLE_MAPPING");
		for (Row row : table) {
			System.out.println("Column2 'VARIABLE_MAPPING_ID' has value: " + row.get("VARIABLE_MAPPING_ID"));
			VARIABLE_MAPPING variableMapping = MappingFactory.eINSTANCE.createVARIABLE_MAPPING();			
			
			variableMapping.setVariable_mapping_id(row.getString("VARIABLE_MAPPING_ID"));
			variableMapping.setCode(row.getString("CODE"));
			variableMapping.setName(row.getString("NAME"));
			variableMapping.setMaintenance_agency_id(row.getString("MAINTENENCE_AGENCY_ID"));
			variableMappingModule.getVariableMappings().add(variableMapping);
	
					

			//combinationItemsModule.getCombination_items().add(item);
		}
		
		table = DatabaseBuilder.open(new File(filepath)).getTable("MAPPING_DEFINITION");

		for (Row row : table) {

			System.out.println("Column 'MAPPING_DEFINITION_ID' has value: " + row.get("MAPPING_DEFINITION_ID"));
			MAPPING_DEFINITION mapping = MappingFactory.eINSTANCE.createMAPPING_DEFINITION();
			mapping.setCode(row.getString("CODE"));
			mapping.setMapping_id(row.getString("MAPPING_ID"));
			mapping.setName(row.getString("NAME"));
			mapping.setMapping_type((row.getString("MAPPING_TYPE")));
			String memberMappingString = (row.getString("MEMBER_MAPPING_ID"));
			
			if(memberMappingString != null)
			{
				mapping.setMemberMapping(findMemberMappingWithID(memberMappingString));
			}
				
			String variableMappingString = (row.getString("VARIABLE_MAPPING_ID"));
			if(variableMappingString != null)
			{
				mapping.setVariableMapping(findVariableMappingWithID(variableMappingString));
			}
			
			System.out.println("mapping = " + mapping.toString());
			mappingDefinitionModule.getMappings().add(mapping);
		}
		
		//create the cubeMappings add them to the module
		//create the mapping to cube, link them by reference to the mapping defintions
		//then add them by containment to the cubeMapping
		table = DatabaseBuilder.open(new File(filepath)).getTable("CUBE_MAPPING");
		for (Row row : table) {

			System.out.println("Column 'CUBE_MAPPING_ID' has value: " + row.get("CUBE_MAPPING_ID"));
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
			
			System.out.println("mapping2 = " + mapping.toString());
		}
		
		table = DatabaseBuilder.open(new File(filepath)).getTable("MAPPING_TO_CUBE");
		for (Row row : table) {

			System.out.println("Column2 'MAPPING_TO_CUBE' has value: " + row.get("CUBE_MAPPING_ID"));
			MAPPING_TO_CUBE mapping = MappingFactory.eINSTANCE.createMAPPING_TO_CUBE();
			
			
			
			String cubeMappingString = (row.getString("CUBE_MAPPING_ID"));
			String mappingString = (row.getString("MAPPING_ID"));
			mapping.setMapping(findMappingDefWithID(mappingString));
			
			CUBE_MAPPING cube_mapping = findCubeMappingWithID(cubeMappingString);
			cube_mapping.getMappingsToCube().add(mapping);
			
			
			System.out.println("mapping2 = " + mapping.toString());
		}
		
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	}

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

	private MEMBER_MAPPING findMemberMappingWithID(String elementID) {
		EList<MEMBER_MAPPING> memberMappingList = memberMappingModule.getMemberMappings();
		MEMBER_MAPPING returnMemberMapping = null;
		for (Iterator iterator = memberMappingList.iterator(); iterator.hasNext();) {
			MEMBER_MAPPING memMapping = (MEMBER_MAPPING) iterator.next();
			if (elementID.equals(memMapping.getMember_mapping_id()))
				returnMemberMapping	 = memMapping;
				}
		return returnMemberMapping;
	}

	public void createAllDomains() {
		Table table;
		try {
			table = DatabaseBuilder.open(new File(filepath)).getTable("DOMAIN");
			for (Row row : table) {
				System.out.println("Column 'DOMAIN_ID' has value: " + row.get("DOMAIN_ID"));
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

				// String agencyString = row.getString("MAINTENANCE_AGENCY_ID");
				// MAINTENANCE_AGENCY agency = CoreFactory.eINSTANCE.createMAINTENANCE_AGENCY();
				// domain.setMaintenance_agency_id(row.getString("MAINTENANCE_AGENCY_ID"));
				domain.setName(row.getString("NAME"));
				boolean is_enumerated = false;
				if (row.getBoolean("IS_ENUMERATED"))
					is_enumerated = true;
				domain.setIs_enumerated(is_enumerated);

				domains.getDomains().add(domain);
				System.out.println("domain = " + domain.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void createAllMembers() {
		Table table;
		try {
			table = DatabaseBuilder.open(new File(filepath)).getTable("MEMBER");
			for (Row row : table) {
				System.out.println("Column 'MEMBER_ID' has value: " + row.get("MEMBER_ID"));
				MEMBER member = CoreFactory.eINSTANCE.createMEMBER();
				member.setCode(row.getString("CODE"));
				member.setMember_id(row.getString("MEMBER_ID"));
				member.setDescription(row.getString("DESCRIPTION"));
				member.setName(row.getString("NAME"));
				String domainIDString = (row.getString("DOMAIN_ID"));
				DOMAIN domain = getDomainWithID(domainIDString);
				member.setDomain_id(domain);

				members.getMembers().add(member);
				System.out.println("member = " + member.toString());
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

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

	public void createAllVariables() {
		Table table;
		try {
			table = DatabaseBuilder.open(new File(filepath)).getTable("VARIABLE");
			for (Row row : table) {
				System.out.println("Column 'VARIABLE_ID' has value: " + row.get("VARIABLE_ID"));
				VARIABLE variable = CoreFactory.eINSTANCE.createVARIABLE();
				variable.setCode(row.getString("CODE"));
				variable.setVariable_id(row.getString("VARIABLE_ID"));
				variable.setDescription(row.getString("DESCRIPTION"));
				variable.setName(row.getString("NAME"));
				String domainIDString = (row.getString("DOMAIN_ID"));
				DOMAIN domain = getDomainWithID(domainIDString);
				variable.setDomain_id(domain);

				variables.getVariables().add(variable);
				System.out.println("variable = " + variable.toString());
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	public void createAllCubes() {
		Table table;
		try {

			table = DatabaseBuilder.open(new File(filepath)).getTable("CUBE_STRUCTURE");
			for (Row row : table) {
				System.out.println("Column 'CUBE_STRUCTURE_ID' has value: " + row.get("CUBE_STRUCTURE_ID"));
				CUBE_STRUCTURE cube_structure = Data_definitionFactory.eINSTANCE.createCUBE_STRUCTURE();
				cube_structure.setCode(row.getString("CODE"));
				cube_structure.setCube_structure_id(row.getString("CUBE_STRUCTURE_ID"));
				cube_structure.setName(row.getString("NAME"));
				cube_structure.setDescription("DESCRIPTION");
				System.out.println("cube_structure = " + cube_structure.toString());
				cubeStructuresModule.getCubeStructures().add(cube_structure);
			}

			table = DatabaseBuilder.open(new File(filepath)).getTable("CUBE");
			for (Row row : table) {
				System.out.println("Column 'CUBE_ID' has value: " + row.get("CUBE_ID"));
				CUBE cube = Data_definitionFactory.eINSTANCE.createCUBE();
				cube.setCode(row.getString("CODE"));
				cube.setCube_id(row.getString("CUBE_ID"));
				cube.setName(row.getString("NAME"));
				cube.setCube_type(row.getString("CUBE_TYPE"));
				cube.setDescription("DESCRIPTION");
				String cubeStructureString = row.getString("CUBE_STRUCTURE_ID");
				CUBE_STRUCTURE structure = findCubeStructureWithID(cubeStructureString);
				cube.setCube_structure_id(structure);

				System.out.println("cube = " + cube.toString());
				cubesModule.getCubes().add(cube);
			}

			table = DatabaseBuilder.open(new File(filepath)).getTable("CUBE_STRUCTURE_ITEM");
			for (Row row : table) {
				System.out.println("Column2 'CUBE_STRUCTURE_ID' has value: " + row.get("CUBE_STRUCTURE_ID"));
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
				item.setDescription(row.getString("DESCRIPTION"));
				try {
					int order = row.getInt("ORDER");
					item.setOrder(order);
				} catch (NullPointerException e) {
					System.out.println("null order");
				}

				System.out.println("item = " + item.toString());

				cubeStructureItemsModule.getCubeStructureItems().add(item);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

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

	public void createAllCombinations() {
		Table table;
		int counter = 0;
		CombinationModule combinationsModule = null;
		try {
			table = DatabaseBuilder.open(new File(filepath)).getTable("COMBINATION");

			for (Row row : table) {

				if (counter == 0) {
					combinationsModule = Efbt_data_definitionFactory.eINSTANCE.createCombinationModule();
					combinationsModules.add(combinationsModule);
					birdModel.getCombinations().add(combinationsModule);
				}

				counter++;
				if (counter == 100)
					counter = 0;

				System.out.println("Column 'COMBINATION_ID' has value: " + row.get("COMBINATION_ID"));
				COMBINATION comb = Data_definitionFactory.eINSTANCE.createCOMBINATION();
				comb.setCode(row.getString("CODE"));
				comb.setCombination_id(row.getString("COMBINATION_ID"));
				comb.setName(row.getString("NAME"));
				System.out.println("comb = " + comb.toString());
				combinationsModule.getCombinations().add(comb);
			}

			table = DatabaseBuilder.open(new File(filepath)).getTable("COMBINATION_ITEM");
			for (Row row : table) {
				System.out.println("Column2 'COMBINATION_ID' has value: " + row.get("COMBINATION_ID"));
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
				//item.setCombination_id(com);
				com.getCombination_items().add(item);

				//combinationItemsModule.getCombination_items().add(item);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

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

	public static void main(String[] args) {
		
		

		BIRDImporter importer = new BIRDImporter();
		importer.filepath = args[0];
		//importer.filepath = "C:\\freebirdtools-master2\\ws\\org.eclipse.efbt.regmodules.bird\\bird\\";
		importer.doImport();
	}
}
