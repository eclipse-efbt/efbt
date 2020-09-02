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

package org.eclipse.efbt.data_structures.smcubes.component.importexport.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.data_structures.smcubes.model.core.CoreFactory;
import org.eclipse.efbt.data_structures.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.data_structures.smcubes.model.core.FACET_VALUE_TYPE;
import org.eclipse.efbt.data_structures.smcubes.model.core.MEMBER;
import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.data_structures.smcubes.model.data_definition.COMBINATION;
import org.eclipse.efbt.data_structures.smcubes.model.data_definition.COMBINATION_ITEM;
import org.eclipse.efbt.data_structures.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.data_structures.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.data_structures.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.data_structures.smcubes.model.data_definition.Data_definitionFactory;
import org.eclipse.efbt.data_structures.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.data_structures.smcubes.model.efbt_data_definition.Efbt_data_definitionFactory;
import org.eclipse.efbt.data_structures.smcubes.model.mapping.CUBE_MAPPING;
import org.eclipse.efbt.data_structures.smcubes.model.mapping.MAPPING_DEFINITION;
import org.eclipse.efbt.data_structures.smcubes.model.mapping.MAPPING_TO_CUBE;
import org.eclipse.efbt.data_structures.smcubes.model.mapping.MEMBER_MAPPING;
import org.eclipse.efbt.data_structures.smcubes.model.mapping.MappingFactory;
import org.eclipse.efbt.data_structures.smcubes.model.mapping.VARIABLE_MAPPING;
import org.eclipse.efbt.data_structures.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME;
import org.eclipse.efbt.data_structures.smcubes.model.vtl_transformation.Vtl_transformationFactory;
import org.eclipse.efbt.util.access.provider.AccessUtilProvider;
import org.eclipse.efbt.util.accessdb.AccessRow;
import org.eclipse.efbt.util.accessdb.AccessUtils;
import org.eclipse.emf.common.util.EList;



/**
 * This class is respnsable for structure data taking data from the BIRD
 * Access Database and also data in CSV defining Test data, and creates 
 * instances of the Chirp Program model.
 *  
 * @author Neil Mackenzie
 *
 */
public class BIRDImporterImpl extends Importer {

	

   public BIRDImporterImpl()
   {
	   super();
   }

   public void doImport(String filepath6, String outputFilepath26, String testdatafilepath6)
	{
	
   super.doImport(filepath6, outputFilepath26, testdatafilepath6);
	}
   
   
   public void sayHello2()
	{
	
 System.out.println("hello2");
	}
	/**
	 * Returns the file path of the access database
	 * @return
	 */
	public  String getFilepath() {
		return filepath;
	}
	/**
	 * Sets the file path of the access database
	 * 
	 */
	public  void setFilepath(String filepath) {
		this.filepath = filepath;
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
	private  MEMBER findMemberWithID(String elementID) {
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
	private  VARIABLE findVariableWithID(String elementID) {
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
	private  TRANSFORMATION_SCHEME getTransformationScheme(String scheme_id) {
		
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

				domain.setName(row.getString("DOMAIN_ID"));
				domain.setDisplayName(row.getString("NAME"));
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
				member.setName(row.getString("MEMBER_ID"));
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
	 * get the DOMAIN instance which corresponds to the String ID of the 
	 * DOMAIN
	 * 
	 * @param domainIDString
	 * @return
	 */
	private  DOMAIN getDomainWithID(String domainIDString) {

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
				variable.setName(row.getString("VARIABLE_ID"));
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
				cube_structure.setName(row.getString("CUBE_STRUCTURE_ID"));
				cube_structure.setDisplayName(row.getString("NAME"));
				cube_structure.setDescription("DESCRIPTION");
				cube_structure.setValid_to(row.getDate("VALID_TO"));
				
				cubeStructuresModule.getCubeStructures().add(cube_structure);
			}
			list = accessUtils.getRowsForTable(filepath, "CUBE");
			for (AccessRow row : list) {
				
				CUBE cube = Data_definitionFactory.eINSTANCE.createCUBE();
				cube.setCode(row.getString("CODE"));
				cube.setCube_id(row.getString("CUBE_ID"));
				cube.setName(row.getString("CUBE_ID"));
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
	private  CUBE_STRUCTURE findCubeStructureWithID(String elementID) {
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
	private  COMBINATION findCombinationWithID(String elementID) {
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
	 * @param cube
	 * @return
	 */
	/**private ColumnStructuredEntity getCubeForCubeName(String cube) {
		
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
	}*/
	/**
	 * Get the related EnumMeber
	 * 
	 * @param cube
	 * @param variable
	 * @param value
	 * @return
	 */
	/**private EnumMember getMember(String cube, String variable, String value) {
		
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
	}*/

	/**
	 * Return true if the variable for a cube is an enumberated varaible
	 * otherwise return false.
	 * 
	 * @param cube
	 * @param variable
	 * @return
	 */
	/**private boolean checkIsEnumeratedColumn(String cube, String variable) {
	
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
	}*/

	/**
	 * Get the Column instance for a particular cube, 
	 * which is for a particular Varaible
	 * @param cube
	 * @param variable
	 * @return
	 */
	/**private Column getColumnFromCube(String cube, String variable) {
		
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
	}*/

	/**
	 * Return true if the cube exists, otherwise return false.
	 * 
	 * @param cube
	 * @return
	 */
/**	private boolean checkCubeExists(String cube) {
		
		EList<ColumnStructuredEntity> cubes = iputDataStructuresProgram.getInput_structures().getColumnStructures();
		boolean exists = false;
		for (Iterator iterator = cubes.iterator(); iterator.hasNext();) {
			ColumnStructuredEntity columnStructuredEntity = (ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube))
				exists = true;

		}
		return exists;
	}*/

	

	/**
	 * Adds a Column the the correct ColumnStructuredEntity in a 
	 * ColumnStructureModule depending upon the string cube_structure_id 
	 * matching the name of a  ColumnStructuredEntity.
	 * 
	 * @param column
	 * @param columnStructureModule
	 * @param cube_structure_id
	 */
	/**private void addColumntoColumnStructuredEntity(Column column, ColumnStructureModule columnStructureModule,
			String cube_structure_id) {

		EList<ColumnStructuredEntity> columnStructures = columnStructureModule.getColumnStructures();
		for (Iterator iterator = columnStructures.iterator(); iterator.hasNext();) {
			ColumnStructuredEntity columnStructuredEntity = (ColumnStructuredEntity) iterator.next();
			if (columnStructuredEntity.getName().equalsIgnoreCase(cube_structure_id))
				columnStructuredEntity.getColumn().add(column);

		}
	}*/

	/**
	 * Sets the domain od a Column correctly depending upon the String domainID matching
	 * the name of a Domain in the domainModule.
	 * 
	 * @param column
	 * @param domainModule
	 * @param domainID
	 */
	/**private void setColumnDomain(Column column, ColumnDomainModule domainModule, String domainID) {
		
		EList<ColumnDomain> columnDomains = domainModule.getColumnDomains();
		for (Iterator iterator = columnDomains.iterator(); iterator.hasNext();) {
			ColumnDomain columnDomain = (ColumnDomain) iterator.next();

			if (columnDomain.getName().equalsIgnoreCase(domainID))
				column.setDomain(columnDomain);
		}
	}*/

	/**
	 * Adds a member to the correct domain depending upon the String domainID matching
	 * the name of a Domain in the domainModule.
	 * @param enumMember
	 * @param domainID
	 * @param domainModule
	 */
	/**private void addEnumMeberToCorrectColumnDomain(EnumMember enumMember, String domainID,
			ColumnDomainModule domainModule) {

		EList<ColumnDomain> columnDomains = domainModule.getColumnDomains();
		for (Iterator iterator = columnDomains.iterator(); iterator.hasNext();) {
			ColumnDomain columnDomain = (ColumnDomain) iterator.next();

			if (columnDomain.getName().equalsIgnoreCase(domainID))
				columnDomain.getEnumMembers().add(enumMember);

		}
	}*/
	
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
		importer.doImport(filepath,outputFilepath,testdatafilepath);
		
	}

}
