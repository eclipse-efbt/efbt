/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * <!-- begin-model-doc -->
 * This package describes the mapping package from SMCubes, using fields from SDD, see ECB documentation for details
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the data definition package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html'"
 * @generated
 */
public interface MappingPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "mapping";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/mapping";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "mapping";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	MappingPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl <em>CUBE MAPPING</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getCUBE_MAPPING()
	 * @generated
	 */
	int CUBE_MAPPING = 0;

	/**
	 * The feature id for the '<em><b>Cube mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__CUBE_MAPPING_ID = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Source cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__SOURCE_CUBE = 2;

	/**
	 * The feature id for the '<em><b>Target cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__TARGET_CUBE = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__NAME = 4;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__CODE = 5;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__MAINTENANCE_AGENCY_ID = 6;

	/**
	 * The feature id for the '<em><b>Mappings To Cube</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING__MAPPINGS_TO_CUBE = 7;

	/**
	 * The number of structural features of the '<em>CUBE MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>CUBE MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPINGImpl <em>MEMBER MAPPING</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPINGImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMEMBER_MAPPING()
	 * @generated
	 */
	int MEMBER_MAPPING = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING__NAME = 0;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING__CODE = 1;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING__MAINTENANCE_AGENCY_ID = 2;

	/**
	 * The feature id for the '<em><b>Member mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING__MEMBER_MAPPING_ID = 3;

	/**
	 * The feature id for the '<em><b>Member Mapping Items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING__MEMBER_MAPPING_ITEMS = 4;

	/**
	 * The number of structural features of the '<em>MEMBER MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>MEMBER MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPING_ITEMImpl <em>MEMBER MAPPING ITEM</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPING_ITEMImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMEMBER_MAPPING_ITEM()
	 * @generated
	 */
	int MEMBER_MAPPING_ITEM = 2;

	/**
	 * The feature id for the '<em><b>Is Source</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_ITEM__IS_SOURCE = 0;

	/**
	 * The feature id for the '<em><b>Member</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_ITEM__MEMBER = 1;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_ITEM__VARIABLE = 2;

	/**
	 * The number of structural features of the '<em>MEMBER MAPPING ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_ITEM_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>MEMBER MAPPING ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_ITEM_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPING_ITEMImpl <em>VARIABLE MAPPING ITEM</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPING_ITEMImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getVARIABLE_MAPPING_ITEM()
	 * @generated
	 */
	int VARIABLE_MAPPING_ITEM = 3;

	/**
	 * The feature id for the '<em><b>Is Source</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_ITEM__IS_SOURCE = 0;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_ITEM__VARIABLE = 1;

	/**
	 * The number of structural features of the '<em>VARIABLE MAPPING ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_ITEM_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>VARIABLE MAPPING ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_ITEM_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPINGImpl <em>VARIABLE MAPPING</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPINGImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getVARIABLE_MAPPING()
	 * @generated
	 */
	int VARIABLE_MAPPING = 4;

	/**
	 * The feature id for the '<em><b>Varaible Mapping Items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING__NAME = 1;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING__CODE = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Variable mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING__VARIABLE_MAPPING_ID = 4;

	/**
	 * The number of structural features of the '<em>VARIABLE MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>VARIABLE MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_TO_CUBEImpl <em>MAPPING TO CUBE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_TO_CUBEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMAPPING_TO_CUBE()
	 * @generated
	 */
	int MAPPING_TO_CUBE = 5;

	/**
	 * The feature id for the '<em><b>Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_TO_CUBE__MAPPING = 0;

	/**
	 * The number of structural features of the '<em>MAPPING TO CUBE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_TO_CUBE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>MAPPING TO CUBE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_TO_CUBE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_SET_MAPPINGImpl <em>VARIABLE SET MAPPING</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_SET_MAPPINGImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getVARIABLE_SET_MAPPING()
	 * @generated
	 */
	int VARIABLE_SET_MAPPING = 6;

	/**
	 * The feature id for the '<em><b>Source Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_MAPPING__SOURCE_MAPPING = 0;

	/**
	 * The feature id for the '<em><b>Target Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_MAPPING__TARGET_MAPPING = 1;

	/**
	 * The number of structural features of the '<em>VARIABLE SET MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_MAPPING_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>VARIABLE SET MAPPING</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_MAPPING_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl <em>MAPPING DEFINITION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMAPPING_DEFINITION()
	 * @generated
	 */
	int MAPPING_DEFINITION = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION__NAME = 0;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION__CODE = 1;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID = 2;

	/**
	 * The feature id for the '<em><b>Mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION__MAPPING_ID = 3;

	/**
	 * The feature id for the '<em><b>Mapping type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION__MAPPING_TYPE = 4;

	/**
	 * The feature id for the '<em><b>Member Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION__MEMBER_MAPPING = 5;

	/**
	 * The feature id for the '<em><b>Variable Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION__VARIABLE_MAPPING = 6;

	/**
	 * The number of structural features of the '<em>MAPPING DEFINITION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>MAPPING DEFINITION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING <em>CUBE MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE MAPPING</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING
	 * @generated
	 */
	EClass getCUBE_MAPPING();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCube_mapping_id <em>Cube mapping id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube mapping id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCube_mapping_id()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EAttribute getCUBE_MAPPING_Cube_mapping_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getDescription()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EAttribute getCUBE_MAPPING_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getSource_cube <em>Source cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Source cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getSource_cube()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EReference getCUBE_MAPPING_Source_cube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getTarget_cube <em>Target cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Target cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getTarget_cube()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EReference getCUBE_MAPPING_Target_cube();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getName()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EAttribute getCUBE_MAPPING_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getCode()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EAttribute getCUBE_MAPPING_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getMaintenance_agency_id()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EAttribute getCUBE_MAPPING_Maintenance_agency_id();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getMappingsToCube <em>Mappings To Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Mappings To Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING#getMappingsToCube()
	 * @see #getCUBE_MAPPING()
	 * @generated
	 */
	EReference getCUBE_MAPPING_MappingsToCube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING <em>MEMBER MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER MAPPING</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING
	 * @generated
	 */
	EClass getMEMBER_MAPPING();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getName()
	 * @see #getMEMBER_MAPPING()
	 * @generated
	 */
	EAttribute getMEMBER_MAPPING_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getCode()
	 * @see #getMEMBER_MAPPING()
	 * @generated
	 */
	EAttribute getMEMBER_MAPPING_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMaintenance_agency_id()
	 * @see #getMEMBER_MAPPING()
	 * @generated
	 */
	EAttribute getMEMBER_MAPPING_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMember_mapping_id <em>Member mapping id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Member mapping id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMember_mapping_id()
	 * @see #getMEMBER_MAPPING()
	 * @generated
	 */
	EAttribute getMEMBER_MAPPING_Member_mapping_id();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMemberMappingItems <em>Member Mapping Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Mapping Items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING#getMemberMappingItems()
	 * @see #getMEMBER_MAPPING()
	 * @generated
	 */
	EReference getMEMBER_MAPPING_MemberMappingItems();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM <em>MEMBER MAPPING ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER MAPPING ITEM</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM
	 * @generated
	 */
	EClass getMEMBER_MAPPING_ITEM();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM#getIsSource <em>Is Source</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Source</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM#getIsSource()
	 * @see #getMEMBER_MAPPING_ITEM()
	 * @generated
	 */
	EAttribute getMEMBER_MAPPING_ITEM_IsSource();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM#getMember <em>Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM#getMember()
	 * @see #getMEMBER_MAPPING_ITEM()
	 * @generated
	 */
	EReference getMEMBER_MAPPING_ITEM_Member();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM#getVariable()
	 * @see #getMEMBER_MAPPING_ITEM()
	 * @generated
	 */
	EReference getMEMBER_MAPPING_ITEM_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM <em>VARIABLE MAPPING ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE MAPPING ITEM</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM
	 * @generated
	 */
	EClass getVARIABLE_MAPPING_ITEM();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM#getIsSource <em>Is Source</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Source</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM#getIsSource()
	 * @see #getVARIABLE_MAPPING_ITEM()
	 * @generated
	 */
	EAttribute getVARIABLE_MAPPING_ITEM_IsSource();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM#getVariable()
	 * @see #getVARIABLE_MAPPING_ITEM()
	 * @generated
	 */
	EReference getVARIABLE_MAPPING_ITEM_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING <em>VARIABLE MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE MAPPING</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING
	 * @generated
	 */
	EClass getVARIABLE_MAPPING();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getVaraibleMappingItems <em>Varaible Mapping Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Varaible Mapping Items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getVaraibleMappingItems()
	 * @see #getVARIABLE_MAPPING()
	 * @generated
	 */
	EReference getVARIABLE_MAPPING_VaraibleMappingItems();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getName()
	 * @see #getVARIABLE_MAPPING()
	 * @generated
	 */
	EAttribute getVARIABLE_MAPPING_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getCode()
	 * @see #getVARIABLE_MAPPING()
	 * @generated
	 */
	EAttribute getVARIABLE_MAPPING_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getMaintenance_agency_id()
	 * @see #getVARIABLE_MAPPING()
	 * @generated
	 */
	EAttribute getVARIABLE_MAPPING_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getVariable_mapping_id <em>Variable mapping id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Variable mapping id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING#getVariable_mapping_id()
	 * @see #getVARIABLE_MAPPING()
	 * @generated
	 */
	EAttribute getVARIABLE_MAPPING_Variable_mapping_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE <em>MAPPING TO CUBE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MAPPING TO CUBE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE
	 * @generated
	 */
	EClass getMAPPING_TO_CUBE();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE#getMapping <em>Mapping</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Mapping</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE#getMapping()
	 * @see #getMAPPING_TO_CUBE()
	 * @generated
	 */
	EReference getMAPPING_TO_CUBE_Mapping();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING <em>VARIABLE SET MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE SET MAPPING</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING
	 * @generated
	 */
	EClass getVARIABLE_SET_MAPPING();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING#getSourceMapping <em>Source Mapping</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Source Mapping</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING#getSourceMapping()
	 * @see #getVARIABLE_SET_MAPPING()
	 * @generated
	 */
	EReference getVARIABLE_SET_MAPPING_SourceMapping();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING#getTargetMapping <em>Target Mapping</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Target Mapping</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING#getTargetMapping()
	 * @see #getVARIABLE_SET_MAPPING()
	 * @generated
	 */
	EReference getVARIABLE_SET_MAPPING_TargetMapping();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION <em>MAPPING DEFINITION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MAPPING DEFINITION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION
	 * @generated
	 */
	EClass getMAPPING_DEFINITION();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getName()
	 * @see #getMAPPING_DEFINITION()
	 * @generated
	 */
	EAttribute getMAPPING_DEFINITION_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getCode()
	 * @see #getMAPPING_DEFINITION()
	 * @generated
	 */
	EAttribute getMAPPING_DEFINITION_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMaintenance_agency_id()
	 * @see #getMAPPING_DEFINITION()
	 * @generated
	 */
	EAttribute getMAPPING_DEFINITION_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_id <em>Mapping id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Mapping id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_id()
	 * @see #getMAPPING_DEFINITION()
	 * @generated
	 */
	EAttribute getMAPPING_DEFINITION_Mapping_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_type <em>Mapping type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Mapping type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_type()
	 * @see #getMAPPING_DEFINITION()
	 * @generated
	 */
	EAttribute getMAPPING_DEFINITION_Mapping_type();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMemberMapping <em>Member Mapping</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member Mapping</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMemberMapping()
	 * @see #getMAPPING_DEFINITION()
	 * @generated
	 */
	EReference getMAPPING_DEFINITION_MemberMapping();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getVariableMapping <em>Variable Mapping</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable Mapping</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getVariableMapping()
	 * @see #getMAPPING_DEFINITION()
	 * @generated
	 */
	EReference getMAPPING_DEFINITION_VariableMapping();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	MappingFactory getMappingFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals {
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl <em>CUBE MAPPING</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getCUBE_MAPPING()
		 * @generated
		 */
		EClass CUBE_MAPPING = eINSTANCE.getCUBE_MAPPING();

		/**
		 * The meta object literal for the '<em><b>Cube mapping id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_MAPPING__CUBE_MAPPING_ID = eINSTANCE.getCUBE_MAPPING_Cube_mapping_id();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_MAPPING__DESCRIPTION = eINSTANCE.getCUBE_MAPPING_Description();

		/**
		 * The meta object literal for the '<em><b>Source cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_MAPPING__SOURCE_CUBE = eINSTANCE.getCUBE_MAPPING_Source_cube();

		/**
		 * The meta object literal for the '<em><b>Target cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_MAPPING__TARGET_CUBE = eINSTANCE.getCUBE_MAPPING_Target_cube();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_MAPPING__NAME = eINSTANCE.getCUBE_MAPPING_Name();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_MAPPING__CODE = eINSTANCE.getCUBE_MAPPING_Code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_MAPPING__MAINTENANCE_AGENCY_ID = eINSTANCE.getCUBE_MAPPING_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Mappings To Cube</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_MAPPING__MAPPINGS_TO_CUBE = eINSTANCE.getCUBE_MAPPING_MappingsToCube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPINGImpl <em>MEMBER MAPPING</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPINGImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMEMBER_MAPPING()
		 * @generated
		 */
		EClass MEMBER_MAPPING = eINSTANCE.getMEMBER_MAPPING();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_MAPPING__NAME = eINSTANCE.getMEMBER_MAPPING_Name();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_MAPPING__CODE = eINSTANCE.getMEMBER_MAPPING_Code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_MAPPING__MAINTENANCE_AGENCY_ID = eINSTANCE.getMEMBER_MAPPING_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Member mapping id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_MAPPING__MEMBER_MAPPING_ID = eINSTANCE.getMEMBER_MAPPING_Member_mapping_id();

		/**
		 * The meta object literal for the '<em><b>Member Mapping Items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_MAPPING__MEMBER_MAPPING_ITEMS = eINSTANCE.getMEMBER_MAPPING_MemberMappingItems();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPING_ITEMImpl <em>MEMBER MAPPING ITEM</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPING_ITEMImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMEMBER_MAPPING_ITEM()
		 * @generated
		 */
		EClass MEMBER_MAPPING_ITEM = eINSTANCE.getMEMBER_MAPPING_ITEM();

		/**
		 * The meta object literal for the '<em><b>Is Source</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_MAPPING_ITEM__IS_SOURCE = eINSTANCE.getMEMBER_MAPPING_ITEM_IsSource();

		/**
		 * The meta object literal for the '<em><b>Member</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_MAPPING_ITEM__MEMBER = eINSTANCE.getMEMBER_MAPPING_ITEM_Member();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_MAPPING_ITEM__VARIABLE = eINSTANCE.getMEMBER_MAPPING_ITEM_Variable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPING_ITEMImpl <em>VARIABLE MAPPING ITEM</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPING_ITEMImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getVARIABLE_MAPPING_ITEM()
		 * @generated
		 */
		EClass VARIABLE_MAPPING_ITEM = eINSTANCE.getVARIABLE_MAPPING_ITEM();

		/**
		 * The meta object literal for the '<em><b>Is Source</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_MAPPING_ITEM__IS_SOURCE = eINSTANCE.getVARIABLE_MAPPING_ITEM_IsSource();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_MAPPING_ITEM__VARIABLE = eINSTANCE.getVARIABLE_MAPPING_ITEM_Variable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPINGImpl <em>VARIABLE MAPPING</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPINGImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getVARIABLE_MAPPING()
		 * @generated
		 */
		EClass VARIABLE_MAPPING = eINSTANCE.getVARIABLE_MAPPING();

		/**
		 * The meta object literal for the '<em><b>Varaible Mapping Items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS = eINSTANCE.getVARIABLE_MAPPING_VaraibleMappingItems();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_MAPPING__NAME = eINSTANCE.getVARIABLE_MAPPING_Name();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_MAPPING__CODE = eINSTANCE.getVARIABLE_MAPPING_Code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID = eINSTANCE.getVARIABLE_MAPPING_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Variable mapping id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_MAPPING__VARIABLE_MAPPING_ID = eINSTANCE.getVARIABLE_MAPPING_Variable_mapping_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_TO_CUBEImpl <em>MAPPING TO CUBE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_TO_CUBEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMAPPING_TO_CUBE()
		 * @generated
		 */
		EClass MAPPING_TO_CUBE = eINSTANCE.getMAPPING_TO_CUBE();

		/**
		 * The meta object literal for the '<em><b>Mapping</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAPPING_TO_CUBE__MAPPING = eINSTANCE.getMAPPING_TO_CUBE_Mapping();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_SET_MAPPINGImpl <em>VARIABLE SET MAPPING</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_SET_MAPPINGImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getVARIABLE_SET_MAPPING()
		 * @generated
		 */
		EClass VARIABLE_SET_MAPPING = eINSTANCE.getVARIABLE_SET_MAPPING();

		/**
		 * The meta object literal for the '<em><b>Source Mapping</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_MAPPING__SOURCE_MAPPING = eINSTANCE.getVARIABLE_SET_MAPPING_SourceMapping();

		/**
		 * The meta object literal for the '<em><b>Target Mapping</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_MAPPING__TARGET_MAPPING = eINSTANCE.getVARIABLE_SET_MAPPING_TargetMapping();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl <em>MAPPING DEFINITION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl#getMAPPING_DEFINITION()
		 * @generated
		 */
		EClass MAPPING_DEFINITION = eINSTANCE.getMAPPING_DEFINITION();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAPPING_DEFINITION__NAME = eINSTANCE.getMAPPING_DEFINITION_Name();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAPPING_DEFINITION__CODE = eINSTANCE.getMAPPING_DEFINITION_Code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID = eINSTANCE.getMAPPING_DEFINITION_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Mapping id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAPPING_DEFINITION__MAPPING_ID = eINSTANCE.getMAPPING_DEFINITION_Mapping_id();

		/**
		 * The meta object literal for the '<em><b>Mapping type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAPPING_DEFINITION__MAPPING_TYPE = eINSTANCE.getMAPPING_DEFINITION_Mapping_type();

		/**
		 * The meta object literal for the '<em><b>Member Mapping</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAPPING_DEFINITION__MEMBER_MAPPING = eINSTANCE.getMAPPING_DEFINITION_MemberMapping();

		/**
		 * The meta object literal for the '<em><b>Variable Mapping</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAPPING_DEFINITION__VARIABLE_MAPPING = eINSTANCE.getMAPPING_DEFINITION_VariableMapping();

	}

} //MappingPackage
