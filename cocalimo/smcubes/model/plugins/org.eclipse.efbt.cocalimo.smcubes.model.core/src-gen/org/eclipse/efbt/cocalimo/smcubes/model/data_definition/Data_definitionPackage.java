/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
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
 * This package describes the data definition package from SMCubes, using fields from SDD, see ECB documentation for details
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the data definition package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html'"
 * @generated
 */
public interface Data_definitionPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "data_definition";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/data_definition";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "data_definition";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_definitionPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATIONImpl <em>COMBINATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCOMBINATION()
	 * @generated
	 */
	int COMBINATION = 0;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__CODE = 0;

	/**
	 * The feature id for the '<em><b>Combination id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__COMBINATION_ID = 1;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__MAINTENANCE_AGENCY_ID = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__NAME = 3;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__VALID_FROM = 4;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__VALID_TO = 5;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__VERSION = 6;

	/**
	 * The feature id for the '<em><b>Combination items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION__COMBINATION_ITEMS = 7;

	/**
	 * The number of structural features of the '<em>COMBINATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>COMBINATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl <em>COMBINATION ITEM</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCOMBINATION_ITEM()
	 * @generated
	 */
	int COMBINATION_ITEM = 1;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_ITEM__MEMBER_ID = 0;

	/**
	 * The feature id for the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_ITEM__SUBDOMAIN_ID = 1;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_ITEM__VARIABLE_ID = 2;

	/**
	 * The feature id for the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_ITEM__VARIABLE_SET_ID = 3;

	/**
	 * The number of structural features of the '<em>COMBINATION ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_ITEM_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>COMBINATION ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_ITEM_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl <em>CUBE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE()
	 * @generated
	 */
	int CUBE = 2;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__CODE = 0;

	/**
	 * The feature id for the '<em><b>Cube id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__CUBE_ID = 1;

	/**
	 * The feature id for the '<em><b>Cube structure id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__CUBE_STRUCTURE_ID = 2;

	/**
	 * The feature id for the '<em><b>Cube type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__CUBE_TYPE = 3;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__DESCRIPTION = 4;

	/**
	 * The feature id for the '<em><b>Framework id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__FRAMEWORK_ID = 5;

	/**
	 * The feature id for the '<em><b>Is allowed</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__IS_ALLOWED = 6;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__MAINTENANCE_AGENCY_ID = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__NAME = 8;

	/**
	 * The feature id for the '<em><b>Published</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__PUBLISHED = 9;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__VALID_FROM = 10;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__VALID_TO = 11;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__VERSION = 12;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE__DISPLAY_NAME = 13;

	/**
	 * The number of structural features of the '<em>CUBE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_FEATURE_COUNT = 14;

	/**
	 * The number of operations of the '<em>CUBE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl <em>CUBE GROUP</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_GROUP()
	 * @generated
	 */
	int CUBE_GROUP = 3;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP__CODE = 0;

	/**
	 * The feature id for the '<em><b>Cube group id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP__CUBE_GROUP_ID = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP__NAME = 4;

	/**
	 * The number of structural features of the '<em>CUBE GROUP</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>CUBE GROUP</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl <em>CUBE GROUP ENUMERATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_GROUP_ENUMERATION()
	 * @generated
	 */
	int CUBE_GROUP_ENUMERATION = 4;

	/**
	 * The feature id for the '<em><b>Cube group id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID = 0;

	/**
	 * The feature id for the '<em><b>Cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_ENUMERATION__CUBE_ID = 1;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_ENUMERATION__ORDER = 2;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_ENUMERATION__VALID_FROM = 3;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_ENUMERATION__VALID_TO = 4;

	/**
	 * The number of structural features of the '<em>CUBE GROUP ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_ENUMERATION_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>CUBE GROUP ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_GROUP_ENUMERATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHYImpl <em>CUBE HIERARCHY</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHYImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_HIERARCHY()
	 * @generated
	 */
	int CUBE_HIERARCHY = 5;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY__CODE = 0;

	/**
	 * The feature id for the '<em><b>Cube hierarchy id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY__CUBE_HIERARCHY_ID = 1;

	/**
	 * The feature id for the '<em><b>Cube hierarchy type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE = 2;

	/**
	 * The feature id for the '<em><b>Framework id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY__FRAMEWORK_ID = 3;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY__NAME = 5;

	/**
	 * The number of structural features of the '<em>CUBE HIERARCHY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>CUBE HIERARCHY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl <em>CUBE HIERARCHY NODE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	int CUBE_HIERARCHY_NODE = 6;

	/**
	 * The feature id for the '<em><b>Cube group id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__CUBE_GROUP_ID = 0;

	/**
	 * The feature id for the '<em><b>Cube hierarchy id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID = 1;

	/**
	 * The feature id for the '<em><b>Level</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__LEVEL = 2;

	/**
	 * The feature id for the '<em><b>NODE CODE</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__NODE_CODE = 3;

	/**
	 * The feature id for the '<em><b>Node name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__NODE_NAME = 4;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__ORDER = 5;

	/**
	 * The feature id for the '<em><b>Parent node code</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__PARENT_NODE_CODE = 6;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__VALID_FROM = 7;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE__VALID_TO = 8;

	/**
	 * The number of structural features of the '<em>CUBE HIERARCHY NODE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE_FEATURE_COUNT = 9;

	/**
	 * The number of operations of the '<em>CUBE HIERARCHY NODE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_HIERARCHY_NODE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl <em>CUBE RELATIONSHIP</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_RELATIONSHIP()
	 * @generated
	 */
	int CUBE_RELATIONSHIP = 7;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__CODE = 0;

	/**
	 * The feature id for the '<em><b>Cube relationship id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Establishes integrity</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY = 3;

	/**
	 * The feature id for the '<em><b>Foreign cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__FOREIGN_CUBE_ID = 4;

	/**
	 * The feature id for the '<em><b>Foreign cube variable code</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE = 5;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__NAME = 7;

	/**
	 * The feature id for the '<em><b>Primary cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__PRIMARY_CUBE_ID = 8;

	/**
	 * The feature id for the '<em><b>Primary cube variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID = 9;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__VALID_FROM = 10;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__VALID_TO = 11;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP__VERSION = 12;

	/**
	 * The number of structural features of the '<em>CUBE RELATIONSHIP</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP_FEATURE_COUNT = 13;

	/**
	 * The number of operations of the '<em>CUBE RELATIONSHIP</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_RELATIONSHIP_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTUREImpl <em>CUBE STRUCTURE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTUREImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_STRUCTURE()
	 * @generated
	 */
	int CUBE_STRUCTURE = 8;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__CODE = 0;

	/**
	 * The feature id for the '<em><b>Cube structure id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__CUBE_STRUCTURE_ID = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__NAME = 4;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__VALID_FROM = 5;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__VALID_TO = 6;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__VERSION = 7;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE__DISPLAY_NAME = 8;

	/**
	 * The number of structural features of the '<em>CUBE STRUCTURE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_FEATURE_COUNT = 9;

	/**
	 * The number of operations of the '<em>CUBE STRUCTURE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl <em>CUBE STRUCTURE ITEM</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	int CUBE_STRUCTURE_ITEM = 9;

	/**
	 * The feature id for the '<em><b>Attribute associated variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE = 0;

	/**
	 * The feature id for the '<em><b>Cube structure id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Dimension type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__DIMENSION_TYPE = 3;

	/**
	 * The feature id for the '<em><b>Is flow</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__IS_FLOW = 4;

	/**
	 * The feature id for the '<em><b>Is mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__IS_MANDATORY = 5;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__MEMBER_ID = 6;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__ORDER = 7;

	/**
	 * The feature id for the '<em><b>Role</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__ROLE = 8;

	/**
	 * The feature id for the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID = 9;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__VARIABLE_ID = 10;

	/**
	 * The feature id for the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID = 11;

	/**
	 * The feature id for the '<em><b>Is Identifier</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__IS_IDENTIFIER = 12;

	/**
	 * The feature id for the '<em><b>Cube variable code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE = 13;

	/**
	 * The number of structural features of the '<em>CUBE STRUCTURE ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM_FEATURE_COUNT = 14;

	/**
	 * The number of operations of the '<em>CUBE STRUCTURE ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_STRUCTURE_ITEM_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_TO_COMBINATIONImpl <em>CUBE TO COMBINATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_TO_COMBINATIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_TO_COMBINATION()
	 * @generated
	 */
	int CUBE_TO_COMBINATION = 10;

	/**
	 * The feature id for the '<em><b>Combination id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TO_COMBINATION__COMBINATION_ID = 0;

	/**
	 * The feature id for the '<em><b>Cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TO_COMBINATION__CUBE_ID = 1;

	/**
	 * The number of structural features of the '<em>CUBE TO COMBINATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TO_COMBINATION_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>CUBE TO COMBINATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TO_COMBINATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORKImpl <em>FRAMEWORK</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORKImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getFRAMEWORK()
	 * @generated
	 */
	int FRAMEWORK = 11;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Framework id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK__FRAMEWORK_ID = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK__NAME = 4;

	/**
	 * The number of structural features of the '<em>FRAMEWORK</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>FRAMEWORK</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_SUBDOMAINImpl <em>FRAMEWORK SUBDOMAIN</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_SUBDOMAINImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getFRAMEWORK_SUBDOMAIN()
	 * @generated
	 */
	int FRAMEWORK_SUBDOMAIN = 12;

	/**
	 * The feature id for the '<em><b>Framework id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID = 0;

	/**
	 * The feature id for the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID = 1;

	/**
	 * The number of structural features of the '<em>FRAMEWORK SUBDOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_SUBDOMAIN_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>FRAMEWORK SUBDOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_SUBDOMAIN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_VARIABLE_SETImpl <em>FRAMEWORK VARIABLE SET</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_VARIABLE_SETImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getFRAMEWORK_VARIABLE_SET()
	 * @generated
	 */
	int FRAMEWORK_VARIABLE_SET = 13;

	/**
	 * The feature id for the '<em><b>Framework id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID = 0;

	/**
	 * The feature id for the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID = 1;

	/**
	 * The number of structural features of the '<em>FRAMEWORK VARIABLE SET</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_VARIABLE_SET_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>FRAMEWORK VARIABLE SET</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FRAMEWORK_VARIABLE_SET_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN <em>TYP DMNSN</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_DMNSN()
	 * @generated
	 */
	int TYP_DMNSN = 14;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL <em>TYP RL</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_RL()
	 * @generated
	 */
	int TYP_RL = 15;

	/**
	 * The meta object id for the '<em>TYP DMNSN Object</em>' data type.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_DMNSNObject()
	 * @generated
	 */
	int TYP_DMNSN_OBJECT = 16;

	/**
	 * The meta object id for the '<em>TYP RL Object</em>' data type.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_RLObject()
	 * @generated
	 */
	int TYP_RL_OBJECT = 17;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION <em>COMBINATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>COMBINATION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION
	 * @generated
	 */
	EClass getCOMBINATION();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getCode()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EAttribute getCOMBINATION_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getCombination_id <em>Combination id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Combination id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getCombination_id()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EAttribute getCOMBINATION_Combination_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getMaintenance_agency_id()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EReference getCOMBINATION_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getName()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EAttribute getCOMBINATION_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getValid_from()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EAttribute getCOMBINATION_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getValid_to()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EAttribute getCOMBINATION_Valid_to();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getVersion()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EAttribute getCOMBINATION_Version();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getCombination_items <em>Combination items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Combination items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION#getCombination_items()
	 * @see #getCOMBINATION()
	 * @generated
	 */
	EReference getCOMBINATION_Combination_items();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM <em>COMBINATION ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>COMBINATION ITEM</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM
	 * @generated
	 */
	EClass getCOMBINATION_ITEM();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getMember_id()
	 * @see #getCOMBINATION_ITEM()
	 * @generated
	 */
	EReference getCOMBINATION_ITEM_Member_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getSubdomain_id <em>Subdomain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Subdomain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getSubdomain_id()
	 * @see #getCOMBINATION_ITEM()
	 * @generated
	 */
	EReference getCOMBINATION_ITEM_Subdomain_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getVariable_id <em>Variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getVariable_id()
	 * @see #getCOMBINATION_ITEM()
	 * @generated
	 */
	EReference getCOMBINATION_ITEM_Variable_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getVariable_set_id <em>Variable set id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable set id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM#getVariable_set_id()
	 * @see #getCOMBINATION_ITEM()
	 * @generated
	 */
	EReference getCOMBINATION_ITEM_Variable_set_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE <em>CUBE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE
	 * @generated
	 */
	EClass getCUBE();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCode()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCube_id <em>Cube id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCube_id()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Cube_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCube_structure_id <em>Cube structure id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube structure id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCube_structure_id()
	 * @see #getCUBE()
	 * @generated
	 */
	EReference getCUBE_Cube_structure_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCube_type <em>Cube type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getCube_type()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Cube_type();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getDescription()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getFramework_id <em>Framework id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Framework id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getFramework_id()
	 * @see #getCUBE()
	 * @generated
	 */
	EReference getCUBE_Framework_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#isIs_allowed <em>Is allowed</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is allowed</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#isIs_allowed()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Is_allowed();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getMaintenance_agency_id()
	 * @see #getCUBE()
	 * @generated
	 */
	EReference getCUBE_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getName()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#isPublished <em>Published</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Published</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#isPublished()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Published();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getValid_from()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getValid_to()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Valid_to();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getVersion()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_Version();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE#getDisplayName()
	 * @see #getCUBE()
	 * @generated
	 */
	EAttribute getCUBE_DisplayName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP <em>CUBE GROUP</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE GROUP</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP
	 * @generated
	 */
	EClass getCUBE_GROUP();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getCode()
	 * @see #getCUBE_GROUP()
	 * @generated
	 */
	EAttribute getCUBE_GROUP_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getCube_group_id <em>Cube group id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube group id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getCube_group_id()
	 * @see #getCUBE_GROUP()
	 * @generated
	 */
	EAttribute getCUBE_GROUP_Cube_group_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getDescription()
	 * @see #getCUBE_GROUP()
	 * @generated
	 */
	EAttribute getCUBE_GROUP_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getMaintenance_agency_id()
	 * @see #getCUBE_GROUP()
	 * @generated
	 */
	EReference getCUBE_GROUP_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP#getName()
	 * @see #getCUBE_GROUP()
	 * @generated
	 */
	EAttribute getCUBE_GROUP_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION <em>CUBE GROUP ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE GROUP ENUMERATION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION
	 * @generated
	 */
	EClass getCUBE_GROUP_ENUMERATION();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_group_id <em>Cube group id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube group id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_group_id()
	 * @see #getCUBE_GROUP_ENUMERATION()
	 * @generated
	 */
	EReference getCUBE_GROUP_ENUMERATION_Cube_group_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_id <em>Cube id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_id()
	 * @see #getCUBE_GROUP_ENUMERATION()
	 * @generated
	 */
	EReference getCUBE_GROUP_ENUMERATION_Cube_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getOrder()
	 * @see #getCUBE_GROUP_ENUMERATION()
	 * @generated
	 */
	EAttribute getCUBE_GROUP_ENUMERATION_Order();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_from()
	 * @see #getCUBE_GROUP_ENUMERATION()
	 * @generated
	 */
	EAttribute getCUBE_GROUP_ENUMERATION_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_to()
	 * @see #getCUBE_GROUP_ENUMERATION()
	 * @generated
	 */
	EAttribute getCUBE_GROUP_ENUMERATION_Valid_to();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY <em>CUBE HIERARCHY</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE HIERARCHY</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY
	 * @generated
	 */
	EClass getCUBE_HIERARCHY();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getCode()
	 * @see #getCUBE_HIERARCHY()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_id <em>Cube hierarchy id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube hierarchy id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_id()
	 * @see #getCUBE_HIERARCHY()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_Cube_hierarchy_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_type <em>Cube hierarchy type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube hierarchy type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getCube_hierarchy_type()
	 * @see #getCUBE_HIERARCHY()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_Cube_hierarchy_type();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getFramework_id <em>Framework id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Framework id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getFramework_id()
	 * @see #getCUBE_HIERARCHY()
	 * @generated
	 */
	EReference getCUBE_HIERARCHY_Framework_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getMaintenance_agency_id()
	 * @see #getCUBE_HIERARCHY()
	 * @generated
	 */
	EReference getCUBE_HIERARCHY_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY#getName()
	 * @see #getCUBE_HIERARCHY()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE <em>CUBE HIERARCHY NODE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE HIERARCHY NODE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE
	 * @generated
	 */
	EClass getCUBE_HIERARCHY_NODE();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_group_id <em>Cube group id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube group id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_group_id()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getCUBE_HIERARCHY_NODE_Cube_group_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_hierarchy_id <em>Cube hierarchy id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube hierarchy id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_hierarchy_id()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getCUBE_HIERARCHY_NODE_Cube_hierarchy_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getLevel <em>Level</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Level</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getLevel()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_NODE_Level();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNODE_CODE <em>NODE CODE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>NODE CODE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNODE_CODE()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_NODE_NODE_CODE();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNode_name <em>Node name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Node name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNode_name()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_NODE_Node_name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getOrder()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_NODE_Order();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getParent_node_code <em>Parent node code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Parent node code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getParent_node_code()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getCUBE_HIERARCHY_NODE_Parent_node_code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_from()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_NODE_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_to()
	 * @see #getCUBE_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getCUBE_HIERARCHY_NODE_Valid_to();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP <em>CUBE RELATIONSHIP</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE RELATIONSHIP</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP
	 * @generated
	 */
	EClass getCUBE_RELATIONSHIP();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCode()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCube_relationship_id <em>Cube relationship id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube relationship id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getCube_relationship_id()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Cube_relationship_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getDescription()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#isEstablishes_integrity <em>Establishes integrity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Establishes integrity</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#isEstablishes_integrity()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Establishes_integrity();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_id <em>Foreign cube id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Foreign cube id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_id()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EReference getCUBE_RELATIONSHIP_Foreign_cube_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_variable_code <em>Foreign cube variable code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Foreign cube variable code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getForeign_cube_variable_code()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EReference getCUBE_RELATIONSHIP_Foreign_cube_variable_code();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getMaintenance_agency_id()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EReference getCUBE_RELATIONSHIP_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getName()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_id <em>Primary cube id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Primary cube id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_id()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EReference getCUBE_RELATIONSHIP_Primary_cube_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_variable_id <em>Primary cube variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Primary cube variable id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getPrimary_cube_variable_id()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EReference getCUBE_RELATIONSHIP_Primary_cube_variable_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_from()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getValid_to()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Valid_to();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP#getVersion()
	 * @see #getCUBE_RELATIONSHIP()
	 * @generated
	 */
	EAttribute getCUBE_RELATIONSHIP_Version();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE <em>CUBE STRUCTURE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE STRUCTURE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE
	 * @generated
	 */
	EClass getCUBE_STRUCTURE();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCode()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCube_structure_id <em>Cube structure id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube structure id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getCube_structure_id()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_Cube_structure_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDescription()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getMaintenance_agency_id()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EReference getCUBE_STRUCTURE_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getName()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_from()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getValid_to()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_Valid_to();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getVersion()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_Version();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE#getDisplayName()
	 * @see #getCUBE_STRUCTURE()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_DisplayName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM <em>CUBE STRUCTURE ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE STRUCTURE ITEM</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM
	 * @generated
	 */
	EClass getCUBE_STRUCTURE_ITEM();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getAttribute_associated_variable <em>Attribute associated variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute associated variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getAttribute_associated_variable()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EReference getCUBE_STRUCTURE_ITEM_Attribute_associated_variable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_structure_id <em>Cube structure id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube structure id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_structure_id()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EReference getCUBE_STRUCTURE_ITEM_Cube_structure_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDescription()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDimension_type <em>Dimension type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Dimension type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDimension_type()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_Dimension_type();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_flow <em>Is flow</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is flow</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_flow()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_Is_flow();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_mandatory <em>Is mandatory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is mandatory</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_mandatory()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_Is_mandatory();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getMember_id()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EReference getCUBE_STRUCTURE_ITEM_Member_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getOrder()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_Order();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getRole <em>Role</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Role</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getRole()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_Role();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getSubdomain_id <em>Subdomain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Subdomain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getSubdomain_id()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EReference getCUBE_STRUCTURE_ITEM_Subdomain_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_id <em>Variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_id()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EReference getCUBE_STRUCTURE_ITEM_Variable_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_set_id <em>Variable set id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable set id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_set_id()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EReference getCUBE_STRUCTURE_ITEM_Variable_set_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIsIdentifier <em>Is Identifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Identifier</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIsIdentifier()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_IsIdentifier();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_variable_code <em>Cube variable code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube variable code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_variable_code()
	 * @see #getCUBE_STRUCTURE_ITEM()
	 * @generated
	 */
	EAttribute getCUBE_STRUCTURE_ITEM_Cube_variable_code();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION <em>CUBE TO COMBINATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CUBE TO COMBINATION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION
	 * @generated
	 */
	EClass getCUBE_TO_COMBINATION();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCombination_id <em>Combination id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Combination id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCombination_id()
	 * @see #getCUBE_TO_COMBINATION()
	 * @generated
	 */
	EReference getCUBE_TO_COMBINATION_Combination_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCube_id <em>Cube id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCube_id()
	 * @see #getCUBE_TO_COMBINATION()
	 * @generated
	 */
	EReference getCUBE_TO_COMBINATION_Cube_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK <em>FRAMEWORK</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>FRAMEWORK</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK
	 * @generated
	 */
	EClass getFRAMEWORK();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getCode()
	 * @see #getFRAMEWORK()
	 * @generated
	 */
	EAttribute getFRAMEWORK_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getDescription()
	 * @see #getFRAMEWORK()
	 * @generated
	 */
	EAttribute getFRAMEWORK_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getFramework_id <em>Framework id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Framework id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getFramework_id()
	 * @see #getFRAMEWORK()
	 * @generated
	 */
	EAttribute getFRAMEWORK_Framework_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getMaintenance_agency_id()
	 * @see #getFRAMEWORK()
	 * @generated
	 */
	EReference getFRAMEWORK_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK#getName()
	 * @see #getFRAMEWORK()
	 * @generated
	 */
	EAttribute getFRAMEWORK_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN <em>FRAMEWORK SUBDOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>FRAMEWORK SUBDOMAIN</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN
	 * @generated
	 */
	EClass getFRAMEWORK_SUBDOMAIN();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getFramework_id <em>Framework id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Framework id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getFramework_id()
	 * @see #getFRAMEWORK_SUBDOMAIN()
	 * @generated
	 */
	EReference getFRAMEWORK_SUBDOMAIN_Framework_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getSubdomain_id <em>Subdomain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Subdomain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN#getSubdomain_id()
	 * @see #getFRAMEWORK_SUBDOMAIN()
	 * @generated
	 */
	EReference getFRAMEWORK_SUBDOMAIN_Subdomain_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET <em>FRAMEWORK VARIABLE SET</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>FRAMEWORK VARIABLE SET</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET
	 * @generated
	 */
	EClass getFRAMEWORK_VARIABLE_SET();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getFramework_id <em>Framework id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Framework id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getFramework_id()
	 * @see #getFRAMEWORK_VARIABLE_SET()
	 * @generated
	 */
	EReference getFRAMEWORK_VARIABLE_SET_Framework_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getVariable_set_id <em>Variable set id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable set id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET#getVariable_set_id()
	 * @see #getFRAMEWORK_VARIABLE_SET()
	 * @generated
	 */
	EReference getFRAMEWORK_VARIABLE_SET_Variable_set_id();

	/**
	 * Returns the meta object for enum '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN <em>TYP DMNSN</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>TYP DMNSN</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
	 * @generated
	 */
	EEnum getTYP_DMNSN();

	/**
	 * Returns the meta object for enum '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL <em>TYP RL</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>TYP RL</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
	 * @generated
	 */
	EEnum getTYP_RL();

	/**
	 * Returns the meta object for data type '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN <em>TYP DMNSN Object</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for data type '<em>TYP DMNSN Object</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
	 * @model instanceClass="org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN"
	 *        extendedMetaData="name='TYP_DMNSN:Object' baseType='TYP_DMNSN'"
	 * @generated
	 */
	EDataType getTYP_DMNSNObject();

	/**
	 * Returns the meta object for data type '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL <em>TYP RL Object</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for data type '<em>TYP RL Object</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
	 * @model instanceClass="org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL"
	 *        extendedMetaData="name='TYP_RL:Object' baseType='TYP_RL'"
	 * @generated
	 */
	EDataType getTYP_RLObject();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Data_definitionFactory getData_definitionFactory();

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
	interface Literals
	{
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATIONImpl <em>COMBINATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCOMBINATION()
		 * @generated
		 */
		EClass COMBINATION = eINSTANCE.getCOMBINATION();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMBINATION__CODE = eINSTANCE.getCOMBINATION_Code();

		/**
		 * The meta object literal for the '<em><b>Combination id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMBINATION__COMBINATION_ID = eINSTANCE.getCOMBINATION_Combination_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION__MAINTENANCE_AGENCY_ID = eINSTANCE.getCOMBINATION_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMBINATION__NAME = eINSTANCE.getCOMBINATION_Name();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMBINATION__VALID_FROM = eINSTANCE.getCOMBINATION_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMBINATION__VALID_TO = eINSTANCE.getCOMBINATION_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMBINATION__VERSION = eINSTANCE.getCOMBINATION_Version();

		/**
		 * The meta object literal for the '<em><b>Combination items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION__COMBINATION_ITEMS = eINSTANCE.getCOMBINATION_Combination_items();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl <em>COMBINATION ITEM</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.COMBINATION_ITEMImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCOMBINATION_ITEM()
		 * @generated
		 */
		EClass COMBINATION_ITEM = eINSTANCE.getCOMBINATION_ITEM();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION_ITEM__MEMBER_ID = eINSTANCE.getCOMBINATION_ITEM_Member_id();

		/**
		 * The meta object literal for the '<em><b>Subdomain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION_ITEM__SUBDOMAIN_ID = eINSTANCE.getCOMBINATION_ITEM_Subdomain_id();

		/**
		 * The meta object literal for the '<em><b>Variable id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION_ITEM__VARIABLE_ID = eINSTANCE.getCOMBINATION_ITEM_Variable_id();

		/**
		 * The meta object literal for the '<em><b>Variable set id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION_ITEM__VARIABLE_SET_ID = eINSTANCE.getCOMBINATION_ITEM_Variable_set_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl <em>CUBE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE()
		 * @generated
		 */
		EClass CUBE = eINSTANCE.getCUBE();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__CODE = eINSTANCE.getCUBE_Code();

		/**
		 * The meta object literal for the '<em><b>Cube id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__CUBE_ID = eINSTANCE.getCUBE_Cube_id();

		/**
		 * The meta object literal for the '<em><b>Cube structure id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE__CUBE_STRUCTURE_ID = eINSTANCE.getCUBE_Cube_structure_id();

		/**
		 * The meta object literal for the '<em><b>Cube type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__CUBE_TYPE = eINSTANCE.getCUBE_Cube_type();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__DESCRIPTION = eINSTANCE.getCUBE_Description();

		/**
		 * The meta object literal for the '<em><b>Framework id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE__FRAMEWORK_ID = eINSTANCE.getCUBE_Framework_id();

		/**
		 * The meta object literal for the '<em><b>Is allowed</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__IS_ALLOWED = eINSTANCE.getCUBE_Is_allowed();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE__MAINTENANCE_AGENCY_ID = eINSTANCE.getCUBE_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__NAME = eINSTANCE.getCUBE_Name();

		/**
		 * The meta object literal for the '<em><b>Published</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__PUBLISHED = eINSTANCE.getCUBE_Published();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__VALID_FROM = eINSTANCE.getCUBE_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__VALID_TO = eINSTANCE.getCUBE_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__VERSION = eINSTANCE.getCUBE_Version();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE__DISPLAY_NAME = eINSTANCE.getCUBE_DisplayName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl <em>CUBE GROUP</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_GROUP()
		 * @generated
		 */
		EClass CUBE_GROUP = eINSTANCE.getCUBE_GROUP();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_GROUP__CODE = eINSTANCE.getCUBE_GROUP_Code();

		/**
		 * The meta object literal for the '<em><b>Cube group id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_GROUP__CUBE_GROUP_ID = eINSTANCE.getCUBE_GROUP_Cube_group_id();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_GROUP__DESCRIPTION = eINSTANCE.getCUBE_GROUP_Description();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_GROUP__MAINTENANCE_AGENCY_ID = eINSTANCE.getCUBE_GROUP_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_GROUP__NAME = eINSTANCE.getCUBE_GROUP_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl <em>CUBE GROUP ENUMERATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUP_ENUMERATIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_GROUP_ENUMERATION()
		 * @generated
		 */
		EClass CUBE_GROUP_ENUMERATION = eINSTANCE.getCUBE_GROUP_ENUMERATION();

		/**
		 * The meta object literal for the '<em><b>Cube group id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID = eINSTANCE.getCUBE_GROUP_ENUMERATION_Cube_group_id();

		/**
		 * The meta object literal for the '<em><b>Cube id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_GROUP_ENUMERATION__CUBE_ID = eINSTANCE.getCUBE_GROUP_ENUMERATION_Cube_id();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_GROUP_ENUMERATION__ORDER = eINSTANCE.getCUBE_GROUP_ENUMERATION_Order();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_GROUP_ENUMERATION__VALID_FROM = eINSTANCE.getCUBE_GROUP_ENUMERATION_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_GROUP_ENUMERATION__VALID_TO = eINSTANCE.getCUBE_GROUP_ENUMERATION_Valid_to();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHYImpl <em>CUBE HIERARCHY</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHYImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_HIERARCHY()
		 * @generated
		 */
		EClass CUBE_HIERARCHY = eINSTANCE.getCUBE_HIERARCHY();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY__CODE = eINSTANCE.getCUBE_HIERARCHY_Code();

		/**
		 * The meta object literal for the '<em><b>Cube hierarchy id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY__CUBE_HIERARCHY_ID = eINSTANCE.getCUBE_HIERARCHY_Cube_hierarchy_id();

		/**
		 * The meta object literal for the '<em><b>Cube hierarchy type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE = eINSTANCE.getCUBE_HIERARCHY_Cube_hierarchy_type();

		/**
		 * The meta object literal for the '<em><b>Framework id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_HIERARCHY__FRAMEWORK_ID = eINSTANCE.getCUBE_HIERARCHY_Framework_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID = eINSTANCE.getCUBE_HIERARCHY_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY__NAME = eINSTANCE.getCUBE_HIERARCHY_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl <em>CUBE HIERARCHY NODE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_HIERARCHY_NODE()
		 * @generated
		 */
		EClass CUBE_HIERARCHY_NODE = eINSTANCE.getCUBE_HIERARCHY_NODE();

		/**
		 * The meta object literal for the '<em><b>Cube group id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_HIERARCHY_NODE__CUBE_GROUP_ID = eINSTANCE.getCUBE_HIERARCHY_NODE_Cube_group_id();

		/**
		 * The meta object literal for the '<em><b>Cube hierarchy id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID = eINSTANCE.getCUBE_HIERARCHY_NODE_Cube_hierarchy_id();

		/**
		 * The meta object literal for the '<em><b>Level</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY_NODE__LEVEL = eINSTANCE.getCUBE_HIERARCHY_NODE_Level();

		/**
		 * The meta object literal for the '<em><b>NODE CODE</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY_NODE__NODE_CODE = eINSTANCE.getCUBE_HIERARCHY_NODE_NODE_CODE();

		/**
		 * The meta object literal for the '<em><b>Node name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY_NODE__NODE_NAME = eINSTANCE.getCUBE_HIERARCHY_NODE_Node_name();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY_NODE__ORDER = eINSTANCE.getCUBE_HIERARCHY_NODE_Order();

		/**
		 * The meta object literal for the '<em><b>Parent node code</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_HIERARCHY_NODE__PARENT_NODE_CODE = eINSTANCE.getCUBE_HIERARCHY_NODE_Parent_node_code();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY_NODE__VALID_FROM = eINSTANCE.getCUBE_HIERARCHY_NODE_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_HIERARCHY_NODE__VALID_TO = eINSTANCE.getCUBE_HIERARCHY_NODE_Valid_to();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl <em>CUBE RELATIONSHIP</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_RELATIONSHIP()
		 * @generated
		 */
		EClass CUBE_RELATIONSHIP = eINSTANCE.getCUBE_RELATIONSHIP();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__CODE = eINSTANCE.getCUBE_RELATIONSHIP_Code();

		/**
		 * The meta object literal for the '<em><b>Cube relationship id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID = eINSTANCE.getCUBE_RELATIONSHIP_Cube_relationship_id();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__DESCRIPTION = eINSTANCE.getCUBE_RELATIONSHIP_Description();

		/**
		 * The meta object literal for the '<em><b>Establishes integrity</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY = eINSTANCE.getCUBE_RELATIONSHIP_Establishes_integrity();

		/**
		 * The meta object literal for the '<em><b>Foreign cube id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_RELATIONSHIP__FOREIGN_CUBE_ID = eINSTANCE.getCUBE_RELATIONSHIP_Foreign_cube_id();

		/**
		 * The meta object literal for the '<em><b>Foreign cube variable code</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE = eINSTANCE.getCUBE_RELATIONSHIP_Foreign_cube_variable_code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID = eINSTANCE.getCUBE_RELATIONSHIP_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__NAME = eINSTANCE.getCUBE_RELATIONSHIP_Name();

		/**
		 * The meta object literal for the '<em><b>Primary cube id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_RELATIONSHIP__PRIMARY_CUBE_ID = eINSTANCE.getCUBE_RELATIONSHIP_Primary_cube_id();

		/**
		 * The meta object literal for the '<em><b>Primary cube variable id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID = eINSTANCE.getCUBE_RELATIONSHIP_Primary_cube_variable_id();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__VALID_FROM = eINSTANCE.getCUBE_RELATIONSHIP_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__VALID_TO = eINSTANCE.getCUBE_RELATIONSHIP_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_RELATIONSHIP__VERSION = eINSTANCE.getCUBE_RELATIONSHIP_Version();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTUREImpl <em>CUBE STRUCTURE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTUREImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_STRUCTURE()
		 * @generated
		 */
		EClass CUBE_STRUCTURE = eINSTANCE.getCUBE_STRUCTURE();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__CODE = eINSTANCE.getCUBE_STRUCTURE_Code();

		/**
		 * The meta object literal for the '<em><b>Cube structure id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__CUBE_STRUCTURE_ID = eINSTANCE.getCUBE_STRUCTURE_Cube_structure_id();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__DESCRIPTION = eINSTANCE.getCUBE_STRUCTURE_Description();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID = eINSTANCE.getCUBE_STRUCTURE_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__NAME = eINSTANCE.getCUBE_STRUCTURE_Name();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__VALID_FROM = eINSTANCE.getCUBE_STRUCTURE_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__VALID_TO = eINSTANCE.getCUBE_STRUCTURE_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__VERSION = eINSTANCE.getCUBE_STRUCTURE_Version();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE__DISPLAY_NAME = eINSTANCE.getCUBE_STRUCTURE_DisplayName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl <em>CUBE STRUCTURE ITEM</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_STRUCTURE_ITEM()
		 * @generated
		 */
		EClass CUBE_STRUCTURE_ITEM = eINSTANCE.getCUBE_STRUCTURE_ITEM();

		/**
		 * The meta object literal for the '<em><b>Attribute associated variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE = eINSTANCE.getCUBE_STRUCTURE_ITEM_Attribute_associated_variable();

		/**
		 * The meta object literal for the '<em><b>Cube structure id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID = eINSTANCE.getCUBE_STRUCTURE_ITEM_Cube_structure_id();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__DESCRIPTION = eINSTANCE.getCUBE_STRUCTURE_ITEM_Description();

		/**
		 * The meta object literal for the '<em><b>Dimension type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__DIMENSION_TYPE = eINSTANCE.getCUBE_STRUCTURE_ITEM_Dimension_type();

		/**
		 * The meta object literal for the '<em><b>Is flow</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__IS_FLOW = eINSTANCE.getCUBE_STRUCTURE_ITEM_Is_flow();

		/**
		 * The meta object literal for the '<em><b>Is mandatory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__IS_MANDATORY = eINSTANCE.getCUBE_STRUCTURE_ITEM_Is_mandatory();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_STRUCTURE_ITEM__MEMBER_ID = eINSTANCE.getCUBE_STRUCTURE_ITEM_Member_id();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__ORDER = eINSTANCE.getCUBE_STRUCTURE_ITEM_Order();

		/**
		 * The meta object literal for the '<em><b>Role</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__ROLE = eINSTANCE.getCUBE_STRUCTURE_ITEM_Role();

		/**
		 * The meta object literal for the '<em><b>Subdomain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID = eINSTANCE.getCUBE_STRUCTURE_ITEM_Subdomain_id();

		/**
		 * The meta object literal for the '<em><b>Variable id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_STRUCTURE_ITEM__VARIABLE_ID = eINSTANCE.getCUBE_STRUCTURE_ITEM_Variable_id();

		/**
		 * The meta object literal for the '<em><b>Variable set id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID = eINSTANCE.getCUBE_STRUCTURE_ITEM_Variable_set_id();

		/**
		 * The meta object literal for the '<em><b>Is Identifier</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__IS_IDENTIFIER = eINSTANCE.getCUBE_STRUCTURE_ITEM_IsIdentifier();

		/**
		 * The meta object literal for the '<em><b>Cube variable code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE = eINSTANCE.getCUBE_STRUCTURE_ITEM_Cube_variable_code();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_TO_COMBINATIONImpl <em>CUBE TO COMBINATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_TO_COMBINATIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getCUBE_TO_COMBINATION()
		 * @generated
		 */
		EClass CUBE_TO_COMBINATION = eINSTANCE.getCUBE_TO_COMBINATION();

		/**
		 * The meta object literal for the '<em><b>Combination id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_TO_COMBINATION__COMBINATION_ID = eINSTANCE.getCUBE_TO_COMBINATION_Combination_id();

		/**
		 * The meta object literal for the '<em><b>Cube id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_TO_COMBINATION__CUBE_ID = eINSTANCE.getCUBE_TO_COMBINATION_Cube_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORKImpl <em>FRAMEWORK</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORKImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getFRAMEWORK()
		 * @generated
		 */
		EClass FRAMEWORK = eINSTANCE.getFRAMEWORK();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FRAMEWORK__CODE = eINSTANCE.getFRAMEWORK_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FRAMEWORK__DESCRIPTION = eINSTANCE.getFRAMEWORK_Description();

		/**
		 * The meta object literal for the '<em><b>Framework id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FRAMEWORK__FRAMEWORK_ID = eINSTANCE.getFRAMEWORK_Framework_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FRAMEWORK__MAINTENANCE_AGENCY_ID = eINSTANCE.getFRAMEWORK_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FRAMEWORK__NAME = eINSTANCE.getFRAMEWORK_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_SUBDOMAINImpl <em>FRAMEWORK SUBDOMAIN</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_SUBDOMAINImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getFRAMEWORK_SUBDOMAIN()
		 * @generated
		 */
		EClass FRAMEWORK_SUBDOMAIN = eINSTANCE.getFRAMEWORK_SUBDOMAIN();

		/**
		 * The meta object literal for the '<em><b>Framework id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID = eINSTANCE.getFRAMEWORK_SUBDOMAIN_Framework_id();

		/**
		 * The meta object literal for the '<em><b>Subdomain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID = eINSTANCE.getFRAMEWORK_SUBDOMAIN_Subdomain_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_VARIABLE_SETImpl <em>FRAMEWORK VARIABLE SET</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_VARIABLE_SETImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getFRAMEWORK_VARIABLE_SET()
		 * @generated
		 */
		EClass FRAMEWORK_VARIABLE_SET = eINSTANCE.getFRAMEWORK_VARIABLE_SET();

		/**
		 * The meta object literal for the '<em><b>Framework id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID = eINSTANCE.getFRAMEWORK_VARIABLE_SET_Framework_id();

		/**
		 * The meta object literal for the '<em><b>Variable set id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID = eINSTANCE.getFRAMEWORK_VARIABLE_SET_Variable_set_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN <em>TYP DMNSN</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_DMNSN()
		 * @generated
		 */
		EEnum TYP_DMNSN = eINSTANCE.getTYP_DMNSN();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL <em>TYP RL</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_RL()
		 * @generated
		 */
		EEnum TYP_RL = eINSTANCE.getTYP_RL();

		/**
		 * The meta object literal for the '<em>TYP DMNSN Object</em>' data type.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_DMNSNObject()
		 * @generated
		 */
		EDataType TYP_DMNSN_OBJECT = eINSTANCE.getTYP_DMNSNObject();

		/**
		 * The meta object literal for the '<em>TYP RL Object</em>' data type.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl#getTYP_RLObject()
		 * @generated
		 */
		EDataType TYP_RL_OBJECT = eINSTANCE.getTYP_RLObject();

	}

} //Data_definitionPackage
