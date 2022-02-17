/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;

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
 * This package describes the Core package from SMCubes, using fields from SDD, see ECB documentation for details
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CoreFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r This file contains a representation of the core package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html'"
 * @generated
 */
public interface CorePackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "core";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/core";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "core";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	CorePackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.DOMAINImpl <em>DOMAIN</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.DOMAINImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getDOMAIN()
	 * @generated
	 */
	int DOMAIN = 0;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__CODE = 0;

	/**
	 * The feature id for the '<em><b>Data type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DATA_TYPE = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DOMAIN_ID = 3;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__FACET_ID = 4;

	/**
	 * The feature id for the '<em><b>Is enumerated</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__IS_ENUMERATED = 5;

	/**
	 * The feature id for the '<em><b>Is reference</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__IS_REFERENCE = 6;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__MAINTENANCE_AGENCY_ID = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__NAME = 8;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DISPLAY_NAME = 9;

	/**
	 * The number of structural features of the '<em>DOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_FEATURE_COUNT = 10;

	/**
	 * The number of operations of the '<em>DOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl <em>FACET COLLECTION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_COLLECTION()
	 * @generated
	 */
	int FACET_COLLECTION = 1;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__CODE = 0;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__FACET_ID = 1;

	/**
	 * The feature id for the '<em><b>Facet value type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__FACET_VALUE_TYPE = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__NAME = 4;

	/**
	 * The number of structural features of the '<em>FACET COLLECTION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>FACET COLLECTION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_ENUMERATIONImpl <em>FACET ENUMERATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_ENUMERATIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_ENUMERATION()
	 * @generated
	 */
	int FACET_ENUMERATION = 2;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION__FACET_ID = 0;

	/**
	 * The feature id for the '<em><b>Facet type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION__FACET_TYPE = 1;

	/**
	 * The feature id for the '<em><b>Observation value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION__OBSERVATION_VALUE = 2;

	/**
	 * The number of structural features of the '<em>FACET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>FACET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.facet_typeImpl <em>facet type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.facet_typeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getfacet_type()
	 * @generated
	 */
	int FACET_TYPE = 3;

	/**
	 * The feature id for the '<em><b>Decimals</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__DECIMALS = 0;

	/**
	 * The feature id for the '<em><b>End Time</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__END_TIME = 1;

	/**
	 * The feature id for the '<em><b>End Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__END_VALUE = 2;

	/**
	 * The feature id for the '<em><b>Interval</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__INTERVAL = 3;

	/**
	 * The feature id for the '<em><b>Is Sequence</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__IS_SEQUENCE = 4;

	/**
	 * The feature id for the '<em><b>Max Length</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MAX_LENGTH = 5;

	/**
	 * The feature id for the '<em><b>Max Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MAX_VALUE = 6;

	/**
	 * The feature id for the '<em><b>Min Length</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MIN_LENGTH = 7;

	/**
	 * The feature id for the '<em><b>Min Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MIN_VALUE = 8;

	/**
	 * The feature id for the '<em><b>Pattern</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__PATTERN = 9;

	/**
	 * The feature id for the '<em><b>Start Time</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__START_TIME = 10;

	/**
	 * The feature id for the '<em><b>Start Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__START_VALUE = 11;

	/**
	 * The feature id for the '<em><b>Time Interval</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__TIME_INTERVAL = 12;

	/**
	 * The number of structural features of the '<em>facet type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE_FEATURE_COUNT = 13;

	/**
	 * The number of operations of the '<em>facet type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MAINTENANCE_AGENCYImpl <em>MAINTENANCE AGENCY</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MAINTENANCE_AGENCYImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMAINTENANCE_AGENCY()
	 * @generated
	 */
	int MAINTENANCE_AGENCY = 4;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY__CODE = 0;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY__NAME = 2;

	/**
	 * The number of structural features of the '<em>MAINTENANCE AGENCY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>MAINTENANCE AGENCY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBERImpl <em>MEMBER</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBERImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMEMBER()
	 * @generated
	 */
	int MEMBER = 5;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__DOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__MEMBER_ID = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__NAME = 5;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__DISPLAY_NAME = 6;

	/**
	 * The number of structural features of the '<em>MEMBER</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>MEMBER</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl <em>MEMBER HIERARCHY</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMEMBER_HIERARCHY()
	 * @generated
	 */
	int MEMBER_HIERARCHY = 6;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__DOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Member hierarchy id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__NAME = 5;

	/**
	 * The number of structural features of the '<em>MEMBER HIERARCHY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>MEMBER HIERARCHY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHY_NODEImpl <em>MEMBER HIERARCHY NODE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHY_NODEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	int MEMBER_HIERARCHY_NODE = 7;

	/**
	 * The feature id for the '<em><b>Comparator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__COMPARATOR = 0;

	/**
	 * The feature id for the '<em><b>Level</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__LEVEL = 1;

	/**
	 * The feature id for the '<em><b>Member hierarchy id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID = 2;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__MEMBER_ID = 3;

	/**
	 * The feature id for the '<em><b>Operator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__OPERATOR = 4;

	/**
	 * The feature id for the '<em><b>Parent member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID = 5;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__VALID_FROM = 6;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__VALID_TO = 7;

	/**
	 * The number of structural features of the '<em>MEMBER HIERARCHY NODE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>MEMBER HIERARCHY NODE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl <em>SUBDOMAIN</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getSUBDOMAIN()
	 * @generated
	 */
	int SUBDOMAIN = 8;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__NAME = Data_meta_modelPackage.CLASSIFIER__NAME;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__CODE = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__DESCRIPTION = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__DOMAIN_ID = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__FACET_ID = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Is listed</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__IS_LISTED = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Is natural</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__IS_NATURAL = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__MAINTENANCE_AGENCY_ID = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 6;

	/**
	 * The feature id for the '<em><b>Subdomain id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__SUBDOMAIN_ID = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 7;

	/**
	 * The feature id for the '<em><b>Items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__ITEMS = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 8;

	/**
	 * The number of structural features of the '<em>SUBDOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_FEATURE_COUNT = Data_meta_modelPackage.CLASSIFIER_FEATURE_COUNT + 9;

	/**
	 * The number of operations of the '<em>SUBDOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_OPERATION_COUNT = Data_meta_modelPackage.CLASSIFIER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl <em>SUBDOMAIN ENUMERATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	int SUBDOMAIN_ENUMERATION = 9;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__MEMBER_ID = 0;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__ORDER = 1;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__VALID_FROM = 2;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__VALID_TO = 3;

	/**
	 * The number of structural features of the '<em>SUBDOMAIN ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>SUBDOMAIN ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLEImpl <em>VARIABLE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getVARIABLE()
	 * @generated
	 */
	int VARIABLE = 10;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__DOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Primary concept</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__PRIMARY_CONCEPT = 4;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__VARIABLE_ID = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__NAME = 6;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__DISPLAY_NAME = 7;

	/**
	 * The number of structural features of the '<em>VARIABLE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>VARIABLE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SETImpl <em>VARIABLE SET</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SETImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getVARIABLE_SET()
	 * @generated
	 */
	int VARIABLE_SET = 11;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__MAINTENANCE_AGENCY_ID = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__NAME = 3;

	/**
	 * The feature id for the '<em><b>Variable set id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__VARIABLE_SET_ID = 4;

	/**
	 * The number of structural features of the '<em>VARIABLE SET</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>VARIABLE SET</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SET_ENUMERATIONImpl <em>VARIABLE SET ENUMERATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SET_ENUMERATIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	int VARIABLE_SET_ENUMERATION = 12;

	/**
	 * The feature id for the '<em><b>Is flow</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__IS_FLOW = 0;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__ORDER = 1;

	/**
	 * The feature id for the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VALID_FROM = 3;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VALID_TO = 4;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VARIABLE_ID = 5;

	/**
	 * The feature id for the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID = 6;

	/**
	 * The number of structural features of the '<em>VARIABLE SET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>VARIABLE SET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE <em>FACET VALUE TYPE</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_VALUE_TYPE()
	 * @generated
	 */
	int FACET_VALUE_TYPE = 13;

	/**
	 * The meta object id for the '<em>FACET VALUE TYPE Object</em>' data type.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_VALUE_TYPEObject()
	 * @generated
	 */
	int FACET_VALUE_TYPE_OBJECT = 14;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN <em>DOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>DOMAIN</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN
	 * @generated
	 */
	EClass getDOMAIN();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getCode()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getData_type <em>Data type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Data type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getData_type()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Data_type();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getDescription()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Domain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getDomain_id()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getFacet_id()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EReference getDOMAIN_Facet_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#isIs_enumerated <em>Is enumerated</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is enumerated</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#isIs_enumerated()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Is_enumerated();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#isIs_reference <em>Is reference</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is reference</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#isIs_reference()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Is_reference();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getMaintenance_agency_id()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EReference getDOMAIN_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getName()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN#getDisplayName()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_DisplayName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION <em>FACET COLLECTION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>FACET COLLECTION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION
	 * @generated
	 */
	EClass getFACET_COLLECTION();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getCode()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Facet id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_id()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Facet_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_value_type <em>Facet value type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Facet value type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getFacet_value_type()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Facet_value_type();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getMaintenance_agency_id()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EReference getFACET_COLLECTION_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION#getName()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION <em>FACET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>FACET ENUMERATION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION
	 * @generated
	 */
	EClass getFACET_ENUMERATION();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_id()
	 * @see #getFACET_ENUMERATION()
	 * @generated
	 */
	EReference getFACET_ENUMERATION_Facet_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_type <em>Facet type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_type()
	 * @see #getFACET_ENUMERATION()
	 * @generated
	 */
	EReference getFACET_ENUMERATION_Facet_type();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getObservation_value <em>Observation value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Observation value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getObservation_value()
	 * @see #getFACET_ENUMERATION()
	 * @generated
	 */
	EAttribute getFACET_ENUMERATION_Observation_value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type <em>facet type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>facet type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type
	 * @generated
	 */
	EClass getfacet_type();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getDecimals <em>Decimals</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Decimals</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getDecimals()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_Decimals();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getEndTime <em>End Time</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>End Time</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getEndTime()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_EndTime();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getEndValue <em>End Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>End Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getEndValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_EndValue();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getInterval <em>Interval</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Interval</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getInterval()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_Interval();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getIsSequence <em>Is Sequence</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Sequence</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getIsSequence()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_IsSequence();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMaxLength <em>Max Length</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Max Length</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMaxLength()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MaxLength();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMaxValue <em>Max Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Max Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMaxValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MaxValue();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMinLength <em>Min Length</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Min Length</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMinLength()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MinLength();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMinValue <em>Min Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Min Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getMinValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MinValue();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getPattern <em>Pattern</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Pattern</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getPattern()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_Pattern();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getStartTime <em>Start Time</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Start Time</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getStartTime()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_StartTime();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getStartValue <em>Start Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Start Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getStartValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_StartValue();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getTimeInterval <em>Time Interval</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Time Interval</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type#getTimeInterval()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_TimeInterval();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY <em>MAINTENANCE AGENCY</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MAINTENANCE AGENCY</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY
	 * @generated
	 */
	EClass getMAINTENANCE_AGENCY();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY#getCode()
	 * @see #getMAINTENANCE_AGENCY()
	 * @generated
	 */
	EAttribute getMAINTENANCE_AGENCY_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY#getMaintenance_agency_id()
	 * @see #getMAINTENANCE_AGENCY()
	 * @generated
	 */
	EAttribute getMAINTENANCE_AGENCY_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY#getName()
	 * @see #getMAINTENANCE_AGENCY()
	 * @generated
	 */
	EAttribute getMAINTENANCE_AGENCY_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER <em>MEMBER</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER
	 * @generated
	 */
	EClass getMEMBER();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getCode()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDescription()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDomain_id()
	 * @see #getMEMBER()
	 * @generated
	 */
	EReference getMEMBER_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMaintenance_agency_id()
	 * @see #getMEMBER()
	 * @generated
	 */
	EReference getMEMBER_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getMember_id()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Member_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getName()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER#getDisplayName()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_DisplayName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY <em>MEMBER HIERARCHY</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER HIERARCHY</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY
	 * @generated
	 */
	EClass getMEMBER_HIERARCHY();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getCode()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getDescription()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getDomain_id()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getMaintenance_agency_id()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getMember_hierarchy_id <em>Member hierarchy id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Member hierarchy id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getMember_hierarchy_id()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Member_hierarchy_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY#getName()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE <em>MEMBER HIERARCHY NODE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER HIERARCHY NODE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE
	 * @generated
	 */
	EClass getMEMBER_HIERARCHY_NODE();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getComparator <em>Comparator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Comparator</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getComparator()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Comparator();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getLevel <em>Level</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Level</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getLevel()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Level();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_hierarchy_id <em>Member hierarchy id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member hierarchy id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_hierarchy_id()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_NODE_Member_hierarchy_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_id()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_NODE_Member_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getOperator <em>Operator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Operator</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getOperator()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Operator();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getParent_member_id <em>Parent member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Parent member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getParent_member_id()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_NODE_Parent_member_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_from()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_to()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Valid_to();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN <em>SUBDOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SUBDOMAIN</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN
	 * @generated
	 */
	EClass getSUBDOMAIN();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getCode()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getDescription()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getDomain_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getFacet_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Facet_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#isIs_listed <em>Is listed</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is listed</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#isIs_listed()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Is_listed();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#isIs_natural <em>Is natural</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is natural</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#isIs_natural()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Is_natural();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getMaintenance_agency_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getSubdomain_id <em>Subdomain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Subdomain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getSubdomain_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Subdomain_id();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getItems <em>Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN#getItems()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Items();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION <em>SUBDOMAIN ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SUBDOMAIN ENUMERATION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION
	 * @generated
	 */
	EClass getSUBDOMAIN_ENUMERATION();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getMember_id()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EReference getSUBDOMAIN_ENUMERATION_Member_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getOrder()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_ENUMERATION_Order();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_from()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_ENUMERATION_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION#getValid_to()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_ENUMERATION_Valid_to();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE <em>VARIABLE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE
	 * @generated
	 */
	EClass getVARIABLE();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getCode()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getDescription()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getDomain_id()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EReference getVARIABLE_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getMaintenance_agency_id()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EReference getVARIABLE_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getPrimary_concept <em>Primary concept</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Primary concept</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getPrimary_concept()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Primary_concept();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getVariable_id <em>Variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Variable id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getVariable_id()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Variable_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getName()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE#getDisplayName()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_DisplayName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET <em>VARIABLE SET</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE SET</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET
	 * @generated
	 */
	EClass getVARIABLE_SET();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getCode()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getDescription()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getMaintenance_agency_id()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EReference getVARIABLE_SET_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getName()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getVariable_set_id <em>Variable set id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Variable set id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET#getVariable_set_id()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Variable_set_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION <em>VARIABLE SET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE SET ENUMERATION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION
	 * @generated
	 */
	EClass getVARIABLE_SET_ENUMERATION();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#isIs_flow <em>Is flow</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is flow</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#isIs_flow()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Is_flow();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getOrder()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Order();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getSubdomain_id <em>Subdomain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Subdomain id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getSubdomain_id()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EReference getVARIABLE_SET_ENUMERATION_Subdomain_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getValid_from()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getValid_to()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Valid_to();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getVariable_id <em>Variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getVariable_id()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EReference getVARIABLE_SET_ENUMERATION_Variable_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getVariable_set_id <em>Variable set id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable set id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION#getVariable_set_id()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EReference getVARIABLE_SET_ENUMERATION_Variable_set_id();

	/**
	 * Returns the meta object for enum '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE <em>FACET VALUE TYPE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>FACET VALUE TYPE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
	 * @generated
	 */
	EEnum getFACET_VALUE_TYPE();

	/**
	 * Returns the meta object for data type '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE <em>FACET VALUE TYPE Object</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for data type '<em>FACET VALUE TYPE Object</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
	 * @model instanceClass="org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE"
	 *        extendedMetaData="name='FACET_VALUE_TYPE:Object' baseType='FACET_VALUE_TYPE'"
	 * @generated
	 */
	EDataType getFACET_VALUE_TYPEObject();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	CoreFactory getCoreFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.DOMAINImpl <em>DOMAIN</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.DOMAINImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getDOMAIN()
		 * @generated
		 */
		EClass DOMAIN = eINSTANCE.getDOMAIN();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__CODE = eINSTANCE.getDOMAIN_Code();

		/**
		 * The meta object literal for the '<em><b>Data type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DATA_TYPE = eINSTANCE.getDOMAIN_Data_type();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DESCRIPTION = eINSTANCE.getDOMAIN_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DOMAIN_ID = eINSTANCE.getDOMAIN_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DOMAIN__FACET_ID = eINSTANCE.getDOMAIN_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Is enumerated</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__IS_ENUMERATED = eINSTANCE.getDOMAIN_Is_enumerated();

		/**
		 * The meta object literal for the '<em><b>Is reference</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__IS_REFERENCE = eINSTANCE.getDOMAIN_Is_reference();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DOMAIN__MAINTENANCE_AGENCY_ID = eINSTANCE.getDOMAIN_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__NAME = eINSTANCE.getDOMAIN_Name();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DISPLAY_NAME = eINSTANCE.getDOMAIN_DisplayName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl <em>FACET COLLECTION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_COLLECTION()
		 * @generated
		 */
		EClass FACET_COLLECTION = eINSTANCE.getFACET_COLLECTION();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__CODE = eINSTANCE.getFACET_COLLECTION_Code();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__FACET_ID = eINSTANCE.getFACET_COLLECTION_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Facet value type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__FACET_VALUE_TYPE = eINSTANCE.getFACET_COLLECTION_Facet_value_type();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FACET_COLLECTION__MAINTENANCE_AGENCY_ID = eINSTANCE.getFACET_COLLECTION_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__NAME = eINSTANCE.getFACET_COLLECTION_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_ENUMERATIONImpl <em>FACET ENUMERATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_ENUMERATIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_ENUMERATION()
		 * @generated
		 */
		EClass FACET_ENUMERATION = eINSTANCE.getFACET_ENUMERATION();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FACET_ENUMERATION__FACET_ID = eINSTANCE.getFACET_ENUMERATION_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Facet type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FACET_ENUMERATION__FACET_TYPE = eINSTANCE.getFACET_ENUMERATION_Facet_type();

		/**
		 * The meta object literal for the '<em><b>Observation value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_ENUMERATION__OBSERVATION_VALUE = eINSTANCE.getFACET_ENUMERATION_Observation_value();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.facet_typeImpl <em>facet type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.facet_typeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getfacet_type()
		 * @generated
		 */
		EClass FACET_TYPE = eINSTANCE.getfacet_type();

		/**
		 * The meta object literal for the '<em><b>Decimals</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__DECIMALS = eINSTANCE.getfacet_type_Decimals();

		/**
		 * The meta object literal for the '<em><b>End Time</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__END_TIME = eINSTANCE.getfacet_type_EndTime();

		/**
		 * The meta object literal for the '<em><b>End Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__END_VALUE = eINSTANCE.getfacet_type_EndValue();

		/**
		 * The meta object literal for the '<em><b>Interval</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__INTERVAL = eINSTANCE.getfacet_type_Interval();

		/**
		 * The meta object literal for the '<em><b>Is Sequence</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__IS_SEQUENCE = eINSTANCE.getfacet_type_IsSequence();

		/**
		 * The meta object literal for the '<em><b>Max Length</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MAX_LENGTH = eINSTANCE.getfacet_type_MaxLength();

		/**
		 * The meta object literal for the '<em><b>Max Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MAX_VALUE = eINSTANCE.getfacet_type_MaxValue();

		/**
		 * The meta object literal for the '<em><b>Min Length</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MIN_LENGTH = eINSTANCE.getfacet_type_MinLength();

		/**
		 * The meta object literal for the '<em><b>Min Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MIN_VALUE = eINSTANCE.getfacet_type_MinValue();

		/**
		 * The meta object literal for the '<em><b>Pattern</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__PATTERN = eINSTANCE.getfacet_type_Pattern();

		/**
		 * The meta object literal for the '<em><b>Start Time</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__START_TIME = eINSTANCE.getfacet_type_StartTime();

		/**
		 * The meta object literal for the '<em><b>Start Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__START_VALUE = eINSTANCE.getfacet_type_StartValue();

		/**
		 * The meta object literal for the '<em><b>Time Interval</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__TIME_INTERVAL = eINSTANCE.getfacet_type_TimeInterval();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MAINTENANCE_AGENCYImpl <em>MAINTENANCE AGENCY</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MAINTENANCE_AGENCYImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMAINTENANCE_AGENCY()
		 * @generated
		 */
		EClass MAINTENANCE_AGENCY = eINSTANCE.getMAINTENANCE_AGENCY();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAINTENANCE_AGENCY__CODE = eINSTANCE.getMAINTENANCE_AGENCY_Code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID = eINSTANCE.getMAINTENANCE_AGENCY_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAINTENANCE_AGENCY__NAME = eINSTANCE.getMAINTENANCE_AGENCY_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBERImpl <em>MEMBER</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBERImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMEMBER()
		 * @generated
		 */
		EClass MEMBER = eINSTANCE.getMEMBER();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__CODE = eINSTANCE.getMEMBER_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__DESCRIPTION = eINSTANCE.getMEMBER_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER__DOMAIN_ID = eINSTANCE.getMEMBER_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER__MAINTENANCE_AGENCY_ID = eINSTANCE.getMEMBER_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__MEMBER_ID = eINSTANCE.getMEMBER_Member_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__NAME = eINSTANCE.getMEMBER_Name();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__DISPLAY_NAME = eINSTANCE.getMEMBER_DisplayName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl <em>MEMBER HIERARCHY</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMEMBER_HIERARCHY()
		 * @generated
		 */
		EClass MEMBER_HIERARCHY = eINSTANCE.getMEMBER_HIERARCHY();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__CODE = eINSTANCE.getMEMBER_HIERARCHY_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__DESCRIPTION = eINSTANCE.getMEMBER_HIERARCHY_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY__DOMAIN_ID = eINSTANCE.getMEMBER_HIERARCHY_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID = eINSTANCE.getMEMBER_HIERARCHY_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Member hierarchy id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID = eINSTANCE.getMEMBER_HIERARCHY_Member_hierarchy_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__NAME = eINSTANCE.getMEMBER_HIERARCHY_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHY_NODEImpl <em>MEMBER HIERARCHY NODE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.MEMBER_HIERARCHY_NODEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getMEMBER_HIERARCHY_NODE()
		 * @generated
		 */
		EClass MEMBER_HIERARCHY_NODE = eINSTANCE.getMEMBER_HIERARCHY_NODE();

		/**
		 * The meta object literal for the '<em><b>Comparator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__COMPARATOR = eINSTANCE.getMEMBER_HIERARCHY_NODE_Comparator();

		/**
		 * The meta object literal for the '<em><b>Level</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__LEVEL = eINSTANCE.getMEMBER_HIERARCHY_NODE_Level();

		/**
		 * The meta object literal for the '<em><b>Member hierarchy id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID = eINSTANCE.getMEMBER_HIERARCHY_NODE_Member_hierarchy_id();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_NODE__MEMBER_ID = eINSTANCE.getMEMBER_HIERARCHY_NODE_Member_id();

		/**
		 * The meta object literal for the '<em><b>Operator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__OPERATOR = eINSTANCE.getMEMBER_HIERARCHY_NODE_Operator();

		/**
		 * The meta object literal for the '<em><b>Parent member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID = eINSTANCE.getMEMBER_HIERARCHY_NODE_Parent_member_id();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__VALID_FROM = eINSTANCE.getMEMBER_HIERARCHY_NODE_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__VALID_TO = eINSTANCE.getMEMBER_HIERARCHY_NODE_Valid_to();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl <em>SUBDOMAIN</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getSUBDOMAIN()
		 * @generated
		 */
		EClass SUBDOMAIN = eINSTANCE.getSUBDOMAIN();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__CODE = eINSTANCE.getSUBDOMAIN_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__DESCRIPTION = eINSTANCE.getSUBDOMAIN_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__DOMAIN_ID = eINSTANCE.getSUBDOMAIN_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__FACET_ID = eINSTANCE.getSUBDOMAIN_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Is listed</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__IS_LISTED = eINSTANCE.getSUBDOMAIN_Is_listed();

		/**
		 * The meta object literal for the '<em><b>Is natural</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__IS_NATURAL = eINSTANCE.getSUBDOMAIN_Is_natural();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__MAINTENANCE_AGENCY_ID = eINSTANCE.getSUBDOMAIN_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Subdomain id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__SUBDOMAIN_ID = eINSTANCE.getSUBDOMAIN_Subdomain_id();

		/**
		 * The meta object literal for the '<em><b>Items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__ITEMS = eINSTANCE.getSUBDOMAIN_Items();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl <em>SUBDOMAIN ENUMERATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getSUBDOMAIN_ENUMERATION()
		 * @generated
		 */
		EClass SUBDOMAIN_ENUMERATION = eINSTANCE.getSUBDOMAIN_ENUMERATION();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN_ENUMERATION__MEMBER_ID = eINSTANCE.getSUBDOMAIN_ENUMERATION_Member_id();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN_ENUMERATION__ORDER = eINSTANCE.getSUBDOMAIN_ENUMERATION_Order();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN_ENUMERATION__VALID_FROM = eINSTANCE.getSUBDOMAIN_ENUMERATION_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN_ENUMERATION__VALID_TO = eINSTANCE.getSUBDOMAIN_ENUMERATION_Valid_to();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLEImpl <em>VARIABLE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getVARIABLE()
		 * @generated
		 */
		EClass VARIABLE = eINSTANCE.getVARIABLE();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__CODE = eINSTANCE.getVARIABLE_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__DESCRIPTION = eINSTANCE.getVARIABLE_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE__DOMAIN_ID = eINSTANCE.getVARIABLE_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE__MAINTENANCE_AGENCY_ID = eINSTANCE.getVARIABLE_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Primary concept</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__PRIMARY_CONCEPT = eINSTANCE.getVARIABLE_Primary_concept();

		/**
		 * The meta object literal for the '<em><b>Variable id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__VARIABLE_ID = eINSTANCE.getVARIABLE_Variable_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__NAME = eINSTANCE.getVARIABLE_Name();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__DISPLAY_NAME = eINSTANCE.getVARIABLE_DisplayName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SETImpl <em>VARIABLE SET</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SETImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getVARIABLE_SET()
		 * @generated
		 */
		EClass VARIABLE_SET = eINSTANCE.getVARIABLE_SET();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__CODE = eINSTANCE.getVARIABLE_SET_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__DESCRIPTION = eINSTANCE.getVARIABLE_SET_Description();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET__MAINTENANCE_AGENCY_ID = eINSTANCE.getVARIABLE_SET_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__NAME = eINSTANCE.getVARIABLE_SET_Name();

		/**
		 * The meta object literal for the '<em><b>Variable set id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__VARIABLE_SET_ID = eINSTANCE.getVARIABLE_SET_Variable_set_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SET_ENUMERATIONImpl <em>VARIABLE SET ENUMERATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.VARIABLE_SET_ENUMERATIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getVARIABLE_SET_ENUMERATION()
		 * @generated
		 */
		EClass VARIABLE_SET_ENUMERATION = eINSTANCE.getVARIABLE_SET_ENUMERATION();

		/**
		 * The meta object literal for the '<em><b>Is flow</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__IS_FLOW = eINSTANCE.getVARIABLE_SET_ENUMERATION_Is_flow();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__ORDER = eINSTANCE.getVARIABLE_SET_ENUMERATION_Order();

		/**
		 * The meta object literal for the '<em><b>Subdomain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID = eINSTANCE.getVARIABLE_SET_ENUMERATION_Subdomain_id();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__VALID_FROM = eINSTANCE.getVARIABLE_SET_ENUMERATION_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__VALID_TO = eINSTANCE.getVARIABLE_SET_ENUMERATION_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Variable id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_ENUMERATION__VARIABLE_ID = eINSTANCE.getVARIABLE_SET_ENUMERATION_Variable_id();

		/**
		 * The meta object literal for the '<em><b>Variable set id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID = eINSTANCE.getVARIABLE_SET_ENUMERATION_Variable_set_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE <em>FACET VALUE TYPE</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_VALUE_TYPE()
		 * @generated
		 */
		EEnum FACET_VALUE_TYPE = eINSTANCE.getFACET_VALUE_TYPE();

		/**
		 * The meta object literal for the '<em>FACET VALUE TYPE Object</em>' data type.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl#getFACET_VALUE_TYPEObject()
		 * @generated
		 */
		EDataType FACET_VALUE_TYPE_OBJECT = eINSTANCE.getFACET_VALUE_TYPEObject();

	}

} //CorePackage
