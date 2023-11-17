/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
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
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface regdnaPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "regdna";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/regdna";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "regdna";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	regdnaPackage eINSTANCE = org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ImportImpl <em>Import</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ImportImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getImport()
	 * @generated
	 */
	int IMPORT = 0;

	/**
	 * The feature id for the '<em><b>Imported Namespace</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT__IMPORTED_NAMESPACE = 0;

	/**
	 * The number of structural features of the '<em>Import</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Import</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ModuleImpl <em>Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ModuleImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getModule()
	 * @generated
	 */
	int MODULE = 1;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__DEPENDENCIES = 0;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__THE_DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__LICENSE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__NAME = 3;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__VERSION = 4;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__IMPORTS = 5;

	/**
	 * The number of structural features of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ModuleListImpl <em>Module List</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ModuleListImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getModuleList()
	 * @generated
	 */
	int MODULE_LIST = 2;

	/**
	 * The feature id for the '<em><b>Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LIST__MODULES = 0;

	/**
	 * The number of structural features of the '<em>Module List</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LIST_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Module List</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LIST_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.RulesForReportImpl <em>Rules For Report</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.RulesForReportImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getRulesForReport()
	 * @generated
	 */
	int RULES_FOR_REPORT = 3;

	/**
	 * The feature id for the '<em><b>Output Layer Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT__OUTPUT_LAYER_CUBE = 0;

	/**
	 * The feature id for the '<em><b>Rules For Table</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT__RULES_FOR_TABLE = 1;

	/**
	 * The number of structural features of the '<em>Rules For Report</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Rules For Report</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.RulesForILTableImpl <em>Rules For IL Table</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.RulesForILTableImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getRulesForILTable()
	 * @generated
	 */
	int RULES_FOR_IL_TABLE = 4;

	/**
	 * The feature id for the '<em><b>Rules For Table Part</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART = 0;

	/**
	 * The feature id for the '<em><b>Input Layer Table</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE = 1;

	/**
	 * The number of structural features of the '<em>Rules For IL Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Rules For IL Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnImpl <em>Select Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getSelectColumn()
	 * @generated
	 */
	int SELECT_COLUMN = 5;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN__AS_ATTRIBUTE = 0;

	/**
	 * The number of structural features of the '<em>Select Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Select Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnMemberAsImpl <em>Select Column Member As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnMemberAsImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getSelectColumnMemberAs()
	 * @generated
	 */
	int SELECT_COLUMN_MEMBER_AS = 6;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS__AS_ATTRIBUTE = SELECT_COLUMN__AS_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Member As Constant</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT = SELECT_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Select Column Member As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS_FEATURE_COUNT = SELECT_COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Select Column Member As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS_OPERATION_COUNT = SELECT_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnAttributeAsImpl <em>Select Column Attribute As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.SelectColumnAttributeAsImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getSelectColumnAttributeAs()
	 * @generated
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS = 7;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS__AS_ATTRIBUTE = SELECT_COLUMN__AS_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS__ATTRIBUTE = SELECT_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Select Column Attribute As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS_FEATURE_COUNT = SELECT_COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Select Column Attribute As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS_OPERATION_COUNT = SELECT_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.SelectDerivedColumnAsImpl <em>Select Derived Column As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.SelectDerivedColumnAsImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getSelectDerivedColumnAs()
	 * @generated
	 */
	int SELECT_DERIVED_COLUMN_AS = 8;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_DERIVED_COLUMN_AS__AS_ATTRIBUTE = SELECT_COLUMN__AS_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_DERIVED_COLUMN_AS__ATTRIBUTE = SELECT_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Select Derived Column As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_DERIVED_COLUMN_AS_FEATURE_COUNT = SELECT_COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Select Derived Column As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_DERIVED_COLUMN_AS_OPERATION_COUNT = SELECT_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.SelectValueAsImpl <em>Select Value As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.SelectValueAsImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getSelectValueAs()
	 * @generated
	 */
	int SELECT_VALUE_AS = 9;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS__AS_ATTRIBUTE = SELECT_COLUMN__AS_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS__VALUE = SELECT_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Select Value As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS_FEATURE_COUNT = SELECT_COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Select Value As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS_OPERATION_COUNT = SELECT_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.TableFilterImpl <em>Table Filter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.TableFilterImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getTableFilter()
	 * @generated
	 */
	int TABLE_FILTER = 10;

	/**
	 * The feature id for the '<em><b>Predicate</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER__PREDICATE = 0;

	/**
	 * The number of structural features of the '<em>Table Filter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Table Filter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.GenerationRulesModuleImpl <em>Generation Rules Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.GenerationRulesModuleImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getGenerationRulesModule()
	 * @generated
	 */
	int GENERATION_RULES_MODULE = 11;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>Rules For Report</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__RULES_FOR_REPORT = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Generation Rules Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Generation Rules Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.RuleForILTablePartImpl <em>Rule For IL Table Part</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.RuleForILTablePartImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getRuleForILTablePart()
	 * @generated
	 */
	int RULE_FOR_IL_TABLE_PART = 12;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART__NAME = 0;

	/**
	 * The feature id for the '<em><b>Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART__COLUMNS = 1;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE = 2;

	/**
	 * The number of structural features of the '<em>Rule For IL Table Part</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Rule For IL Table Part</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.PredicateImpl <em>Predicate</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.PredicateImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getPredicate()
	 * @generated
	 */
	int PREDICATE = 13;

	/**
	 * The number of structural features of the '<em>Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREDICATE_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREDICATE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.AndPredicateImpl <em>And Predicate</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.AndPredicateImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getAndPredicate()
	 * @generated
	 */
	int AND_PREDICATE = 14;

	/**
	 * The feature id for the '<em><b>Operands</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AND_PREDICATE__OPERANDS = PREDICATE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>And Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AND_PREDICATE_FEATURE_COUNT = PREDICATE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>And Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AND_PREDICATE_OPERATION_COUNT = PREDICATE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.OrPredicateImpl <em>Or Predicate</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.OrPredicateImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getOrPredicate()
	 * @generated
	 */
	int OR_PREDICATE = 15;

	/**
	 * The feature id for the '<em><b>Operands</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OR_PREDICATE__OPERANDS = PREDICATE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Or Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OR_PREDICATE_FEATURE_COUNT = PREDICATE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Or Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OR_PREDICATE_OPERATION_COUNT = PREDICATE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.NotPredicateImpl <em>Not Predicate</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.NotPredicateImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getNotPredicate()
	 * @generated
	 */
	int NOT_PREDICATE = 16;

	/**
	 * The feature id for the '<em><b>Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NOT_PREDICATE__OPERAND = PREDICATE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Not Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NOT_PREDICATE_FEATURE_COUNT = PREDICATE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Not Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NOT_PREDICATE_OPERATION_COUNT = PREDICATE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.AttributePredicateImpl <em>Attribute Predicate</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.AttributePredicateImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getAttributePredicate()
	 * @generated
	 */
	int ATTRIBUTE_PREDICATE = 17;

	/**
	 * The feature id for the '<em><b>Attribute1</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_PREDICATE__ATTRIBUTE1 = PREDICATE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Comparitor</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_PREDICATE__COMPARITOR = PREDICATE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Member</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_PREDICATE__MEMBER = PREDICATE_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_PREDICATE__VALUE = PREDICATE_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Attribute Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_PREDICATE_FEATURE_COUNT = PREDICATE_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Attribute Predicate</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_PREDICATE_OPERATION_COUNT = PREDICATE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELModelElementImpl <em>EL Model Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELModelElementImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELModelElement()
	 * @generated
	 */
	int EL_MODEL_ELEMENT = 24;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_MODEL_ELEMENT__EANNOTATIONS = 0;

	/**
	 * The number of structural features of the '<em>EL Model Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_MODEL_ELEMENT_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>EL Model Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_MODEL_ELEMENT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELNamedElementImpl <em>EL Named Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELNamedElementImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELNamedElement()
	 * @generated
	 */
	int EL_NAMED_ELEMENT = 25;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT__EANNOTATIONS = EL_MODEL_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT__NAME = EL_MODEL_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT_FEATURE_COUNT = EL_MODEL_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT_OPERATION_COUNT = EL_MODEL_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELTypedElementImpl <em>EL Typed Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELTypedElementImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELTypedElement()
	 * @generated
	 */
	int EL_TYPED_ELEMENT = 33;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__EANNOTATIONS = EL_NAMED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__NAME = EL_NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__ETYPE = EL_NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__UPPER_BOUND = EL_NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__LOWER_BOUND = EL_NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>EL Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT_FEATURE_COUNT = EL_NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>EL Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT_OPERATION_COUNT = EL_NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELStructuralFeatureImpl <em>EL Structural Feature</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELStructuralFeatureImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELStructuralFeature()
	 * @generated
	 */
	int EL_STRUCTURAL_FEATURE = 32;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__EANNOTATIONS = EL_TYPED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__NAME = EL_TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__ETYPE = EL_TYPED_ELEMENT__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__UPPER_BOUND = EL_TYPED_ELEMENT__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__LOWER_BOUND = EL_TYPED_ELEMENT__LOWER_BOUND;

	/**
	 * The number of structural features of the '<em>EL Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE_FEATURE_COUNT = EL_TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>EL Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE_OPERATION_COUNT = EL_TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAttributeImpl <em>EL Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELAttributeImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELAttribute()
	 * @generated
	 */
	int EL_ATTRIBUTE = 18;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__EANNOTATIONS = EL_STRUCTURAL_FEATURE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__NAME = EL_STRUCTURAL_FEATURE__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__ETYPE = EL_STRUCTURAL_FEATURE__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__UPPER_BOUND = EL_STRUCTURAL_FEATURE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__LOWER_BOUND = EL_STRUCTURAL_FEATURE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__ID = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__EATTRIBUTE_TYPE = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE_FEATURE_COUNT = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE_OPERATION_COUNT = EL_STRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELClassifierImpl <em>EL Classifier</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELClassifierImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELClassifier()
	 * @generated
	 */
	int EL_CLASSIFIER = 20;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER__EANNOTATIONS = EL_NAMED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER__NAME = EL_NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER__EPACKAGE = EL_NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER_FEATURE_COUNT = EL_NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER_OPERATION_COUNT = EL_NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELClassImpl <em>EL Class</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELClassImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELClass()
	 * @generated
	 */
	int EL_CLASS = 19;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__EANNOTATIONS = EL_CLASSIFIER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__NAME = EL_CLASSIFIER__NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__EPACKAGE = EL_CLASSIFIER__EPACKAGE;

	/**
	 * The feature id for the '<em><b>EAbstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__EABSTRACT = EL_CLASSIFIER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__ESUPER_TYPES = EL_CLASSIFIER_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__ESTRUCTURAL_FEATURES = EL_CLASSIFIER_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__EOPERATIONS = EL_CLASSIFIER_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>EL Class</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS_FEATURE_COUNT = EL_CLASSIFIER_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>EL Class</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS_OPERATION_COUNT = EL_CLASSIFIER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELDataTypeImpl <em>EL Data Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELDataTypeImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELDataType()
	 * @generated
	 */
	int EL_DATA_TYPE = 21;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__EANNOTATIONS = EL_CLASSIFIER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__NAME = EL_CLASSIFIER__NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__EPACKAGE = EL_CLASSIFIER__EPACKAGE;

	/**
	 * The feature id for the '<em><b>Industry Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__INDUSTRY_NAME = EL_CLASSIFIER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Data Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE_FEATURE_COUNT = EL_CLASSIFIER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Data Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE_OPERATION_COUNT = EL_CLASSIFIER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELEnumImpl <em>EL Enum</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELEnumImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELEnum()
	 * @generated
	 */
	int EL_ENUM = 22;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__EANNOTATIONS = EL_DATA_TYPE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__NAME = EL_DATA_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__EPACKAGE = EL_DATA_TYPE__EPACKAGE;

	/**
	 * The feature id for the '<em><b>Industry Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__INDUSTRY_NAME = EL_DATA_TYPE__INDUSTRY_NAME;

	/**
	 * The feature id for the '<em><b>ELiterals</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__ELITERALS = EL_DATA_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Enum</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_FEATURE_COUNT = EL_DATA_TYPE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Enum</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_OPERATION_COUNT = EL_DATA_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELEnumLiteralImpl <em>EL Enum Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELEnumLiteralImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELEnumLiteral()
	 * @generated
	 */
	int EL_ENUM_LITERAL = 23;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__EANNOTATIONS = EL_NAMED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__NAME = EL_NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__VALUE = EL_NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__LITERAL = EL_NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Enum Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL_FEATURE_COUNT = EL_NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Enum Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL_OPERATION_COUNT = EL_NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELOperationImpl <em>EL Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELOperationImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELOperation()
	 * @generated
	 */
	int EL_OPERATION = 26;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__EANNOTATIONS = EL_TYPED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__NAME = EL_TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__ETYPE = EL_TYPED_ELEMENT__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__UPPER_BOUND = EL_TYPED_ELEMENT__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__LOWER_BOUND = EL_TYPED_ELEMENT__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Body</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__BODY = EL_TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION_FEATURE_COUNT = EL_TYPED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION_OPERATION_COUNT = EL_TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELPublicOperationImpl <em>EL Public Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELPublicOperationImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELPublicOperation()
	 * @generated
	 */
	int EL_PUBLIC_OPERATION = 27;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION__EANNOTATIONS = EL_OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION__NAME = EL_OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION__ETYPE = EL_OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION__UPPER_BOUND = EL_OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION__LOWER_BOUND = EL_OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Body</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION__BODY = EL_OPERATION__BODY;

	/**
	 * The feature id for the '<em><b>Called Private Operations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION__CALLED_PRIVATE_OPERATIONS = EL_OPERATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Public Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION_FEATURE_COUNT = EL_OPERATION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Public Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PUBLIC_OPERATION_OPERATION_COUNT = EL_OPERATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELPrivateOperationImpl <em>EL Private Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELPrivateOperationImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELPrivateOperation()
	 * @generated
	 */
	int EL_PRIVATE_OPERATION = 28;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION__EANNOTATIONS = EL_OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION__NAME = EL_OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION__ETYPE = EL_OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION__UPPER_BOUND = EL_OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION__LOWER_BOUND = EL_OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Body</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION__BODY = EL_OPERATION__BODY;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION__EPARAMETERS = EL_OPERATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Private Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION_FEATURE_COUNT = EL_OPERATION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Private Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PRIVATE_OPERATION_OPERATION_COUNT = EL_OPERATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELParameterImpl <em>EL Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELParameterImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELParameter()
	 * @generated
	 */
	int EL_PARAMETER = 29;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PARAMETER__EANNOTATIONS = EL_TYPED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PARAMETER__NAME = EL_TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PARAMETER__ETYPE = EL_TYPED_ELEMENT__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PARAMETER__UPPER_BOUND = EL_TYPED_ELEMENT__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PARAMETER__LOWER_BOUND = EL_TYPED_ELEMENT__LOWER_BOUND;

	/**
	 * The number of structural features of the '<em>EL Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PARAMETER_FEATURE_COUNT = EL_TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>EL Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PARAMETER_OPERATION_COUNT = EL_TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELPackageImpl <em>EL Package</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELPackageImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELPackage()
	 * @generated
	 */
	int EL_PACKAGE = 30;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>EClassifiers</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__ECLASSIFIERS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Ns URI</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__NS_URI = MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Ns Prefix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__NS_PREFIX = MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>EL Package</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>EL Package</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELReferenceImpl <em>EL Reference</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELReferenceImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELReference()
	 * @generated
	 */
	int EL_REFERENCE = 31;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__EANNOTATIONS = EL_STRUCTURAL_FEATURE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__NAME = EL_STRUCTURAL_FEATURE__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__ETYPE = EL_STRUCTURAL_FEATURE__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__UPPER_BOUND = EL_STRUCTURAL_FEATURE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__LOWER_BOUND = EL_STRUCTURAL_FEATURE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__CONTAINMENT = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EReference Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__EREFERENCE_TYPE = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Reference</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE_FEATURE_COUNT = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Reference</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE_OPERATION_COUNT = EL_STRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAnnotationImpl <em>EL Annotation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELAnnotationImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELAnnotation()
	 * @generated
	 */
	int EL_ANNOTATION = 34;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION__EANNOTATIONS = EL_MODEL_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Details</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION__DETAILS = EL_MODEL_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Source</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION__SOURCE = EL_MODEL_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Annotation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION_FEATURE_COUNT = EL_MODEL_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Annotation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION_OPERATION_COUNT = EL_MODEL_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ELStringToStringMapEntryImpl <em>EL String To String Map Entry</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ELStringToStringMapEntryImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getELStringToStringMapEntry()
	 * @generated
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY = 35;

	/**
	 * The feature id for the '<em><b>Key</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY__KEY = 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY__VALUE = 1;

	/**
	 * The number of structural features of the '<em>EL String To String Map Entry</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>EL String To String Map Entry</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportImpl <em>Report</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ReportImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getReport()
	 * @generated
	 */
	int REPORT = 36;

	/**
	 * The feature id for the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT__OUTPUT_LAYER = 0;

	/**
	 * The feature id for the '<em><b>Rows</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT__ROWS = 1;

	/**
	 * The feature id for the '<em><b>Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT__COLUMNS = 2;

	/**
	 * The feature id for the '<em><b>Report Cells</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT__REPORT_CELLS = 3;

	/**
	 * The number of structural features of the '<em>Report</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Report</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportRowImpl <em>Report Row</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ReportRowImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getReportRow()
	 * @generated
	 */
	int REPORT_ROW = 37;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_ROW__NAME = 0;

	/**
	 * The number of structural features of the '<em>Report Row</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_ROW_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Report Row</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_ROW_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportColumnImpl <em>Report Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ReportColumnImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getReportColumn()
	 * @generated
	 */
	int REPORT_COLUMN = 38;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_COLUMN__NAME = 0;

	/**
	 * The number of structural features of the '<em>Report Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_COLUMN_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Report Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_COLUMN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportCellImpl <em>Report Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ReportCellImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getReportCell()
	 * @generated
	 */
	int REPORT_CELL = 39;

	/**
	 * The feature id for the '<em><b>Row</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL__ROW = 0;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL__COLUMN = 1;

	/**
	 * The feature id for the '<em><b>Filters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL__FILTERS = 2;

	/**
	 * The feature id for the '<em><b>Metric</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL__METRIC = 3;

	/**
	 * The feature id for the '<em><b>Datapoint ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL__DATAPOINT_ID = 4;

	/**
	 * The number of structural features of the '<em>Report Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>Report Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.FilterImpl <em>Filter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.FilterImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getFilter()
	 * @generated
	 */
	int FILTER = 40;

	/**
	 * The feature id for the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER__OUTPUT_LAYER = 0;

	/**
	 * The feature id for the '<em><b>Operation</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER__OPERATION = 1;

	/**
	 * The feature id for the '<em><b>Member</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER__MEMBER = 2;

	/**
	 * The number of structural features of the '<em>Filter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Filter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.impl.ReportModuleImpl <em>Report Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.ReportModuleImpl
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getReportModule()
	 * @generated
	 */
	int REPORT_MODULE = 41;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>Reports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE__REPORTS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Report Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Report Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.regdna.model.regdna.Comparitor <em>Comparitor</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.regdna.model.regdna.Comparitor
	 * @see org.eclipse.efbt.regdna.model.regdna.impl.regdnaPackageImpl#getComparitor()
	 * @generated
	 */
	int COMPARITOR = 42;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.Import <em>Import</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Import</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Import
	 * @generated
	 */
	EClass getImport();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.Import#getImportedNamespace <em>Imported Namespace</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Imported Namespace</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Import#getImportedNamespace()
	 * @see #getImport()
	 * @generated
	 */
	EAttribute getImport_ImportedNamespace();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Module
	 * @generated
	 */
	EClass getModule();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.regdna.model.regdna.Module#getDependencies <em>Dependencies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Dependencies</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Module#getDependencies()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_Dependencies();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.Module#getTheDescription <em>The Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>The Description</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Module#getTheDescription()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_TheDescription();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.Module#getLicense <em>License</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>License</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Module#getLicense()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_License();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.Module#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Module#getName()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.Module#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Module#getVersion()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Version();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.Module#getImports <em>Imports</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Imports</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Module#getImports()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_Imports();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ModuleList <em>Module List</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module List</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ModuleList
	 * @generated
	 */
	EClass getModuleList();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ModuleList#getModules <em>Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Modules</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ModuleList#getModules()
	 * @see #getModuleList()
	 * @generated
	 */
	EReference getModuleList_Modules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.RulesForReport <em>Rules For Report</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rules For Report</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RulesForReport
	 * @generated
	 */
	EClass getRulesForReport();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.RulesForReport#getOutputLayerCube <em>Output Layer Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Output Layer Cube</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RulesForReport#getOutputLayerCube()
	 * @see #getRulesForReport()
	 * @generated
	 */
	EReference getRulesForReport_OutputLayerCube();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.RulesForReport#getRulesForTable <em>Rules For Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules For Table</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RulesForReport#getRulesForTable()
	 * @see #getRulesForReport()
	 * @generated
	 */
	EReference getRulesForReport_RulesForTable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.RulesForILTable <em>Rules For IL Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rules For IL Table</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RulesForILTable
	 * @generated
	 */
	EClass getRulesForILTable();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.RulesForILTable#getRulesForTablePart <em>Rules For Table Part</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules For Table Part</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RulesForILTable#getRulesForTablePart()
	 * @see #getRulesForILTable()
	 * @generated
	 */
	EReference getRulesForILTable_RulesForTablePart();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.RulesForILTable#getInputLayerTable <em>Input Layer Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Input Layer Table</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RulesForILTable#getInputLayerTable()
	 * @see #getRulesForILTable()
	 * @generated
	 */
	EReference getRulesForILTable_InputLayerTable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.SelectColumn <em>Select Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Column</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectColumn
	 * @generated
	 */
	EClass getSelectColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.SelectColumn#getAsAttribute <em>As Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>As Attribute</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectColumn#getAsAttribute()
	 * @see #getSelectColumn()
	 * @generated
	 */
	EReference getSelectColumn_AsAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.SelectColumnMemberAs <em>Select Column Member As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Column Member As</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectColumnMemberAs
	 * @generated
	 */
	EClass getSelectColumnMemberAs();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.SelectColumnMemberAs#getMemberAsConstant <em>Member As Constant</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member As Constant</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectColumnMemberAs#getMemberAsConstant()
	 * @see #getSelectColumnMemberAs()
	 * @generated
	 */
	EReference getSelectColumnMemberAs_MemberAsConstant();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.SelectColumnAttributeAs <em>Select Column Attribute As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Column Attribute As</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectColumnAttributeAs
	 * @generated
	 */
	EClass getSelectColumnAttributeAs();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.SelectColumnAttributeAs#getAttribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectColumnAttributeAs#getAttribute()
	 * @see #getSelectColumnAttributeAs()
	 * @generated
	 */
	EReference getSelectColumnAttributeAs_Attribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.SelectDerivedColumnAs <em>Select Derived Column As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Derived Column As</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectDerivedColumnAs
	 * @generated
	 */
	EClass getSelectDerivedColumnAs();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.SelectDerivedColumnAs#getAttribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectDerivedColumnAs#getAttribute()
	 * @see #getSelectDerivedColumnAs()
	 * @generated
	 */
	EReference getSelectDerivedColumnAs_Attribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.SelectValueAs <em>Select Value As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Value As</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectValueAs
	 * @generated
	 */
	EClass getSelectValueAs();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.SelectValueAs#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.SelectValueAs#getValue()
	 * @see #getSelectValueAs()
	 * @generated
	 */
	EAttribute getSelectValueAs_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.TableFilter <em>Table Filter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Table Filter</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.TableFilter
	 * @generated
	 */
	EClass getTableFilter();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.regdna.model.regdna.TableFilter#getPredicate <em>Predicate</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Predicate</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.TableFilter#getPredicate()
	 * @see #getTableFilter()
	 * @generated
	 */
	EReference getTableFilter_Predicate();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.GenerationRulesModule <em>Generation Rules Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generation Rules Module</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.GenerationRulesModule
	 * @generated
	 */
	EClass getGenerationRulesModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.GenerationRulesModule#getRulesForReport <em>Rules For Report</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules For Report</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.GenerationRulesModule#getRulesForReport()
	 * @see #getGenerationRulesModule()
	 * @generated
	 */
	EReference getGenerationRulesModule_RulesForReport();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart <em>Rule For IL Table Part</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rule For IL Table Part</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart
	 * @generated
	 */
	EClass getRuleForILTablePart();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getName()
	 * @see #getRuleForILTablePart()
	 * @generated
	 */
	EAttribute getRuleForILTablePart_Name();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getColumns <em>Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Columns</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getColumns()
	 * @see #getRuleForILTablePart()
	 * @generated
	 */
	EReference getRuleForILTablePart_Columns();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getWhereClause()
	 * @see #getRuleForILTablePart()
	 * @generated
	 */
	EReference getRuleForILTablePart_WhereClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.Predicate <em>Predicate</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Predicate</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Predicate
	 * @generated
	 */
	EClass getPredicate();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.AndPredicate <em>And Predicate</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>And Predicate</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.AndPredicate
	 * @generated
	 */
	EClass getAndPredicate();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.AndPredicate#getOperands <em>Operands</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Operands</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.AndPredicate#getOperands()
	 * @see #getAndPredicate()
	 * @generated
	 */
	EReference getAndPredicate_Operands();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.OrPredicate <em>Or Predicate</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Or Predicate</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.OrPredicate
	 * @generated
	 */
	EClass getOrPredicate();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.OrPredicate#getOperands <em>Operands</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Operands</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.OrPredicate#getOperands()
	 * @see #getOrPredicate()
	 * @generated
	 */
	EReference getOrPredicate_Operands();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.NotPredicate <em>Not Predicate</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Not Predicate</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.NotPredicate
	 * @generated
	 */
	EClass getNotPredicate();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.regdna.model.regdna.NotPredicate#getOperand <em>Operand</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Operand</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.NotPredicate#getOperand()
	 * @see #getNotPredicate()
	 * @generated
	 */
	EReference getNotPredicate_Operand();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate <em>Attribute Predicate</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute Predicate</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.AttributePredicate
	 * @generated
	 */
	EClass getAttributePredicate();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getAttribute1 <em>Attribute1</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute1</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getAttribute1()
	 * @see #getAttributePredicate()
	 * @generated
	 */
	EReference getAttributePredicate_Attribute1();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getComparitor <em>Comparitor</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Comparitor</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getComparitor()
	 * @see #getAttributePredicate()
	 * @generated
	 */
	EAttribute getAttributePredicate_Comparitor();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getMember <em>Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getMember()
	 * @see #getAttributePredicate()
	 * @generated
	 */
	EReference getAttributePredicate_Member();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.AttributePredicate#getValue()
	 * @see #getAttributePredicate()
	 * @generated
	 */
	EAttribute getAttributePredicate_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELAttribute <em>EL Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Attribute</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELAttribute
	 * @generated
	 */
	EClass getELAttribute();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELAttribute#isID <em>ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>ID</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELAttribute#isID()
	 * @see #getELAttribute()
	 * @generated
	 */
	EAttribute getELAttribute_ID();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.ELAttribute#getEAttributeType <em>EAttribute Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>EAttribute Type</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELAttribute#getEAttributeType()
	 * @see #getELAttribute()
	 * @generated
	 */
	EReference getELAttribute_EAttributeType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELClass <em>EL Class</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Class</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClass
	 * @generated
	 */
	EClass getELClass();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELClass#isEAbstract <em>EAbstract</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>EAbstract</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClass#isEAbstract()
	 * @see #getELClass()
	 * @generated
	 */
	EAttribute getELClass_EAbstract();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELClass#getESuperTypes <em>ESuper Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>ESuper Types</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClass#getESuperTypes()
	 * @see #getELClass()
	 * @generated
	 */
	EReference getELClass_ESuperTypes();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELClass#getEStructuralFeatures <em>EStructural Features</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>EStructural Features</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClass#getEStructuralFeatures()
	 * @see #getELClass()
	 * @generated
	 */
	EReference getELClass_EStructuralFeatures();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELClass#getEOperations <em>EOperations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>EOperations</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClass#getEOperations()
	 * @see #getELClass()
	 * @generated
	 */
	EReference getELClass_EOperations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELClassifier <em>EL Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Classifier</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClassifier
	 * @generated
	 */
	EClass getELClassifier();

	/**
	 * Returns the meta object for the container reference '{@link org.eclipse.efbt.regdna.model.regdna.ELClassifier#getEPackage <em>EPackage</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>EPackage</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELClassifier#getEPackage()
	 * @see #getELClassifier()
	 * @generated
	 */
	EReference getELClassifier_EPackage();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELDataType <em>EL Data Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Data Type</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELDataType
	 * @generated
	 */
	EClass getELDataType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELDataType#getIndustryName <em>Industry Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Industry Name</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELDataType#getIndustryName()
	 * @see #getELDataType()
	 * @generated
	 */
	EAttribute getELDataType_IndustryName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELEnum <em>EL Enum</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Enum</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELEnum
	 * @generated
	 */
	EClass getELEnum();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELEnum#getELiterals <em>ELiterals</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>ELiterals</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELEnum#getELiterals()
	 * @see #getELEnum()
	 * @generated
	 */
	EReference getELEnum_ELiterals();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral <em>EL Enum Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Enum Literal</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral
	 * @generated
	 */
	EClass getELEnumLiteral();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral#getValue()
	 * @see #getELEnumLiteral()
	 * @generated
	 */
	EAttribute getELEnumLiteral_Value();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral#getLiteral <em>Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Literal</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral#getLiteral()
	 * @see #getELEnumLiteral()
	 * @generated
	 */
	EAttribute getELEnumLiteral_Literal();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELModelElement <em>EL Model Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Model Element</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELModelElement
	 * @generated
	 */
	EClass getELModelElement();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.regdna.model.regdna.ELModelElement#getEAnnotations <em>EAnnotations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>EAnnotations</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELModelElement#getEAnnotations()
	 * @see #getELModelElement()
	 * @generated
	 */
	EReference getELModelElement_EAnnotations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELNamedElement <em>EL Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Named Element</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELNamedElement
	 * @generated
	 */
	EClass getELNamedElement();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELNamedElement#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELNamedElement#getName()
	 * @see #getELNamedElement()
	 * @generated
	 */
	EAttribute getELNamedElement_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELOperation <em>EL Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Operation</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELOperation
	 * @generated
	 */
	EClass getELOperation();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELOperation#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Body</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELOperation#getBody()
	 * @see #getELOperation()
	 * @generated
	 */
	EAttribute getELOperation_Body();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELPublicOperation <em>EL Public Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Public Operation</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPublicOperation
	 * @generated
	 */
	EClass getELPublicOperation();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELPublicOperation#getCalledPrivateOperations <em>Called Private Operations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Called Private Operations</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPublicOperation#getCalledPrivateOperations()
	 * @see #getELPublicOperation()
	 * @generated
	 */
	EReference getELPublicOperation_CalledPrivateOperations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELPrivateOperation <em>EL Private Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Private Operation</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPrivateOperation
	 * @generated
	 */
	EClass getELPrivateOperation();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELPrivateOperation#getEParameters <em>EParameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>EParameters</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPrivateOperation#getEParameters()
	 * @see #getELPrivateOperation()
	 * @generated
	 */
	EReference getELPrivateOperation_EParameters();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELParameter <em>EL Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Parameter</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELParameter
	 * @generated
	 */
	EClass getELParameter();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELPackage <em>EL Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Package</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPackage
	 * @generated
	 */
	EClass getELPackage();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getEClassifiers <em>EClassifiers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>EClassifiers</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPackage#getEClassifiers()
	 * @see #getELPackage()
	 * @generated
	 */
	EReference getELPackage_EClassifiers();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsURI <em>Ns URI</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns URI</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsURI()
	 * @see #getELPackage()
	 * @generated
	 */
	EAttribute getELPackage_NsURI();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsPrefix <em>Ns Prefix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns Prefix</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELPackage#getNsPrefix()
	 * @see #getELPackage()
	 * @generated
	 */
	EAttribute getELPackage_NsPrefix();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELReference <em>EL Reference</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Reference</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELReference
	 * @generated
	 */
	EClass getELReference();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELReference#isContainment <em>Containment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Containment</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELReference#isContainment()
	 * @see #getELReference()
	 * @generated
	 */
	EAttribute getELReference_Containment();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.ELReference#getEReferenceType <em>EReference Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>EReference Type</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELReference#getEReferenceType()
	 * @see #getELReference()
	 * @generated
	 */
	EReference getELReference_EReferenceType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELStructuralFeature <em>EL Structural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Structural Feature</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELStructuralFeature
	 * @generated
	 */
	EClass getELStructuralFeature();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement <em>EL Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Typed Element</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELTypedElement
	 * @generated
	 */
	EClass getELTypedElement();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getEType <em>EType</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>EType</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getEType()
	 * @see #getELTypedElement()
	 * @generated
	 */
	EReference getELTypedElement_EType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getUpperBound <em>Upper Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Upper Bound</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getUpperBound()
	 * @see #getELTypedElement()
	 * @generated
	 */
	EAttribute getELTypedElement_UpperBound();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getLowerBound <em>Lower Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Lower Bound</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELTypedElement#getLowerBound()
	 * @see #getELTypedElement()
	 * @generated
	 */
	EAttribute getELTypedElement_LowerBound();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELAnnotation <em>EL Annotation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Annotation</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELAnnotation
	 * @generated
	 */
	EClass getELAnnotation();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ELAnnotation#getDetails <em>Details</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Details</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELAnnotation#getDetails()
	 * @see #getELAnnotation()
	 * @generated
	 */
	EReference getELAnnotation_Details();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELAnnotation#getSource <em>Source</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Source</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELAnnotation#getSource()
	 * @see #getELAnnotation()
	 * @generated
	 */
	EAttribute getELAnnotation_Source();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry <em>EL String To String Map Entry</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL String To String Map Entry</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry
	 * @generated
	 */
	EClass getELStringToStringMapEntry();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry#getKey <em>Key</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Key</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry#getKey()
	 * @see #getELStringToStringMapEntry()
	 * @generated
	 */
	EAttribute getELStringToStringMapEntry_Key();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry#getValue()
	 * @see #getELStringToStringMapEntry()
	 * @generated
	 */
	EAttribute getELStringToStringMapEntry_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.Report <em>Report</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Report
	 * @generated
	 */
	EClass getReport();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.Report#getOutputLayer <em>Output Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Output Layer</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Report#getOutputLayer()
	 * @see #getReport()
	 * @generated
	 */
	EReference getReport_OutputLayer();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.Report#getRows <em>Rows</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rows</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Report#getRows()
	 * @see #getReport()
	 * @generated
	 */
	EReference getReport_Rows();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.Report#getColumns <em>Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Columns</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Report#getColumns()
	 * @see #getReport()
	 * @generated
	 */
	EReference getReport_Columns();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.Report#getReportCells <em>Report Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Cells</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Report#getReportCells()
	 * @see #getReport()
	 * @generated
	 */
	EReference getReport_ReportCells();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ReportRow <em>Report Row</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Row</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportRow
	 * @generated
	 */
	EClass getReportRow();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ReportRow#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportRow#getName()
	 * @see #getReportRow()
	 * @generated
	 */
	EAttribute getReportRow_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ReportColumn <em>Report Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Column</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportColumn
	 * @generated
	 */
	EClass getReportColumn();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ReportColumn#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportColumn#getName()
	 * @see #getReportColumn()
	 * @generated
	 */
	EAttribute getReportColumn_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell <em>Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Cell</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportCell
	 * @generated
	 */
	EClass getReportCell();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getRow <em>Row</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Row</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportCell#getRow()
	 * @see #getReportCell()
	 * @generated
	 */
	EReference getReportCell_Row();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getColumn <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportCell#getColumn()
	 * @see #getReportCell()
	 * @generated
	 */
	EReference getReportCell_Column();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getFilters <em>Filters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Filters</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportCell#getFilters()
	 * @see #getReportCell()
	 * @generated
	 */
	EReference getReportCell_Filters();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getMetric <em>Metric</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Metric</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportCell#getMetric()
	 * @see #getReportCell()
	 * @generated
	 */
	EReference getReportCell_Metric();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.regdna.model.regdna.ReportCell#getDatapointID <em>Datapoint ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Datapoint ID</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportCell#getDatapointID()
	 * @see #getReportCell()
	 * @generated
	 */
	EAttribute getReportCell_DatapointID();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.Filter <em>Filter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Filter</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Filter
	 * @generated
	 */
	EClass getFilter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.Filter#getOutputLayer <em>Output Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Output Layer</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Filter#getOutputLayer()
	 * @see #getFilter()
	 * @generated
	 */
	EReference getFilter_OutputLayer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.regdna.model.regdna.Filter#getOperation <em>Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Operation</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Filter#getOperation()
	 * @see #getFilter()
	 * @generated
	 */
	EReference getFilter_Operation();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.regdna.model.regdna.Filter#getMember <em>Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Member</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Filter#getMember()
	 * @see #getFilter()
	 * @generated
	 */
	EReference getFilter_Member();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.regdna.model.regdna.ReportModule <em>Report Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Module</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportModule
	 * @generated
	 */
	EClass getReportModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.regdna.model.regdna.ReportModule#getReports <em>Reports</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Reports</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.ReportModule#getReports()
	 * @see #getReportModule()
	 * @generated
	 */
	EReference getReportModule_Reports();

	/**
	 * Returns the meta object for enum '{@link org.eclipse.efbt.regdna.model.regdna.Comparitor <em>Comparitor</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Comparitor</em>'.
	 * @see org.eclipse.efbt.regdna.model.regdna.Comparitor
	 * @generated
	 */
	EEnum getComparitor();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	regdnaFactory getregdnaFactory();

} //regdnaPackage
