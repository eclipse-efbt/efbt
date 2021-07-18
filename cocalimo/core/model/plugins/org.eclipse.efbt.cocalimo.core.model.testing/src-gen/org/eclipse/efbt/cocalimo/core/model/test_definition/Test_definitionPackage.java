/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

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
 * This package describes how we describe tests and groups of tests.
 * Test Definitions are defined using given , when , and then  clauses as used in the popular Gherkin language for defining tests according to the Behaviour Driven Developement (BDD) approach.
 * We can have multiple tests with different test data which are linked to the same test definition.
 * In order to group tests together, we parameterize the clauses of test definitions, this is also popular in BDD.
 * In order to provide well defined coverage, we constrain how the parameters can be set to a particular set of values, the way we constrain this is by defineing in constraints what the 'parent object' of a parameter is. 
 * By having a contrained list of choices for a parameter, we can create a coverage grid of the set of possible combined  choices for the parameters of a test definition.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Test_definitionPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "test_definition";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/test_definition";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "test_definition";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Test_definitionPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl <em>BDD Test Contraints</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestContraints()
	 * @generated
	 */
	int BDD_TEST_CONTRAINTS = 0;

	/**
	 * The feature id for the '<em><b>Template</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONTRAINTS__TEMPLATE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONTRAINTS__NAME = 1;

	/**
	 * The feature id for the '<em><b>Given Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONTRAINTS__GIVEN_PARAMS = 2;

	/**
	 * The feature id for the '<em><b>When Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONTRAINTS__WHEN_PARAMS = 3;

	/**
	 * The feature id for the '<em><b>Then Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONTRAINTS__THEN_PARAMS = 4;

	/**
	 * The number of structural features of the '<em>BDD Test Contraints</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONTRAINTS_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>BDD Test Contraints</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONTRAINTS_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateModuleImpl <em>BDD Test Template Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestTemplateModule()
	 * @generated
	 */
	int BDD_TEST_TEMPLATE_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Templates</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE__TEMPLATES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>BDD Test Template Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>BDD Test Template Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl <em>BDD Test Template</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestTemplate()
	 * @generated
	 */
	int BDD_TEST_TEMPLATE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE__NAME = 0;

	/**
	 * The feature id for the '<em><b>Given Text</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE__GIVEN_TEXT = 1;

	/**
	 * The feature id for the '<em><b>When Text</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE__WHEN_TEXT = 2;

	/**
	 * The feature id for the '<em><b>Then Text</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE__THEN_TEXT = 3;

	/**
	 * The feature id for the '<em><b>Given Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE__GIVEN_PARAMS = 4;

	/**
	 * The feature id for the '<em><b>When Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE__WHEN_PARAMS = 5;

	/**
	 * The feature id for the '<em><b>Then Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE__THEN_PARAMS = 6;

	/**
	 * The number of structural features of the '<em>BDD Test Template</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>BDD Test Template</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_TEMPLATE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestConstraintsModuleImpl <em>BDD Test Constraints Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestConstraintsModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestConstraintsModule()
	 * @generated
	 */
	int BDD_TEST_CONSTRAINTS_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Coverage Test Sets</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>BDD Test Constraints Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>BDD Test Constraints Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_CONSTRAINTS_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.TestDefinitionImpl <em>Test Definition</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.TestDefinitionImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getTestDefinition()
	 * @generated
	 */
	int TEST_DEFINITION = 16;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION__NAME = 0;

	/**
	 * The number of structural features of the '<em>Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl <em>BDD Test Definition</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestDefinition()
	 * @generated
	 */
	int BDD_TEST_DEFINITION = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION__NAME = TEST_DEFINITION__NAME;

	/**
	 * The feature id for the '<em><b>Test Contraints</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION__TEST_CONTRAINTS = TEST_DEFINITION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Given</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION__GIVEN = TEST_DEFINITION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>When</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION__WHEN = TEST_DEFINITION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Then</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION__THEN = TEST_DEFINITION_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>BDD Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_FEATURE_COUNT = TEST_DEFINITION_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>BDD Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_OPERATION_COUNT = TEST_DEFINITION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.E2EBDDTestDefinitionImpl <em>E2EBDD Test Definition</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.E2EBDDTestDefinitionImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getE2EBDDTestDefinition()
	 * @generated
	 */
	int E2EBDD_TEST_DEFINITION = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2EBDD_TEST_DEFINITION__NAME = BDD_TEST_DEFINITION__NAME;

	/**
	 * The feature id for the '<em><b>Test Contraints</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2EBDD_TEST_DEFINITION__TEST_CONTRAINTS = BDD_TEST_DEFINITION__TEST_CONTRAINTS;

	/**
	 * The feature id for the '<em><b>Given</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2EBDD_TEST_DEFINITION__GIVEN = BDD_TEST_DEFINITION__GIVEN;

	/**
	 * The feature id for the '<em><b>When</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2EBDD_TEST_DEFINITION__WHEN = BDD_TEST_DEFINITION__WHEN;

	/**
	 * The feature id for the '<em><b>Then</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2EBDD_TEST_DEFINITION__THEN = BDD_TEST_DEFINITION__THEN;

	/**
	 * The number of structural features of the '<em>E2EBDD Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2EBDD_TEST_DEFINITION_FEATURE_COUNT = BDD_TEST_DEFINITION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>E2EBDD Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2EBDD_TEST_DEFINITION_OPERATION_COUNT = BDD_TEST_DEFINITION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.UnitTestDefinitionImpl <em>Unit Test Definition</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.UnitTestDefinitionImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getUnitTestDefinition()
	 * @generated
	 */
	int UNIT_TEST_DEFINITION = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_DEFINITION__NAME = BDD_TEST_DEFINITION__NAME;

	/**
	 * The feature id for the '<em><b>Test Contraints</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_DEFINITION__TEST_CONTRAINTS = BDD_TEST_DEFINITION__TEST_CONTRAINTS;

	/**
	 * The feature id for the '<em><b>Given</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_DEFINITION__GIVEN = BDD_TEST_DEFINITION__GIVEN;

	/**
	 * The feature id for the '<em><b>When</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_DEFINITION__WHEN = BDD_TEST_DEFINITION__WHEN;

	/**
	 * The feature id for the '<em><b>Then</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_DEFINITION__THEN = BDD_TEST_DEFINITION__THEN;

	/**
	 * The number of structural features of the '<em>Unit Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_DEFINITION_FEATURE_COUNT = BDD_TEST_DEFINITION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Unit Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_DEFINITION_OPERATION_COUNT = BDD_TEST_DEFINITION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseImpl <em>Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getClause()
	 * @generated
	 */
	int CLAUSE = 7;

	/**
	 * The feature id for the '<em><b>Text</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE__TEXT = 0;

	/**
	 * The feature id for the '<em><b>Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE__PARAMS = 1;

	/**
	 * The feature id for the '<em><b>Regulatory Text Links</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE__REGULATORY_TEXT_LINKS = 2;

	/**
	 * The number of structural features of the '<em>Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ParamImpl <em>Param</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ParamImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getParam()
	 * @generated
	 */
	int PARAM = 8;

	/**
	 * The feature id for the '<em><b>Param</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAM__PARAM = 0;

	/**
	 * The number of structural features of the '<em>Param</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAM_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Param</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAM_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.GivenImpl <em>Given</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.GivenImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getGiven()
	 * @generated
	 */
	int GIVEN = 9;

	/**
	 * The feature id for the '<em><b>Text</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GIVEN__TEXT = CLAUSE__TEXT;

	/**
	 * The feature id for the '<em><b>Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GIVEN__PARAMS = CLAUSE__PARAMS;

	/**
	 * The feature id for the '<em><b>Regulatory Text Links</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GIVEN__REGULATORY_TEXT_LINKS = CLAUSE__REGULATORY_TEXT_LINKS;

	/**
	 * The number of structural features of the '<em>Given</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GIVEN_FEATURE_COUNT = CLAUSE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Given</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GIVEN_OPERATION_COUNT = CLAUSE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.WhenImpl <em>When</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.WhenImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getWhen()
	 * @generated
	 */
	int WHEN = 10;

	/**
	 * The feature id for the '<em><b>Text</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHEN__TEXT = CLAUSE__TEXT;

	/**
	 * The feature id for the '<em><b>Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHEN__PARAMS = CLAUSE__PARAMS;

	/**
	 * The feature id for the '<em><b>Regulatory Text Links</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHEN__REGULATORY_TEXT_LINKS = CLAUSE__REGULATORY_TEXT_LINKS;

	/**
	 * The number of structural features of the '<em>When</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHEN_FEATURE_COUNT = CLAUSE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>When</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHEN_OPERATION_COUNT = CLAUSE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ThenImpl <em>Then</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ThenImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getThen()
	 * @generated
	 */
	int THEN = 11;

	/**
	 * The feature id for the '<em><b>Text</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THEN__TEXT = CLAUSE__TEXT;

	/**
	 * The feature id for the '<em><b>Params</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THEN__PARAMS = CLAUSE__PARAMS;

	/**
	 * The feature id for the '<em><b>Regulatory Text Links</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THEN__REGULATORY_TEXT_LINKS = CLAUSE__REGULATORY_TEXT_LINKS;

	/**
	 * The number of structural features of the '<em>Then</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THEN_FEATURE_COUNT = CLAUSE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Then</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THEN_OPERATION_COUNT = CLAUSE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseTextImpl <em>Clause Text</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseTextImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getClauseText()
	 * @generated
	 */
	int CLAUSE_TEXT = 12;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE_TEXT__NAME = 0;

	/**
	 * The number of structural features of the '<em>Clause Text</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE_TEXT_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Clause Text</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLAUSE_TEXT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridImpl <em>Coverage Grid</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getCoverageGrid()
	 * @generated
	 */
	int COVERAGE_GRID = 13;

	/**
	 * The feature id for the '<em><b>Coverage Test Set</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID__COVERAGE_TEST_SET = 0;

	/**
	 * The feature id for the '<em><b>Coverage Grid Points</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID__COVERAGE_GRID_POINTS = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID__NAME = 2;

	/**
	 * The number of structural features of the '<em>Coverage Grid</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Coverage Grid</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridPointImpl <em>Coverage Grid Point</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridPointImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getCoverageGridPoint()
	 * @generated
	 */
	int COVERAGE_GRID_POINT = 14;

	/**
	 * The feature id for the '<em><b>Count</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID_POINT__COUNT = 0;

	/**
	 * The feature id for the '<em><b>Objects</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID_POINT__OBJECTS = 1;

	/**
	 * The number of structural features of the '<em>Coverage Grid Point</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID_POINT_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Coverage Grid Point</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COVERAGE_GRID_POINT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionModuleImpl <em>BDD Test Definition Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestDefinitionModule()
	 * @generated
	 */
	int BDD_TEST_DEFINITION_MODULE = 15;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Test Definitions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE__TEST_DEFINITIONS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>BDD Test Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>BDD Test Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_DEFINITION_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionImpl <em>Reg Functionality Test Definition</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getRegFunctionalityTestDefinition()
	 * @generated
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION = 17;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION__NAME = TEST_DEFINITION__NAME;

	/**
	 * The feature id for the '<em><b>Input Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION__INPUT_MODULES = TEST_DEFINITION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Processing Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION__PROCESSING_MODULES = TEST_DEFINITION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Report Cell Creation Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION__REPORT_CELL_CREATION_MODULES = TEST_DEFINITION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Scenarios</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION__SCENARIOS = TEST_DEFINITION_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Picture</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION__PICTURE = TEST_DEFINITION_FEATURE_COUNT + 4;

	/**
	 * The number of structural features of the '<em>Reg Functionality Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_FEATURE_COUNT = TEST_DEFINITION_FEATURE_COUNT + 5;

	/**
	 * The number of operations of the '<em>Reg Functionality Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_OPERATION_COUNT = TEST_DEFINITION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionModuleImpl <em>Reg Functionality Test Definition Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getRegFunctionalityTestDefinitionModule()
	 * @generated
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE = 18;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Test Definitions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Reg Functionality Test Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Reg Functionality Test Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_DEFINITION_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints <em>BDD Test Contraints</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BDD Test Contraints</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints
	 * @generated
	 */
	EClass getBDDTestContraints();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getTemplate <em>Template</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Template</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getTemplate()
	 * @see #getBDDTestContraints()
	 * @generated
	 */
	EReference getBDDTestContraints_Template();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getName()
	 * @see #getBDDTestContraints()
	 * @generated
	 */
	EAttribute getBDDTestContraints_Name();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getGivenParams <em>Given Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Given Params</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getGivenParams()
	 * @see #getBDDTestContraints()
	 * @generated
	 */
	EReference getBDDTestContraints_GivenParams();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getWhenParams <em>When Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>When Params</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getWhenParams()
	 * @see #getBDDTestContraints()
	 * @generated
	 */
	EReference getBDDTestContraints_WhenParams();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getThenParams <em>Then Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Then Params</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints#getThenParams()
	 * @see #getBDDTestContraints()
	 * @generated
	 */
	EReference getBDDTestContraints_ThenParams();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule <em>BDD Test Template Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BDD Test Template Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule
	 * @generated
	 */
	EClass getBDDTestTemplateModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule#getTemplates <em>Templates</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Templates</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule#getTemplates()
	 * @see #getBDDTestTemplateModule()
	 * @generated
	 */
	EReference getBDDTestTemplateModule_Templates();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate <em>BDD Test Template</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BDD Test Template</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate
	 * @generated
	 */
	EClass getBDDTestTemplate();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getName()
	 * @see #getBDDTestTemplate()
	 * @generated
	 */
	EAttribute getBDDTestTemplate_Name();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getGivenText <em>Given Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Given Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getGivenText()
	 * @see #getBDDTestTemplate()
	 * @generated
	 */
	EReference getBDDTestTemplate_GivenText();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getWhenText <em>When Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>When Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getWhenText()
	 * @see #getBDDTestTemplate()
	 * @generated
	 */
	EReference getBDDTestTemplate_WhenText();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getThenText <em>Then Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Then Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getThenText()
	 * @see #getBDDTestTemplate()
	 * @generated
	 */
	EReference getBDDTestTemplate_ThenText();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getGivenParams <em>Given Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Given Params</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getGivenParams()
	 * @see #getBDDTestTemplate()
	 * @generated
	 */
	EReference getBDDTestTemplate_GivenParams();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getWhenParams <em>When Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>When Params</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getWhenParams()
	 * @see #getBDDTestTemplate()
	 * @generated
	 */
	EReference getBDDTestTemplate_WhenParams();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getThenParams <em>Then Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Then Params</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getThenParams()
	 * @see #getBDDTestTemplate()
	 * @generated
	 */
	EReference getBDDTestTemplate_ThenParams();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule <em>BDD Test Constraints Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BDD Test Constraints Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule
	 * @generated
	 */
	EClass getBDDTestConstraintsModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule#getCoverageTestSets <em>Coverage Test Sets</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Coverage Test Sets</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule#getCoverageTestSets()
	 * @see #getBDDTestConstraintsModule()
	 * @generated
	 */
	EReference getBDDTestConstraintsModule_CoverageTestSets();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition <em>BDD Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BDD Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition
	 * @generated
	 */
	EClass getBDDTestDefinition();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getTestContraints <em>Test Contraints</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Test Contraints</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getTestContraints()
	 * @see #getBDDTestDefinition()
	 * @generated
	 */
	EReference getBDDTestDefinition_TestContraints();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getGiven <em>Given</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Given</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getGiven()
	 * @see #getBDDTestDefinition()
	 * @generated
	 */
	EReference getBDDTestDefinition_Given();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getWhen <em>When</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>When</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getWhen()
	 * @see #getBDDTestDefinition()
	 * @generated
	 */
	EReference getBDDTestDefinition_When();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getThen <em>Then</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Then</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition#getThen()
	 * @see #getBDDTestDefinition()
	 * @generated
	 */
	EReference getBDDTestDefinition_Then();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.E2EBDDTestDefinition <em>E2EBDD Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>E2EBDD Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.E2EBDDTestDefinition
	 * @generated
	 */
	EClass getE2EBDDTestDefinition();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.UnitTestDefinition <em>Unit Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unit Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.UnitTestDefinition
	 * @generated
	 */
	EClass getUnitTestDefinition();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause <em>Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Clause</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Clause
	 * @generated
	 */
	EClass getClause();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getText <em>Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getText()
	 * @see #getClause()
	 * @generated
	 */
	EReference getClause_Text();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getParams <em>Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Params</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getParams()
	 * @see #getClause()
	 * @generated
	 */
	EReference getClause_Params();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getRegulatoryTextLinks <em>Regulatory Text Links</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Regulatory Text Links</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getRegulatoryTextLinks()
	 * @see #getClause()
	 * @generated
	 */
	EReference getClause_RegulatoryTextLinks();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Param <em>Param</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Param</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Param
	 * @generated
	 */
	EClass getParam();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Param#getParam <em>Param</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Param</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Param#getParam()
	 * @see #getParam()
	 * @generated
	 */
	EReference getParam_Param();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Given <em>Given</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Given</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Given
	 * @generated
	 */
	EClass getGiven();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.When <em>When</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>When</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.When
	 * @generated
	 */
	EClass getWhen();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Then <em>Then</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Then</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Then
	 * @generated
	 */
	EClass getThen();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText <em>Clause Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Clause Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText
	 * @generated
	 */
	EClass getClauseText();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText#getName()
	 * @see #getClauseText()
	 * @generated
	 */
	EAttribute getClauseText_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid <em>Coverage Grid</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Coverage Grid</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid
	 * @generated
	 */
	EClass getCoverageGrid();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageTestSet <em>Coverage Test Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Coverage Test Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageTestSet()
	 * @see #getCoverageGrid()
	 * @generated
	 */
	EReference getCoverageGrid_CoverageTestSet();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageGridPoints <em>Coverage Grid Points</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Coverage Grid Points</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageGridPoints()
	 * @see #getCoverageGrid()
	 * @generated
	 */
	EReference getCoverageGrid_CoverageGridPoints();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getName()
	 * @see #getCoverageGrid()
	 * @generated
	 */
	EAttribute getCoverageGrid_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint <em>Coverage Grid Point</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Coverage Grid Point</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint
	 * @generated
	 */
	EClass getCoverageGridPoint();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint#getCount <em>Count</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Count</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint#getCount()
	 * @see #getCoverageGridPoint()
	 * @generated
	 */
	EAttribute getCoverageGridPoint_Count();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint#getObjects <em>Objects</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Objects</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint#getObjects()
	 * @see #getCoverageGridPoint()
	 * @generated
	 */
	EReference getCoverageGridPoint_Objects();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule <em>BDD Test Definition Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BDD Test Definition Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule
	 * @generated
	 */
	EClass getBDDTestDefinitionModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule#getTestDefinitions <em>Test Definitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Test Definitions</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule#getTestDefinitions()
	 * @see #getBDDTestDefinitionModule()
	 * @generated
	 */
	EReference getBDDTestDefinitionModule_TestDefinitions();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition <em>Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition
	 * @generated
	 */
	EClass getTestDefinition();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition#getName()
	 * @see #getTestDefinition()
	 * @generated
	 */
	EAttribute getTestDefinition_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition <em>Reg Functionality Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Reg Functionality Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition
	 * @generated
	 */
	EClass getRegFunctionalityTestDefinition();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getInputModules <em>Input Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Input Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getInputModules()
	 * @see #getRegFunctionalityTestDefinition()
	 * @generated
	 */
	EReference getRegFunctionalityTestDefinition_InputModules();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getProcessingModules <em>Processing Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Processing Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getProcessingModules()
	 * @see #getRegFunctionalityTestDefinition()
	 * @generated
	 */
	EReference getRegFunctionalityTestDefinition_ProcessingModules();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getReportCellCreationModules <em>Report Cell Creation Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Report Cell Creation Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getReportCellCreationModules()
	 * @see #getRegFunctionalityTestDefinition()
	 * @generated
	 */
	EReference getRegFunctionalityTestDefinition_ReportCellCreationModules();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getScenarios <em>Scenarios</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Scenarios</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getScenarios()
	 * @see #getRegFunctionalityTestDefinition()
	 * @generated
	 */
	EReference getRegFunctionalityTestDefinition_Scenarios();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getPicture <em>Picture</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Picture</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getPicture()
	 * @see #getRegFunctionalityTestDefinition()
	 * @generated
	 */
	EReference getRegFunctionalityTestDefinition_Picture();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule <em>Reg Functionality Test Definition Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Reg Functionality Test Definition Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule
	 * @generated
	 */
	EClass getRegFunctionalityTestDefinitionModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule#getTestDefinitions <em>Test Definitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Test Definitions</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule#getTestDefinitions()
	 * @see #getRegFunctionalityTestDefinitionModule()
	 * @generated
	 */
	EReference getRegFunctionalityTestDefinitionModule_TestDefinitions();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Test_definitionFactory getTest_definitionFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl <em>BDD Test Contraints</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestContraints()
		 * @generated
		 */
		EClass BDD_TEST_CONTRAINTS = eINSTANCE.getBDDTestContraints();

		/**
		 * The meta object literal for the '<em><b>Template</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_CONTRAINTS__TEMPLATE = eINSTANCE.getBDDTestContraints_Template();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute BDD_TEST_CONTRAINTS__NAME = eINSTANCE.getBDDTestContraints_Name();

		/**
		 * The meta object literal for the '<em><b>Given Params</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_CONTRAINTS__GIVEN_PARAMS = eINSTANCE.getBDDTestContraints_GivenParams();

		/**
		 * The meta object literal for the '<em><b>When Params</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_CONTRAINTS__WHEN_PARAMS = eINSTANCE.getBDDTestContraints_WhenParams();

		/**
		 * The meta object literal for the '<em><b>Then Params</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_CONTRAINTS__THEN_PARAMS = eINSTANCE.getBDDTestContraints_ThenParams();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateModuleImpl <em>BDD Test Template Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestTemplateModule()
		 * @generated
		 */
		EClass BDD_TEST_TEMPLATE_MODULE = eINSTANCE.getBDDTestTemplateModule();

		/**
		 * The meta object literal for the '<em><b>Templates</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_TEMPLATE_MODULE__TEMPLATES = eINSTANCE.getBDDTestTemplateModule_Templates();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl <em>BDD Test Template</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestTemplate()
		 * @generated
		 */
		EClass BDD_TEST_TEMPLATE = eINSTANCE.getBDDTestTemplate();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute BDD_TEST_TEMPLATE__NAME = eINSTANCE.getBDDTestTemplate_Name();

		/**
		 * The meta object literal for the '<em><b>Given Text</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_TEMPLATE__GIVEN_TEXT = eINSTANCE.getBDDTestTemplate_GivenText();

		/**
		 * The meta object literal for the '<em><b>When Text</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_TEMPLATE__WHEN_TEXT = eINSTANCE.getBDDTestTemplate_WhenText();

		/**
		 * The meta object literal for the '<em><b>Then Text</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_TEMPLATE__THEN_TEXT = eINSTANCE.getBDDTestTemplate_ThenText();

		/**
		 * The meta object literal for the '<em><b>Given Params</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_TEMPLATE__GIVEN_PARAMS = eINSTANCE.getBDDTestTemplate_GivenParams();

		/**
		 * The meta object literal for the '<em><b>When Params</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_TEMPLATE__WHEN_PARAMS = eINSTANCE.getBDDTestTemplate_WhenParams();

		/**
		 * The meta object literal for the '<em><b>Then Params</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_TEMPLATE__THEN_PARAMS = eINSTANCE.getBDDTestTemplate_ThenParams();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestConstraintsModuleImpl <em>BDD Test Constraints Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestConstraintsModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestConstraintsModule()
		 * @generated
		 */
		EClass BDD_TEST_CONSTRAINTS_MODULE = eINSTANCE.getBDDTestConstraintsModule();

		/**
		 * The meta object literal for the '<em><b>Coverage Test Sets</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS = eINSTANCE.getBDDTestConstraintsModule_CoverageTestSets();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl <em>BDD Test Definition</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestDefinition()
		 * @generated
		 */
		EClass BDD_TEST_DEFINITION = eINSTANCE.getBDDTestDefinition();

		/**
		 * The meta object literal for the '<em><b>Test Contraints</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_DEFINITION__TEST_CONTRAINTS = eINSTANCE.getBDDTestDefinition_TestContraints();

		/**
		 * The meta object literal for the '<em><b>Given</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_DEFINITION__GIVEN = eINSTANCE.getBDDTestDefinition_Given();

		/**
		 * The meta object literal for the '<em><b>When</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_DEFINITION__WHEN = eINSTANCE.getBDDTestDefinition_When();

		/**
		 * The meta object literal for the '<em><b>Then</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_DEFINITION__THEN = eINSTANCE.getBDDTestDefinition_Then();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.E2EBDDTestDefinitionImpl <em>E2EBDD Test Definition</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.E2EBDDTestDefinitionImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getE2EBDDTestDefinition()
		 * @generated
		 */
		EClass E2EBDD_TEST_DEFINITION = eINSTANCE.getE2EBDDTestDefinition();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.UnitTestDefinitionImpl <em>Unit Test Definition</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.UnitTestDefinitionImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getUnitTestDefinition()
		 * @generated
		 */
		EClass UNIT_TEST_DEFINITION = eINSTANCE.getUnitTestDefinition();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseImpl <em>Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getClause()
		 * @generated
		 */
		EClass CLAUSE = eINSTANCE.getClause();

		/**
		 * The meta object literal for the '<em><b>Text</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLAUSE__TEXT = eINSTANCE.getClause_Text();

		/**
		 * The meta object literal for the '<em><b>Params</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLAUSE__PARAMS = eINSTANCE.getClause_Params();

		/**
		 * The meta object literal for the '<em><b>Regulatory Text Links</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLAUSE__REGULATORY_TEXT_LINKS = eINSTANCE.getClause_RegulatoryTextLinks();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ParamImpl <em>Param</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ParamImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getParam()
		 * @generated
		 */
		EClass PARAM = eINSTANCE.getParam();

		/**
		 * The meta object literal for the '<em><b>Param</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PARAM__PARAM = eINSTANCE.getParam_Param();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.GivenImpl <em>Given</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.GivenImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getGiven()
		 * @generated
		 */
		EClass GIVEN = eINSTANCE.getGiven();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.WhenImpl <em>When</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.WhenImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getWhen()
		 * @generated
		 */
		EClass WHEN = eINSTANCE.getWhen();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ThenImpl <em>Then</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ThenImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getThen()
		 * @generated
		 */
		EClass THEN = eINSTANCE.getThen();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseTextImpl <em>Clause Text</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseTextImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getClauseText()
		 * @generated
		 */
		EClass CLAUSE_TEXT = eINSTANCE.getClauseText();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CLAUSE_TEXT__NAME = eINSTANCE.getClauseText_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridImpl <em>Coverage Grid</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getCoverageGrid()
		 * @generated
		 */
		EClass COVERAGE_GRID = eINSTANCE.getCoverageGrid();

		/**
		 * The meta object literal for the '<em><b>Coverage Test Set</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COVERAGE_GRID__COVERAGE_TEST_SET = eINSTANCE.getCoverageGrid_CoverageTestSet();

		/**
		 * The meta object literal for the '<em><b>Coverage Grid Points</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COVERAGE_GRID__COVERAGE_GRID_POINTS = eINSTANCE.getCoverageGrid_CoverageGridPoints();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COVERAGE_GRID__NAME = eINSTANCE.getCoverageGrid_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridPointImpl <em>Coverage Grid Point</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridPointImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getCoverageGridPoint()
		 * @generated
		 */
		EClass COVERAGE_GRID_POINT = eINSTANCE.getCoverageGridPoint();

		/**
		 * The meta object literal for the '<em><b>Count</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COVERAGE_GRID_POINT__COUNT = eINSTANCE.getCoverageGridPoint_Count();

		/**
		 * The meta object literal for the '<em><b>Objects</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COVERAGE_GRID_POINT__OBJECTS = eINSTANCE.getCoverageGridPoint_Objects();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionModuleImpl <em>BDD Test Definition Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getBDDTestDefinitionModule()
		 * @generated
		 */
		EClass BDD_TEST_DEFINITION_MODULE = eINSTANCE.getBDDTestDefinitionModule();

		/**
		 * The meta object literal for the '<em><b>Test Definitions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST_DEFINITION_MODULE__TEST_DEFINITIONS = eINSTANCE.getBDDTestDefinitionModule_TestDefinitions();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.TestDefinitionImpl <em>Test Definition</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.TestDefinitionImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getTestDefinition()
		 * @generated
		 */
		EClass TEST_DEFINITION = eINSTANCE.getTestDefinition();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST_DEFINITION__NAME = eINSTANCE.getTestDefinition_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionImpl <em>Reg Functionality Test Definition</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getRegFunctionalityTestDefinition()
		 * @generated
		 */
		EClass REG_FUNCTIONALITY_TEST_DEFINITION = eINSTANCE.getRegFunctionalityTestDefinition();

		/**
		 * The meta object literal for the '<em><b>Input Modules</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REG_FUNCTIONALITY_TEST_DEFINITION__INPUT_MODULES = eINSTANCE.getRegFunctionalityTestDefinition_InputModules();

		/**
		 * The meta object literal for the '<em><b>Processing Modules</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REG_FUNCTIONALITY_TEST_DEFINITION__PROCESSING_MODULES = eINSTANCE.getRegFunctionalityTestDefinition_ProcessingModules();

		/**
		 * The meta object literal for the '<em><b>Report Cell Creation Modules</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REG_FUNCTIONALITY_TEST_DEFINITION__REPORT_CELL_CREATION_MODULES = eINSTANCE.getRegFunctionalityTestDefinition_ReportCellCreationModules();

		/**
		 * The meta object literal for the '<em><b>Scenarios</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REG_FUNCTIONALITY_TEST_DEFINITION__SCENARIOS = eINSTANCE.getRegFunctionalityTestDefinition_Scenarios();

		/**
		 * The meta object literal for the '<em><b>Picture</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REG_FUNCTIONALITY_TEST_DEFINITION__PICTURE = eINSTANCE.getRegFunctionalityTestDefinition_Picture();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionModuleImpl <em>Reg Functionality Test Definition Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl#getRegFunctionalityTestDefinitionModule()
		 * @generated
		 */
		EClass REG_FUNCTIONALITY_TEST_DEFINITION_MODULE = eINSTANCE.getRegFunctionalityTestDefinitionModule();

		/**
		 * The meta object literal for the '<em><b>Test Definitions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS = eINSTANCE.getRegFunctionalityTestDefinitionModule_TestDefinitions();

	}

} //Test_definitionPackage
