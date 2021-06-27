/**
 */
package org.eclipse.efbt.cocalimo.core.model.scenarios;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

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
 * This package describes Scenarios
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface ScenariosPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "scenarios";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/scenarios";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "scenarios";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	ScenariosPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioImpl <em>Scenario</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenario()
	 * @generated
	 */
	int SCENARIO = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO__NAME = 0;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO__USED_IN_SUBSET = 1;

	/**
	 * The number of structural features of the '<em>Scenario</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Scenario</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetImpl <em>Scenario Set</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenarioSet()
	 * @generated
	 */
	int SCENARIO_SET = 1;

	/**
	 * The feature id for the '<em><b>Scenarios</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET__SCENARIOS = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET__NAME = 1;

	/**
	 * The number of structural features of the '<em>Scenario Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Scenario Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioTagImpl <em>Scenario Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioTagImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenarioTag()
	 * @generated
	 */
	int SCENARIO_TAG = 2;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__DISPLAY_NAME = Requirements_textPackage.TAG__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__NAME = Requirements_textPackage.TAG__NAME;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__REQUIREMENTS = Requirements_textPackage.TAG__REQUIREMENTS;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__SCENARIO = Requirements_textPackage.TAG_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Scenario Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG_FEATURE_COUNT = Requirements_textPackage.TAG_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Scenario Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG_OPERATION_COUNT = Requirements_textPackage.TAG_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetTagImpl <em>Scenario Set Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetTagImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenarioSetTag()
	 * @generated
	 */
	int SCENARIO_SET_TAG = 3;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_TAG__DISPLAY_NAME = Requirements_textPackage.TAG__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_TAG__NAME = Requirements_textPackage.TAG__NAME;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_TAG__REQUIREMENTS = Requirements_textPackage.TAG__REQUIREMENTS;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_TAG__SCENARIO_SET = Requirements_textPackage.TAG_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Scenario Set Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_TAG_FEATURE_COUNT = Requirements_textPackage.TAG_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Scenario Set Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_TAG_OPERATION_COUNT = Requirements_textPackage.TAG_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario
	 * @generated
	 */
	EClass getScenario();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#getName()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#isUsedInSubset <em>Used In Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Used In Subset</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#isUsedInSubset()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_UsedInSubset();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet <em>Scenario Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet
	 * @generated
	 */
	EClass getScenarioSet();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet#getScenarios <em>Scenarios</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Scenarios</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet#getScenarios()
	 * @see #getScenarioSet()
	 * @generated
	 */
	EReference getScenarioSet_Scenarios();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet#getName()
	 * @see #getScenarioSet()
	 * @generated
	 */
	EAttribute getScenarioSet_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag <em>Scenario Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario Tag</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag
	 * @generated
	 */
	EClass getScenarioTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag#getScenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag#getScenario()
	 * @see #getScenarioTag()
	 * @generated
	 */
	EReference getScenarioTag_Scenario();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSetTag <em>Scenario Set Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario Set Tag</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSetTag
	 * @generated
	 */
	EClass getScenarioSetTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSetTag#getScenarioSet <em>Scenario Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenario Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSetTag#getScenarioSet()
	 * @see #getScenarioSetTag()
	 * @generated
	 */
	EReference getScenarioSetTag_ScenarioSet();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	ScenariosFactory getScenariosFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioImpl <em>Scenario</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenario()
		 * @generated
		 */
		EClass SCENARIO = eINSTANCE.getScenario();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SCENARIO__NAME = eINSTANCE.getScenario_Name();

		/**
		 * The meta object literal for the '<em><b>Used In Subset</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SCENARIO__USED_IN_SUBSET = eINSTANCE.getScenario_UsedInSubset();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetImpl <em>Scenario Set</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenarioSet()
		 * @generated
		 */
		EClass SCENARIO_SET = eINSTANCE.getScenarioSet();

		/**
		 * The meta object literal for the '<em><b>Scenarios</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SCENARIO_SET__SCENARIOS = eINSTANCE.getScenarioSet_Scenarios();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SCENARIO_SET__NAME = eINSTANCE.getScenarioSet_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioTagImpl <em>Scenario Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioTagImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenarioTag()
		 * @generated
		 */
		EClass SCENARIO_TAG = eINSTANCE.getScenarioTag();

		/**
		 * The meta object literal for the '<em><b>Scenario</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SCENARIO_TAG__SCENARIO = eINSTANCE.getScenarioTag_Scenario();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetTagImpl <em>Scenario Set Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetTagImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosPackageImpl#getScenarioSetTag()
		 * @generated
		 */
		EClass SCENARIO_SET_TAG = eINSTANCE.getScenarioSetTag();

		/**
		 * The meta object literal for the '<em><b>Scenario Set</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SCENARIO_SET_TAG__SCENARIO_SET = eINSTANCE.getScenarioSetTag_ScenarioSet();

	}

} //ScenariosPackage
