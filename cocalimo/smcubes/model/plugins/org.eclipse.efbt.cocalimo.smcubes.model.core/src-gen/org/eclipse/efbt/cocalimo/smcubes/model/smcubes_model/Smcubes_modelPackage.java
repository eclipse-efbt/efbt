/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model;

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
 *  This package describes a class which composes the diferent modules, whereby each of those modules composes  a list of items from the ECBS SMCubes model, which we represent as the core package and the data_definition package. 
 *  
 *  
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Smcubes_modelPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "smcubes_model";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/smcubes_model";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "smcubes_model";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Smcubes_modelPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.Smcubes_modelPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl <em>Smcubes Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.Smcubes_modelPackageImpl#getSmcubesModel()
	 * @generated
	 */
	int SMCUBES_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Domains</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL__DOMAINS = 0;

	/**
	 * The feature id for the '<em><b>Members</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL__MEMBERS = 1;

	/**
	 * The feature id for the '<em><b>Variables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL__VARIABLES = 2;

	/**
	 * The feature id for the '<em><b>Cubes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL__CUBES = 3;

	/**
	 * The feature id for the '<em><b>Combinations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL__COMBINATIONS = 4;

	/**
	 * The feature id for the '<em><b>Mappings</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL__MAPPINGS = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL__NAME = 6;

	/**
	 * The number of structural features of the '<em>Smcubes Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>Smcubes Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_MODEL_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel <em>Smcubes Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Smcubes Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel
	 * @generated
	 */
	EClass getSmcubesModel();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getDomains <em>Domains</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Domains</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getDomains()
	 * @see #getSmcubesModel()
	 * @generated
	 */
	EReference getSmcubesModel_Domains();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getMembers <em>Members</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Members</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getMembers()
	 * @see #getSmcubesModel()
	 * @generated
	 */
	EReference getSmcubesModel_Members();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getVariables <em>Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Variables</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getVariables()
	 * @see #getSmcubesModel()
	 * @generated
	 */
	EReference getSmcubesModel_Variables();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getCubes <em>Cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cubes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getCubes()
	 * @see #getSmcubesModel()
	 * @generated
	 */
	EReference getSmcubesModel_Cubes();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getCombinations <em>Combinations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Combinations</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getCombinations()
	 * @see #getSmcubesModel()
	 * @generated
	 */
	EReference getSmcubesModel_Combinations();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getMappings <em>Mappings</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Mappings</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getMappings()
	 * @see #getSmcubesModel()
	 * @generated
	 */
	EReference getSmcubesModel_Mappings();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel#getName()
	 * @see #getSmcubesModel()
	 * @generated
	 */
	EAttribute getSmcubesModel_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Smcubes_modelFactory getSmcubes_modelFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl <em>Smcubes Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.Smcubes_modelPackageImpl#getSmcubesModel()
		 * @generated
		 */
		EClass SMCUBES_MODEL = eINSTANCE.getSmcubesModel();

		/**
		 * The meta object literal for the '<em><b>Domains</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SMCUBES_MODEL__DOMAINS = eINSTANCE.getSmcubesModel_Domains();

		/**
		 * The meta object literal for the '<em><b>Members</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SMCUBES_MODEL__MEMBERS = eINSTANCE.getSmcubesModel_Members();

		/**
		 * The meta object literal for the '<em><b>Variables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SMCUBES_MODEL__VARIABLES = eINSTANCE.getSmcubesModel_Variables();

		/**
		 * The meta object literal for the '<em><b>Cubes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SMCUBES_MODEL__CUBES = eINSTANCE.getSmcubesModel_Cubes();

		/**
		 * The meta object literal for the '<em><b>Combinations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SMCUBES_MODEL__COMBINATIONS = eINSTANCE.getSmcubesModel_Combinations();

		/**
		 * The meta object literal for the '<em><b>Mappings</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SMCUBES_MODEL__MAPPINGS = eINSTANCE.getSmcubesModel_Mappings();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SMCUBES_MODEL__NAME = eINSTANCE.getSmcubesModel_Name();

	}

} //Smcubes_modelPackage
