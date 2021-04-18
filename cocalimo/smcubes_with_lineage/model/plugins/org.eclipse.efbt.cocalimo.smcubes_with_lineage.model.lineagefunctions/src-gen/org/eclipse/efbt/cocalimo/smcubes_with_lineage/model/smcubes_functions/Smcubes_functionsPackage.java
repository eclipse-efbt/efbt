/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

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
 * This Package Functions that Specifically refer to SMCubes types (i.e. MEMBER and FACET_VALUE_TYPE)
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.Smcubes_functionsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Smcubes_functionsPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "smcubes_functions";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/smcubes_functions";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "smcubes_functions";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Smcubes_functionsPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.Smcubes_functionsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.MemberParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.Smcubes_functionsPackageImpl#getMemberParameter()
	 * @generated
	 */
	int MEMBER_PARAMETER = 0;

	/**
	 * The feature id for the '<em><b>Param</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__PARAM = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.ValueParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.Smcubes_functionsPackageImpl#getValueParameter()
	 * @generated
	 */
	int VALUE_PARAMETER = 1;

	/**
	 * The feature id for the '<em><b>Data Type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__DATA_TYPE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__VALUE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.MemberParameter <em>Member Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.MemberParameter
	 * @generated
	 */
	EClass getMemberParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.MemberParameter#getParam <em>Param</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Param</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.MemberParameter#getParam()
	 * @see #getMemberParameter()
	 * @generated
	 */
	EReference getMemberParameter_Param();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.ValueParameter <em>Value Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Value Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.ValueParameter
	 * @generated
	 */
	EClass getValueParameter();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.ValueParameter#getDataType <em>Data Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Data Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.ValueParameter#getDataType()
	 * @see #getValueParameter()
	 * @generated
	 */
	EAttribute getValueParameter_DataType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.ValueParameter#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.ValueParameter#getValue()
	 * @see #getValueParameter()
	 * @generated
	 */
	EAttribute getValueParameter_Value();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Smcubes_functionsFactory getSmcubes_functionsFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.MemberParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.Smcubes_functionsPackageImpl#getMemberParameter()
		 * @generated
		 */
		EClass MEMBER_PARAMETER = eINSTANCE.getMemberParameter();

		/**
		 * The meta object literal for the '<em><b>Param</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_PARAMETER__PARAM = eINSTANCE.getMemberParameter_Param();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.ValueParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.Smcubes_functionsPackageImpl#getValueParameter()
		 * @generated
		 */
		EClass VALUE_PARAMETER = eINSTANCE.getValueParameter();

		/**
		 * The meta object literal for the '<em><b>Data Type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VALUE_PARAMETER__DATA_TYPE = eINSTANCE.getValueParameter_DataType();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VALUE_PARAMETER__VALUE = eINSTANCE.getValueParameter_Value();

	}

} //Smcubes_functionsPackage
