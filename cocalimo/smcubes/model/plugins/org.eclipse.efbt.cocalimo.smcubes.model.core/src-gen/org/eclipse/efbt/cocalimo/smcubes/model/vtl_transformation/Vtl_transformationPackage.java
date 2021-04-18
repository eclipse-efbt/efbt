/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;

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
 * This package describes the transformation package from SMCubes, using fields from SDD, see ECB documentation for details
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the data definition package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html'"
 * @generated
 */
public interface Vtl_transformationPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "vtl_transformation";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/vtl_transformation";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "vtl_transformation";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Vtl_transformationPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.Vtl_transformationPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl <em>TRANSFORMATION SCHEME</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.Vtl_transformationPackageImpl#getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	int TRANSFORMATION_SCHEME = 0;

	/**
	 * The feature id for the '<em><b>Transformation scheme id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__NAME = 2;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__CODE = 3;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__VERSION = 4;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__VALID_TO = 5;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__VALID_FROM = 6;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME__DISPLAY_NAME = 7;

	/**
	 * The number of structural features of the '<em>TRANSFORMATION SCHEME</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>TRANSFORMATION SCHEME</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_SCHEME_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl <em>TRANSFORMATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.Vtl_transformationPackageImpl#getTRANSFORMATION()
	 * @generated
	 */
	int TRANSFORMATION = 1;

	/**
	 * The feature id for the '<em><b>Transformation id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION__TRANSFORMATION_ID = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION__EXPRESSION = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION__NAME = 3;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION__CODE = 4;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION__MAINTENANCE_AGENCY_ID = 5;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION__DISPLAY_NAME = 6;

	/**
	 * The number of structural features of the '<em>TRANSFORMATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>TRANSFORMATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSFORMATION_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME <em>TRANSFORMATION SCHEME</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>TRANSFORMATION SCHEME</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME
	 * @generated
	 */
	EClass getTRANSFORMATION_SCHEME();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getTransformation_scheme_id <em>Transformation scheme id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Transformation scheme id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getTransformation_scheme_id()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_Transformation_scheme_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getDescription()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getName()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getCode()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getVersion()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_Version();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getValid_to()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_Valid_to();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getValid_from()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME#getDisplayName()
	 * @see #getTRANSFORMATION_SCHEME()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_SCHEME_DisplayName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION <em>TRANSFORMATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>TRANSFORMATION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION
	 * @generated
	 */
	EClass getTRANSFORMATION();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getTransformation_id <em>Transformation id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Transformation id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getTransformation_id()
	 * @see #getTRANSFORMATION()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_Transformation_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getDescription()
	 * @see #getTRANSFORMATION()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Expression</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getExpression()
	 * @see #getTRANSFORMATION()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_Expression();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getName()
	 * @see #getTRANSFORMATION()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getCode()
	 * @see #getTRANSFORMATION()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getMaintenance_agency_id()
	 * @see #getTRANSFORMATION()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION#getDisplayName()
	 * @see #getTRANSFORMATION()
	 * @generated
	 */
	EAttribute getTRANSFORMATION_DisplayName();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Vtl_transformationFactory getVtl_transformationFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl <em>TRANSFORMATION SCHEME</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.Vtl_transformationPackageImpl#getTRANSFORMATION_SCHEME()
		 * @generated
		 */
		EClass TRANSFORMATION_SCHEME = eINSTANCE.getTRANSFORMATION_SCHEME();

		/**
		 * The meta object literal for the '<em><b>Transformation scheme id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID = eINSTANCE.getTRANSFORMATION_SCHEME_Transformation_scheme_id();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__DESCRIPTION = eINSTANCE.getTRANSFORMATION_SCHEME_Description();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__NAME = eINSTANCE.getTRANSFORMATION_SCHEME_Name();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__CODE = eINSTANCE.getTRANSFORMATION_SCHEME_Code();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__VERSION = eINSTANCE.getTRANSFORMATION_SCHEME_Version();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__VALID_TO = eINSTANCE.getTRANSFORMATION_SCHEME_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__VALID_FROM = eINSTANCE.getTRANSFORMATION_SCHEME_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION_SCHEME__DISPLAY_NAME = eINSTANCE.getTRANSFORMATION_SCHEME_DisplayName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl <em>TRANSFORMATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.Vtl_transformationPackageImpl#getTRANSFORMATION()
		 * @generated
		 */
		EClass TRANSFORMATION = eINSTANCE.getTRANSFORMATION();

		/**
		 * The meta object literal for the '<em><b>Transformation id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION__TRANSFORMATION_ID = eINSTANCE.getTRANSFORMATION_Transformation_id();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION__DESCRIPTION = eINSTANCE.getTRANSFORMATION_Description();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION__EXPRESSION = eINSTANCE.getTRANSFORMATION_Expression();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION__NAME = eINSTANCE.getTRANSFORMATION_Name();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION__CODE = eINSTANCE.getTRANSFORMATION_Code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION__MAINTENANCE_AGENCY_ID = eINSTANCE.getTRANSFORMATION_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSFORMATION__DISPLAY_NAME = eINSTANCE.getTRANSFORMATION_DisplayName();

	}

} //Vtl_transformationPackage
