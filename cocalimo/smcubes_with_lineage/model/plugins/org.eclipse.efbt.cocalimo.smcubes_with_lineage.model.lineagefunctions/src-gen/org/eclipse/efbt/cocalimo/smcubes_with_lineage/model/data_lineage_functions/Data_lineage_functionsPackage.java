/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

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
 * The package describes the CellsParameter
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.Data_lineage_functionsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Data_lineage_functionsPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "data_lineage_functions";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/data_lineage_functions";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "data_lineage_functions";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_lineage_functionsPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.Data_lineage_functionsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.CellsParameterImpl <em>Cells Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.CellsParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.Data_lineage_functionsPackageImpl#getCellsParameter()
	 * @generated
	 */
	int CELLS_PARAMETER = 0;

	/**
	 * The feature id for the '<em><b>Cells</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__CELLS = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Cells Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Cells Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter <em>Cells Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cells Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter
	 * @generated
	 */
	EClass getCellsParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter#getCells <em>Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.CellsParameter#getCells()
	 * @see #getCellsParameter()
	 * @generated
	 */
	EReference getCellsParameter_Cells();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Data_lineage_functionsFactory getData_lineage_functionsFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.CellsParameterImpl <em>Cells Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.CellsParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.impl.Data_lineage_functionsPackageImpl#getCellsParameter()
		 * @generated
		 */
		EClass CELLS_PARAMETER = eINSTANCE.getCellsParameter();

		/**
		 * The meta object literal for the '<em><b>Cells</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CELLS_PARAMETER__CELLS = eINSTANCE.getCellsParameter_Cells();

	}

} //Data_lineage_functionsPackage
