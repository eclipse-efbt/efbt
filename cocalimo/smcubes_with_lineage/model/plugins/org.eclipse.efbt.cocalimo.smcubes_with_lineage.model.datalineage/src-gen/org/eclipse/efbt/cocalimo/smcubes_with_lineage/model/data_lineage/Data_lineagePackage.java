/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage;

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
 *  This package describes the DataLineageModel, DataLineageModels store the actaul lineage between calculated results, and input data, with lineage recorded between all the intermediate datasets which may have been calculated to get to the final results.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.Data_lineageFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Data_lineagePackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "data_lineage";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/data_lineage";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "data_lineage";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_lineagePackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.Data_lineagePackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.DataLineageModelImpl <em>Data Lineage Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.DataLineageModelImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.Data_lineagePackageImpl#getDataLineageModel()
	 * @generated
	 */
	int DATA_LINEAGE_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Data Traceable By Cell</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_LINEAGE_MODEL__DATA_TRACEABLE_BY_CELL = 0;

	/**
	 * The feature id for the '<em><b>Source Data</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_LINEAGE_MODEL__SOURCE_DATA = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_LINEAGE_MODEL__NAME = 2;

	/**
	 * The number of structural features of the '<em>Data Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_LINEAGE_MODEL_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Data Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_LINEAGE_MODEL_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel <em>Data Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Data Lineage Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel
	 * @generated
	 */
	EClass getDataLineageModel();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel#getDataTraceableByCell <em>Data Traceable By Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Data Traceable By Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel#getDataTraceableByCell()
	 * @see #getDataLineageModel()
	 * @generated
	 */
	EReference getDataLineageModel_DataTraceableByCell();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel#getSourceData <em>Source Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Source Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel#getSourceData()
	 * @see #getDataLineageModel()
	 * @generated
	 */
	EReference getDataLineageModel_SourceData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.DataLineageModel#getName()
	 * @see #getDataLineageModel()
	 * @generated
	 */
	EAttribute getDataLineageModel_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Data_lineageFactory getData_lineageFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.DataLineageModelImpl <em>Data Lineage Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.DataLineageModelImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.Data_lineagePackageImpl#getDataLineageModel()
		 * @generated
		 */
		EClass DATA_LINEAGE_MODEL = eINSTANCE.getDataLineageModel();

		/**
		 * The meta object literal for the '<em><b>Data Traceable By Cell</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DATA_LINEAGE_MODEL__DATA_TRACEABLE_BY_CELL = eINSTANCE.getDataLineageModel_DataTraceableByCell();

		/**
		 * The meta object literal for the '<em><b>Source Data</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DATA_LINEAGE_MODEL__SOURCE_DATA = eINSTANCE.getDataLineageModel_SourceData();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DATA_LINEAGE_MODEL__NAME = eINSTANCE.getDataLineageModel_Name();

	}

} //Data_lineagePackage
