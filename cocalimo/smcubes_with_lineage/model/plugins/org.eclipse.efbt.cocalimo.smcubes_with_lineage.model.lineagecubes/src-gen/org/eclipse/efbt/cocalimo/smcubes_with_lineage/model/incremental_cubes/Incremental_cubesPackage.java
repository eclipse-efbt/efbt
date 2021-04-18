/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

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
 *  This package describes the incremental FreeBirdToolsCubes , these assist in lineage of incremental processing.
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r\r\nCopyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Incremental_cubesPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "incremental_cubes";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/incremental_cubes";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "incremental_cubes";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Incremental_cubesPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseDeltaCubeImpl <em>Base Delta Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseDeltaCubeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl#getBaseDeltaCube()
	 * @generated
	 */
	int BASE_DELTA_CUBE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE__NAME = CubesPackage.BASE_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Ultimate Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE = CubesPackage.BASE_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base Delta Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE_FEATURE_COUNT = CubesPackage.BASE_CUBE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base Delta Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE_OPERATION_COUNT = CubesPackage.BASE_CUBE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseViewCubeImpl <em>Base View Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseViewCubeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl#getBaseViewCube()
	 * @generated
	 */
	int BASE_VIEW_CUBE = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE__NAME = CubesPackage.DERIVED_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Source Cubes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE__SOURCE_CUBES = CubesPackage.DERIVED_CUBE__SOURCE_CUBES;

	/**
	 * The feature id for the '<em><b>Ultimate Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE__ULTIMATE_BASE_CUBE = CubesPackage.DERIVED_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base View Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE_FEATURE_COUNT = CubesPackage.DERIVED_CUBE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base View Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE_OPERATION_COUNT = CubesPackage.DERIVED_CUBE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.DeltaAccumulationCubeImpl <em>Delta Accumulation Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.DeltaAccumulationCubeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl#getDeltaAccumulationCube()
	 * @generated
	 */
	int DELTA_ACCUMULATION_CUBE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE__NAME = CubesPackage.DERIVED_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Source Cubes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE__SOURCE_CUBES = CubesPackage.DERIVED_CUBE__SOURCE_CUBES;

	/**
	 * The number of structural features of the '<em>Delta Accumulation Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE_FEATURE_COUNT = CubesPackage.DERIVED_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Delta Accumulation Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE_OPERATION_COUNT = CubesPackage.DERIVED_CUBE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube <em>Base Delta Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Delta Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube
	 * @generated
	 */
	EClass getBaseDeltaCube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Ultimate Base Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube#getUltimateBaseCube()
	 * @see #getBaseDeltaCube()
	 * @generated
	 */
	EReference getBaseDeltaCube_UltimateBaseCube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseViewCube <em>Base View Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base View Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseViewCube
	 * @generated
	 */
	EClass getBaseViewCube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseViewCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Ultimate Base Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseViewCube#getUltimateBaseCube()
	 * @see #getBaseViewCube()
	 * @generated
	 */
	EReference getBaseViewCube_UltimateBaseCube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.DeltaAccumulationCube <em>Delta Accumulation Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Delta Accumulation Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.DeltaAccumulationCube
	 * @generated
	 */
	EClass getDeltaAccumulationCube();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Incremental_cubesFactory getIncremental_cubesFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseDeltaCubeImpl <em>Base Delta Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseDeltaCubeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl#getBaseDeltaCube()
		 * @generated
		 */
		EClass BASE_DELTA_CUBE = eINSTANCE.getBaseDeltaCube();

		/**
		 * The meta object literal for the '<em><b>Ultimate Base Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE = eINSTANCE.getBaseDeltaCube_UltimateBaseCube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseViewCubeImpl <em>Base View Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.BaseViewCubeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl#getBaseViewCube()
		 * @generated
		 */
		EClass BASE_VIEW_CUBE = eINSTANCE.getBaseViewCube();

		/**
		 * The meta object literal for the '<em><b>Ultimate Base Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_VIEW_CUBE__ULTIMATE_BASE_CUBE = eINSTANCE.getBaseViewCube_UltimateBaseCube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.DeltaAccumulationCubeImpl <em>Delta Accumulation Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.DeltaAccumulationCubeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesPackageImpl#getDeltaAccumulationCube()
		 * @generated
		 */
		EClass DELTA_ACCUMULATION_CUBE = eINSTANCE.getDeltaAccumulationCube();

	}

} //Incremental_cubesPackage
