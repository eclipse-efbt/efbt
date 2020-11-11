/**
 */
package cubes;

import org.eclipse.efbt.cocamo.core.model.module_management.Module_managementPackage;

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
 *  This package describes the FreeBirdToolsCubes of Bird Bench, these are at a slightly different 
 *     level of abstraction form  the related Cubes from the data_definition package. 
 *  
 *  
 * This package describes Module Management classes.
 * These classes allow items to be grouped together and given a version. For example a FunctionSpecModule will contain a group of Functions.
 * Modules can also state which other modules they depend upon, allowing for fine grained dependencies between groups of items.
 * 
 * <!-- end-model-doc -->
 * @see cubes.CubesFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r\r\nCopyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface CubesPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "cubes";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/cubes";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "cubes";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	CubesPackage eINSTANCE = cubes.impl.CubesPackageImpl.init();

	/**
	 * The meta object id for the '{@link cubes.impl.FreeBirdToolsCubeImpl <em>Free Bird Tools Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.FreeBirdToolsCubeImpl
	 * @see cubes.impl.CubesPackageImpl#getFreeBirdToolsCube()
	 * @generated
	 */
	int FREE_BIRD_TOOLS_CUBE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE__NAME = 0;

	/**
	 * The number of structural features of the '<em>Free Bird Tools Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Free Bird Tools Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link cubes.impl.BaseCubeImpl <em>Base Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.BaseCubeImpl
	 * @see cubes.impl.CubesPackageImpl#getBaseCube()
	 * @generated
	 */
	int BASE_CUBE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CUBE__NAME = FREE_BIRD_TOOLS_CUBE__NAME;

	/**
	 * The number of structural features of the '<em>Base Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CUBE_FEATURE_COUNT = FREE_BIRD_TOOLS_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Base Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CUBE_OPERATION_COUNT = FREE_BIRD_TOOLS_CUBE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link cubes.impl.DerivedCubeImpl <em>Derived Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.DerivedCubeImpl
	 * @see cubes.impl.CubesPackageImpl#getDerivedCube()
	 * @generated
	 */
	int DERIVED_CUBE = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE__NAME = FREE_BIRD_TOOLS_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Source Cubes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE__SOURCE_CUBES = FREE_BIRD_TOOLS_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Derived Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_FEATURE_COUNT = FREE_BIRD_TOOLS_CUBE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Derived Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_OPERATION_COUNT = FREE_BIRD_TOOLS_CUBE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link cubes.impl.FreeBirdToolsCubeModuleImpl <em>Free Bird Tools Cube Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.FreeBirdToolsCubeModuleImpl
	 * @see cubes.impl.CubesPackageImpl#getFreeBirdToolsCubeModule()
	 * @generated
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Cubes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE__CUBES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Free Bird Tools Cube Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Free Bird Tools Cube Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link cubes.impl.TargetCubeImpl <em>Target Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.TargetCubeImpl
	 * @see cubes.impl.CubesPackageImpl#getTargetCube()
	 * @generated
	 */
	int TARGET_CUBE = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TARGET_CUBE__NAME = DERIVED_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Source Cubes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TARGET_CUBE__SOURCE_CUBES = DERIVED_CUBE__SOURCE_CUBES;

	/**
	 * The number of structural features of the '<em>Target Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TARGET_CUBE_FEATURE_COUNT = DERIVED_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Target Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TARGET_CUBE_OPERATION_COUNT = DERIVED_CUBE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link cubes.impl.BaseDeltaCubeImpl <em>Base Delta Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.BaseDeltaCubeImpl
	 * @see cubes.impl.CubesPackageImpl#getBaseDeltaCube()
	 * @generated
	 */
	int BASE_DELTA_CUBE = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE__NAME = BASE_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Ultimate Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE__ULTIMATE_BASE_CUBE = BASE_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base Delta Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE_FEATURE_COUNT = BASE_CUBE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base Delta Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_DELTA_CUBE_OPERATION_COUNT = BASE_CUBE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link cubes.impl.BaseViewCubeImpl <em>Base View Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.BaseViewCubeImpl
	 * @see cubes.impl.CubesPackageImpl#getBaseViewCube()
	 * @generated
	 */
	int BASE_VIEW_CUBE = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE__NAME = DERIVED_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Source Cubes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE__SOURCE_CUBES = DERIVED_CUBE__SOURCE_CUBES;

	/**
	 * The feature id for the '<em><b>Ultimate Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE__ULTIMATE_BASE_CUBE = DERIVED_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base View Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE_FEATURE_COUNT = DERIVED_CUBE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base View Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_CUBE_OPERATION_COUNT = DERIVED_CUBE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link cubes.impl.DeltaAccumulationCubeImpl <em>Delta Accumulation Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cubes.impl.DeltaAccumulationCubeImpl
	 * @see cubes.impl.CubesPackageImpl#getDeltaAccumulationCube()
	 * @generated
	 */
	int DELTA_ACCUMULATION_CUBE = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE__NAME = DERIVED_CUBE__NAME;

	/**
	 * The feature id for the '<em><b>Source Cubes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE__SOURCE_CUBES = DERIVED_CUBE__SOURCE_CUBES;

	/**
	 * The number of structural features of the '<em>Delta Accumulation Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE_FEATURE_COUNT = DERIVED_CUBE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Delta Accumulation Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_CUBE_OPERATION_COUNT = DERIVED_CUBE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link cubes.BaseCube <em>Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Cube</em>'.
	 * @see cubes.BaseCube
	 * @generated
	 */
	EClass getBaseCube();

	/**
	 * Returns the meta object for class '{@link cubes.DerivedCube <em>Derived Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Cube</em>'.
	 * @see cubes.DerivedCube
	 * @generated
	 */
	EClass getDerivedCube();

	/**
	 * Returns the meta object for the reference list '{@link cubes.DerivedCube#getSourceCubes <em>Source Cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Source Cubes</em>'.
	 * @see cubes.DerivedCube#getSourceCubes()
	 * @see #getDerivedCube()
	 * @generated
	 */
	EReference getDerivedCube_SourceCubes();

	/**
	 * Returns the meta object for class '{@link cubes.FreeBirdToolsCube <em>Free Bird Tools Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Free Bird Tools Cube</em>'.
	 * @see cubes.FreeBirdToolsCube
	 * @generated
	 */
	EClass getFreeBirdToolsCube();

	/**
	 * Returns the meta object for the attribute '{@link cubes.FreeBirdToolsCube#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see cubes.FreeBirdToolsCube#getName()
	 * @see #getFreeBirdToolsCube()
	 * @generated
	 */
	EAttribute getFreeBirdToolsCube_Name();

	/**
	 * Returns the meta object for class '{@link cubes.FreeBirdToolsCubeModule <em>Free Bird Tools Cube Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Free Bird Tools Cube Module</em>'.
	 * @see cubes.FreeBirdToolsCubeModule
	 * @generated
	 */
	EClass getFreeBirdToolsCubeModule();

	/**
	 * Returns the meta object for the containment reference list '{@link cubes.FreeBirdToolsCubeModule#getCubes <em>Cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cubes</em>'.
	 * @see cubes.FreeBirdToolsCubeModule#getCubes()
	 * @see #getFreeBirdToolsCubeModule()
	 * @generated
	 */
	EReference getFreeBirdToolsCubeModule_Cubes();

	/**
	 * Returns the meta object for class '{@link cubes.TargetCube <em>Target Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Target Cube</em>'.
	 * @see cubes.TargetCube
	 * @generated
	 */
	EClass getTargetCube();

	/**
	 * Returns the meta object for class '{@link cubes.BaseDeltaCube <em>Base Delta Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Delta Cube</em>'.
	 * @see cubes.BaseDeltaCube
	 * @generated
	 */
	EClass getBaseDeltaCube();

	/**
	 * Returns the meta object for the reference '{@link cubes.BaseDeltaCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Ultimate Base Cube</em>'.
	 * @see cubes.BaseDeltaCube#getUltimateBaseCube()
	 * @see #getBaseDeltaCube()
	 * @generated
	 */
	EReference getBaseDeltaCube_UltimateBaseCube();

	/**
	 * Returns the meta object for class '{@link cubes.BaseViewCube <em>Base View Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base View Cube</em>'.
	 * @see cubes.BaseViewCube
	 * @generated
	 */
	EClass getBaseViewCube();

	/**
	 * Returns the meta object for the reference '{@link cubes.BaseViewCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Ultimate Base Cube</em>'.
	 * @see cubes.BaseViewCube#getUltimateBaseCube()
	 * @see #getBaseViewCube()
	 * @generated
	 */
	EReference getBaseViewCube_UltimateBaseCube();

	/**
	 * Returns the meta object for class '{@link cubes.DeltaAccumulationCube <em>Delta Accumulation Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Delta Accumulation Cube</em>'.
	 * @see cubes.DeltaAccumulationCube
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
	CubesFactory getCubesFactory();

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
		 * The meta object literal for the '{@link cubes.impl.BaseCubeImpl <em>Base Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.BaseCubeImpl
		 * @see cubes.impl.CubesPackageImpl#getBaseCube()
		 * @generated
		 */
		EClass BASE_CUBE = eINSTANCE.getBaseCube();

		/**
		 * The meta object literal for the '{@link cubes.impl.DerivedCubeImpl <em>Derived Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.DerivedCubeImpl
		 * @see cubes.impl.CubesPackageImpl#getDerivedCube()
		 * @generated
		 */
		EClass DERIVED_CUBE = eINSTANCE.getDerivedCube();

		/**
		 * The meta object literal for the '<em><b>Source Cubes</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_CUBE__SOURCE_CUBES = eINSTANCE.getDerivedCube_SourceCubes();

		/**
		 * The meta object literal for the '{@link cubes.impl.FreeBirdToolsCubeImpl <em>Free Bird Tools Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.FreeBirdToolsCubeImpl
		 * @see cubes.impl.CubesPackageImpl#getFreeBirdToolsCube()
		 * @generated
		 */
		EClass FREE_BIRD_TOOLS_CUBE = eINSTANCE.getFreeBirdToolsCube();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FREE_BIRD_TOOLS_CUBE__NAME = eINSTANCE.getFreeBirdToolsCube_Name();

		/**
		 * The meta object literal for the '{@link cubes.impl.FreeBirdToolsCubeModuleImpl <em>Free Bird Tools Cube Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.FreeBirdToolsCubeModuleImpl
		 * @see cubes.impl.CubesPackageImpl#getFreeBirdToolsCubeModule()
		 * @generated
		 */
		EClass FREE_BIRD_TOOLS_CUBE_MODULE = eINSTANCE.getFreeBirdToolsCubeModule();

		/**
		 * The meta object literal for the '<em><b>Cubes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FREE_BIRD_TOOLS_CUBE_MODULE__CUBES = eINSTANCE.getFreeBirdToolsCubeModule_Cubes();

		/**
		 * The meta object literal for the '{@link cubes.impl.TargetCubeImpl <em>Target Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.TargetCubeImpl
		 * @see cubes.impl.CubesPackageImpl#getTargetCube()
		 * @generated
		 */
		EClass TARGET_CUBE = eINSTANCE.getTargetCube();

		/**
		 * The meta object literal for the '{@link cubes.impl.BaseDeltaCubeImpl <em>Base Delta Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.BaseDeltaCubeImpl
		 * @see cubes.impl.CubesPackageImpl#getBaseDeltaCube()
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
		 * The meta object literal for the '{@link cubes.impl.BaseViewCubeImpl <em>Base View Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.BaseViewCubeImpl
		 * @see cubes.impl.CubesPackageImpl#getBaseViewCube()
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
		 * The meta object literal for the '{@link cubes.impl.DeltaAccumulationCubeImpl <em>Delta Accumulation Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cubes.impl.DeltaAccumulationCubeImpl
		 * @see cubes.impl.CubesPackageImpl#getDeltaAccumulationCube()
		 * @generated
		 */
		EClass DELTA_ACCUMULATION_CUBE = eINSTANCE.getDeltaAccumulationCube();

	}

} //CubesPackage
