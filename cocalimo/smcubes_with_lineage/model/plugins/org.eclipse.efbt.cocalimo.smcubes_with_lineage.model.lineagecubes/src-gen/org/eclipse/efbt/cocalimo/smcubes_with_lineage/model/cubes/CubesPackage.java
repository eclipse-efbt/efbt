/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes;

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
 * This package describes the FreeBirdToolsCubes.
 * DataStructure Approaches such as SMCubes allow us to define an input layer a the start of computation as a set of input Layer cubes (which are represented in this package as BaseCubes.
 * For languages such as VTL which create a chain of transformations of cubes, we can consider the inetrmediate datasets created as derived cubes, and the end results as Target Cubes.
 * This package allows us to represent these ideas at a high level, and show the lineage between target, derived and base cubes.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesFactory
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
	CubesPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeImpl <em>Free Bird Tools Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBirdToolsCube()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.BaseCubeImpl <em>Base Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.BaseCubeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getBaseCube()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.DerivedCubeImpl <em>Derived Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.DerivedCubeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getDerivedCube()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeModuleImpl <em>Free Bird Tools Cube Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBirdToolsCubeModule()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.TargetCubeImpl <em>Target Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.TargetCubeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getTargetCube()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipModuleImpl <em>Free BIRD Tools Cube Hierarchy Relationship Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBIRDToolsCubeHierarchyRelationshipModule()
	 * @generated
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE = 5;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Hierarchy Relationships</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Free BIRD Tools Cube Hierarchy Relationship Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Free BIRD Tools Cube Hierarchy Relationship Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipImpl <em>Free BIRD Tools Cube Hierarchy Relationship</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBIRDToolsCubeHierarchyRelationship()
	 * @generated
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP = 6;

	/**
	 * The feature id for the '<em><b>Super Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE = 0;

	/**
	 * The feature id for the '<em><b>Sub Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE = 1;

	/**
	 * The number of structural features of the '<em>Free BIRD Tools Cube Hierarchy Relationship</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Free BIRD Tools Cube Hierarchy Relationship</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube <em>Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube
	 * @generated
	 */
	EClass getBaseCube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube <em>Derived Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube
	 * @generated
	 */
	EClass getDerivedCube();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube#getSourceCubes <em>Source Cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Source Cubes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube#getSourceCubes()
	 * @see #getDerivedCube()
	 * @generated
	 */
	EReference getDerivedCube_SourceCubes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube <em>Free Bird Tools Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Free Bird Tools Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube
	 * @generated
	 */
	EClass getFreeBirdToolsCube();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube#getName()
	 * @see #getFreeBirdToolsCube()
	 * @generated
	 */
	EAttribute getFreeBirdToolsCube_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule <em>Free Bird Tools Cube Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Free Bird Tools Cube Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule
	 * @generated
	 */
	EClass getFreeBirdToolsCubeModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule#getCubes <em>Cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cubes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule#getCubes()
	 * @see #getFreeBirdToolsCubeModule()
	 * @generated
	 */
	EReference getFreeBirdToolsCubeModule_Cubes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.TargetCube <em>Target Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Target Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.TargetCube
	 * @generated
	 */
	EClass getTargetCube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule <em>Free BIRD Tools Cube Hierarchy Relationship Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Free BIRD Tools Cube Hierarchy Relationship Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule
	 * @generated
	 */
	EClass getFreeBIRDToolsCubeHierarchyRelationshipModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule#getHierarchyRelationships <em>Hierarchy Relationships</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Hierarchy Relationships</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule#getHierarchyRelationships()
	 * @see #getFreeBIRDToolsCubeHierarchyRelationshipModule()
	 * @generated
	 */
	EReference getFreeBIRDToolsCubeHierarchyRelationshipModule_HierarchyRelationships();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship <em>Free BIRD Tools Cube Hierarchy Relationship</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Free BIRD Tools Cube Hierarchy Relationship</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship
	 * @generated
	 */
	EClass getFreeBIRDToolsCubeHierarchyRelationship();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSuperCube <em>Super Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Super Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSuperCube()
	 * @see #getFreeBIRDToolsCubeHierarchyRelationship()
	 * @generated
	 */
	EReference getFreeBIRDToolsCubeHierarchyRelationship_SuperCube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSubCube <em>Sub Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Sub Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSubCube()
	 * @see #getFreeBIRDToolsCubeHierarchyRelationship()
	 * @generated
	 */
	EReference getFreeBIRDToolsCubeHierarchyRelationship_SubCube();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.BaseCubeImpl <em>Base Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.BaseCubeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getBaseCube()
		 * @generated
		 */
		EClass BASE_CUBE = eINSTANCE.getBaseCube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.DerivedCubeImpl <em>Derived Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.DerivedCubeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getDerivedCube()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeImpl <em>Free Bird Tools Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBirdToolsCube()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeModuleImpl <em>Free Bird Tools Cube Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBirdToolsCubeModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBirdToolsCubeModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.TargetCubeImpl <em>Target Cube</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.TargetCubeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getTargetCube()
		 * @generated
		 */
		EClass TARGET_CUBE = eINSTANCE.getTargetCube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipModuleImpl <em>Free BIRD Tools Cube Hierarchy Relationship Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBIRDToolsCubeHierarchyRelationshipModule()
		 * @generated
		 */
		EClass FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE = eINSTANCE.getFreeBIRDToolsCubeHierarchyRelationshipModule();

		/**
		 * The meta object literal for the '<em><b>Hierarchy Relationships</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS = eINSTANCE.getFreeBIRDToolsCubeHierarchyRelationshipModule_HierarchyRelationships();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipImpl <em>Free BIRD Tools Cube Hierarchy Relationship</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.CubesPackageImpl#getFreeBIRDToolsCubeHierarchyRelationship()
		 * @generated
		 */
		EClass FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP = eINSTANCE.getFreeBIRDToolsCubeHierarchyRelationship();

		/**
		 * The meta object literal for the '<em><b>Super Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE = eINSTANCE.getFreeBIRDToolsCubeHierarchyRelationship_SuperCube();

		/**
		 * The meta object literal for the '<em><b>Sub Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE = eINSTANCE.getFreeBIRDToolsCubeHierarchyRelationship_SubCube();

	}

} //CubesPackage
