/**
 */
package org.eclipse.efbt.language.dtm.model.transformation;

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
 * This package describes DataSetTransformations used to derive DerivedCubes, and the grouping of these DataSetTransformations into Schemes and Modules.
 *  
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface TransformationPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "transformation";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/transformation";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "transformation";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TransformationPackage eINSTANCE = org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.DataSetTransformationImpl <em>Data Set Transformation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.DataSetTransformationImpl
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getDataSetTransformation()
	 * @generated
	 */
	int DATA_SET_TRANSFORMATION = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_SET_TRANSFORMATION__CUBE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_SET_TRANSFORMATION__NAME = 1;

	/**
	 * The number of structural features of the '<em>Data Set Transformation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_SET_TRANSFORMATION_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Data Set Transformation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_SET_TRANSFORMATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl <em>Versioned Components Set</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getVersionedComponentsSet()
	 * @generated
	 */
	int VERSIONED_COMPONENTS_SET = 2;

	/**
	 * The feature id for the '<em><b>Cube Schema Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES = 0;

	/**
	 * The feature id for the '<em><b>Dataset Transformation Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_COMPONENTS_SET__NAME = 2;

	/**
	 * The feature id for the '<em><b>Report Cell View Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES = 3;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_COMPONENTS_SET__LICENSE = 4;

	/**
	 * The number of structural features of the '<em>Versioned Components Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_COMPONENTS_SET_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>Versioned Components Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_COMPONENTS_SET_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.ReleaseImpl <em>Release</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.ReleaseImpl
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getRelease()
	 * @generated
	 */
	int RELEASE = 1;

	/**
	 * The feature id for the '<em><b>Cube Schema Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE__CUBE_SCHEMA_MODULES = VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES;

	/**
	 * The feature id for the '<em><b>Dataset Transformation Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE__DATASET_TRANSFORMATION_MODULES = VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE__NAME = VERSIONED_COMPONENTS_SET__NAME;

	/**
	 * The feature id for the '<em><b>Report Cell View Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE__REPORT_CELL_VIEW_MODULES = VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE__LICENSE = VERSIONED_COMPONENTS_SET__LICENSE;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE__VERSION = VERSIONED_COMPONENTS_SET_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Release</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE_FEATURE_COUNT = VERSIONED_COMPONENTS_SET_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Release</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELEASE_OPERATION_COUNT = VERSIONED_COMPONENTS_SET_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedCubeSchemaModuleImpl <em>Versioned Cube Schema Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedCubeSchemaModuleImpl
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getVersionedCubeSchemaModule()
	 * @generated
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Schemas</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE__SCHEMAS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Versioned Cube Schema Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Versioned Cube Schema Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_CUBE_SCHEMA_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedFunctionalModuleLogicImpl <em>Versioned Functional Module Logic</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedFunctionalModuleLogicImpl
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getVersionedFunctionalModuleLogic()
	 * @generated
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The number of structural features of the '<em>Versioned Functional Module Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Versioned Functional Module Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_FUNCTIONAL_MODULE_LOGIC_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl <em>dtm Executable Logic</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getdtmExecutableLogic()
	 * @generated
	 */
	int DTM_EXECUTABLE_LOGIC = 5;

	/**
	 * The feature id for the '<em><b>Cubes Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC__CUBES_MODULE = 0;

	/**
	 * The feature id for the '<em><b>Function Spec Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC__NAME = 2;

	/**
	 * The feature id for the '<em><b>Cubes Hierarchy Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Cube Schema Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES = 4;

	/**
	 * The feature id for the '<em><b>Dataset Transformation Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES = 5;

	/**
	 * The feature id for the '<em><b>Report Cell View Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES = 6;

	/**
	 * The number of structural features of the '<em>dtm Executable Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>dtm Executable Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_EXECUTABLE_LOGIC_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl <em>dtm Program</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl
	 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getdtmProgram()
	 * @generated
	 */
	int DTM_PROGRAM = 6;

	/**
	 * The feature id for the '<em><b>Executable Logic</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_PROGRAM__EXECUTABLE_LOGIC = 0;

	/**
	 * The feature id for the '<em><b>Smcubes Static Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_PROGRAM__SMCUBES_STATIC_MODEL = 1;

	/**
	 * The feature id for the '<em><b>Struct Variables Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_PROGRAM__STRUCT_VARIABLES_MODULE = 2;

	/**
	 * The feature id for the '<em><b>Array Typed Variables Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Release</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_PROGRAM__RELEASE = 4;

	/**
	 * The number of structural features of the '<em>dtm Program</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_PROGRAM_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>dtm Program</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_PROGRAM_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation <em>Data Set Transformation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Data Set Transformation</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation
	 * @generated
	 */
	EClass getDataSetTransformation();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation#getCube()
	 * @see #getDataSetTransformation()
	 * @generated
	 */
	EReference getDataSetTransformation_Cube();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation#getName()
	 * @see #getDataSetTransformation()
	 * @generated
	 */
	EAttribute getDataSetTransformation_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.transformation.Release <em>Release</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Release</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.Release
	 * @generated
	 */
	EClass getRelease();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.language.dtm.model.transformation.Release#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.Release#getVersion()
	 * @see #getRelease()
	 * @generated
	 */
	EAttribute getRelease_Version();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet <em>Versioned Components Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Versioned Components Set</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet
	 * @generated
	 */
	EClass getVersionedComponentsSet();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getCubeSchemaModules <em>Cube Schema Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Cube Schema Modules</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getCubeSchemaModules()
	 * @see #getVersionedComponentsSet()
	 * @generated
	 */
	EReference getVersionedComponentsSet_CubeSchemaModules();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getDatasetTransformationModules <em>Dataset Transformation Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Dataset Transformation Modules</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getDatasetTransformationModules()
	 * @see #getVersionedComponentsSet()
	 * @generated
	 */
	EReference getVersionedComponentsSet_DatasetTransformationModules();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getName()
	 * @see #getVersionedComponentsSet()
	 * @generated
	 */
	EAttribute getVersionedComponentsSet_Name();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getReportCellViewModules <em>Report Cell View Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Report Cell View Modules</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getReportCellViewModules()
	 * @see #getVersionedComponentsSet()
	 * @generated
	 */
	EReference getVersionedComponentsSet_ReportCellViewModules();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getLicense <em>License</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>License</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getLicense()
	 * @see #getVersionedComponentsSet()
	 * @generated
	 */
	EAttribute getVersionedComponentsSet_License();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule <em>Versioned Cube Schema Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Versioned Cube Schema Module</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule
	 * @generated
	 */
	EClass getVersionedCubeSchemaModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule#getSchemas <em>Schemas</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Schemas</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule#getSchemas()
	 * @see #getVersionedCubeSchemaModule()
	 * @generated
	 */
	EReference getVersionedCubeSchemaModule_Schemas();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic <em>Versioned Functional Module Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Versioned Functional Module Logic</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic
	 * @generated
	 */
	EClass getVersionedFunctionalModuleLogic();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic <em>dtm Executable Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>dtm Executable Logic</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic
	 * @generated
	 */
	EClass getdtmExecutableLogic();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesModule <em>Cubes Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Cubes Module</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesModule()
	 * @see #getdtmExecutableLogic()
	 * @generated
	 */
	EReference getdtmExecutableLogic_CubesModule();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getFunctionSpecModule <em>Function Spec Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Function Spec Module</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getFunctionSpecModule()
	 * @see #getdtmExecutableLogic()
	 * @generated
	 */
	EReference getdtmExecutableLogic_FunctionSpecModule();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getName()
	 * @see #getdtmExecutableLogic()
	 * @generated
	 */
	EAttribute getdtmExecutableLogic_Name();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesHierarchyModule <em>Cubes Hierarchy Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Cubes Hierarchy Module</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesHierarchyModule()
	 * @see #getdtmExecutableLogic()
	 * @generated
	 */
	EReference getdtmExecutableLogic_CubesHierarchyModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubeSchemaModules <em>Cube Schema Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube Schema Modules</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubeSchemaModules()
	 * @see #getdtmExecutableLogic()
	 * @generated
	 */
	EReference getdtmExecutableLogic_CubeSchemaModules();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getDatasetTransformationModules <em>Dataset Transformation Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Dataset Transformation Modules</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getDatasetTransformationModules()
	 * @see #getdtmExecutableLogic()
	 * @generated
	 */
	EReference getdtmExecutableLogic_DatasetTransformationModules();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getReportCellViewModules <em>Report Cell View Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Cell View Modules</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getReportCellViewModules()
	 * @see #getdtmExecutableLogic()
	 * @generated
	 */
	EReference getdtmExecutableLogic_ReportCellViewModules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram <em>dtm Program</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>dtm Program</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmProgram
	 * @generated
	 */
	EClass getdtmProgram();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getExecutableLogic <em>Executable Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Executable Logic</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getExecutableLogic()
	 * @see #getdtmProgram()
	 * @generated
	 */
	EReference getdtmProgram_ExecutableLogic();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getSmcubesStaticModel <em>Smcubes Static Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Smcubes Static Model</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getSmcubesStaticModel()
	 * @see #getdtmProgram()
	 * @generated
	 */
	EReference getdtmProgram_SmcubesStaticModel();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getStructVariablesModule <em>Struct Variables Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Struct Variables Module</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getStructVariablesModule()
	 * @see #getdtmProgram()
	 * @generated
	 */
	EReference getdtmProgram_StructVariablesModule();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getArrayTypedVariablesModule <em>Array Typed Variables Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Array Typed Variables Module</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getArrayTypedVariablesModule()
	 * @see #getdtmProgram()
	 * @generated
	 */
	EReference getdtmProgram_ArrayTypedVariablesModule();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getRelease <em>Release</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Release</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getRelease()
	 * @see #getdtmProgram()
	 * @generated
	 */
	EReference getdtmProgram_Release();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TransformationFactory getTransformationFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.DataSetTransformationImpl <em>Data Set Transformation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.DataSetTransformationImpl
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getDataSetTransformation()
		 * @generated
		 */
		EClass DATA_SET_TRANSFORMATION = eINSTANCE.getDataSetTransformation();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DATA_SET_TRANSFORMATION__CUBE = eINSTANCE.getDataSetTransformation_Cube();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DATA_SET_TRANSFORMATION__NAME = eINSTANCE.getDataSetTransformation_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.ReleaseImpl <em>Release</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.ReleaseImpl
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getRelease()
		 * @generated
		 */
		EClass RELEASE = eINSTANCE.getRelease();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RELEASE__VERSION = eINSTANCE.getRelease_Version();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl <em>Versioned Components Set</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getVersionedComponentsSet()
		 * @generated
		 */
		EClass VERSIONED_COMPONENTS_SET = eINSTANCE.getVersionedComponentsSet();

		/**
		 * The meta object literal for the '<em><b>Cube Schema Modules</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES = eINSTANCE.getVersionedComponentsSet_CubeSchemaModules();

		/**
		 * The meta object literal for the '<em><b>Dataset Transformation Modules</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES = eINSTANCE.getVersionedComponentsSet_DatasetTransformationModules();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VERSIONED_COMPONENTS_SET__NAME = eINSTANCE.getVersionedComponentsSet_Name();

		/**
		 * The meta object literal for the '<em><b>Report Cell View Modules</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES = eINSTANCE.getVersionedComponentsSet_ReportCellViewModules();

		/**
		 * The meta object literal for the '<em><b>License</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VERSIONED_COMPONENTS_SET__LICENSE = eINSTANCE.getVersionedComponentsSet_License();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedCubeSchemaModuleImpl <em>Versioned Cube Schema Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedCubeSchemaModuleImpl
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getVersionedCubeSchemaModule()
		 * @generated
		 */
		EClass VERSIONED_CUBE_SCHEMA_MODULE = eINSTANCE.getVersionedCubeSchemaModule();

		/**
		 * The meta object literal for the '<em><b>Schemas</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VERSIONED_CUBE_SCHEMA_MODULE__SCHEMAS = eINSTANCE.getVersionedCubeSchemaModule_Schemas();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedFunctionalModuleLogicImpl <em>Versioned Functional Module Logic</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedFunctionalModuleLogicImpl
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getVersionedFunctionalModuleLogic()
		 * @generated
		 */
		EClass VERSIONED_FUNCTIONAL_MODULE_LOGIC = eINSTANCE.getVersionedFunctionalModuleLogic();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl <em>dtm Executable Logic</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getdtmExecutableLogic()
		 * @generated
		 */
		EClass DTM_EXECUTABLE_LOGIC = eINSTANCE.getdtmExecutableLogic();

		/**
		 * The meta object literal for the '<em><b>Cubes Module</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_EXECUTABLE_LOGIC__CUBES_MODULE = eINSTANCE.getdtmExecutableLogic_CubesModule();

		/**
		 * The meta object literal for the '<em><b>Function Spec Module</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE = eINSTANCE.getdtmExecutableLogic_FunctionSpecModule();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DTM_EXECUTABLE_LOGIC__NAME = eINSTANCE.getdtmExecutableLogic_Name();

		/**
		 * The meta object literal for the '<em><b>Cubes Hierarchy Module</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE = eINSTANCE.getdtmExecutableLogic_CubesHierarchyModule();

		/**
		 * The meta object literal for the '<em><b>Cube Schema Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES = eINSTANCE.getdtmExecutableLogic_CubeSchemaModules();

		/**
		 * The meta object literal for the '<em><b>Dataset Transformation Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES = eINSTANCE.getdtmExecutableLogic_DatasetTransformationModules();

		/**
		 * The meta object literal for the '<em><b>Report Cell View Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES = eINSTANCE.getdtmExecutableLogic_ReportCellViewModules();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl <em>dtm Program</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl
		 * @see org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationPackageImpl#getdtmProgram()
		 * @generated
		 */
		EClass DTM_PROGRAM = eINSTANCE.getdtmProgram();

		/**
		 * The meta object literal for the '<em><b>Executable Logic</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_PROGRAM__EXECUTABLE_LOGIC = eINSTANCE.getdtmProgram_ExecutableLogic();

		/**
		 * The meta object literal for the '<em><b>Smcubes Static Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_PROGRAM__SMCUBES_STATIC_MODEL = eINSTANCE.getdtmProgram_SmcubesStaticModel();

		/**
		 * The meta object literal for the '<em><b>Struct Variables Module</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_PROGRAM__STRUCT_VARIABLES_MODULE = eINSTANCE.getdtmProgram_StructVariablesModule();

		/**
		 * The meta object literal for the '<em><b>Array Typed Variables Module</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE = eINSTANCE.getdtmProgram_ArrayTypedVariablesModule();

		/**
		 * The meta object literal for the '<em><b>Release</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DTM_PROGRAM__RELEASE = eINSTANCE.getdtmProgram_Release();

	}

} //TransformationPackage
