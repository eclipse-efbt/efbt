/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema;

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
 *  This package describes a CubeSchema, which is defined as a BaseCube which has no structure, and an associated CUBE from the SMCubes model.
 * From The SMCubes CUBE we can derive what variables are associated with the CubeSchema.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Cube_schemaPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "cube_schema";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/cube_schema";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "cube_schema";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Cube_schemaPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.Cube_schemaPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.CubeSchemaImpl <em>Cube Schema</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.CubeSchemaImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.Cube_schemaPackageImpl#getCubeSchema()
	 * @generated
	 */
	int CUBE_SCHEMA = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_SCHEMA__CUBE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_SCHEMA__NAME = 1;

	/**
	 * The feature id for the '<em><b>Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_SCHEMA__BASE_CUBE = 2;

	/**
	 * The number of structural features of the '<em>Cube Schema</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_SCHEMA_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Cube Schema</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_SCHEMA_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema <em>Cube Schema</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Schema</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema
	 * @generated
	 */
	EClass getCubeSchema();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema#getCube()
	 * @see #getCubeSchema()
	 * @generated
	 */
	EReference getCubeSchema_Cube();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema#getName()
	 * @see #getCubeSchema()
	 * @generated
	 */
	EAttribute getCubeSchema_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema#getBaseCube <em>Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Base Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema#getBaseCube()
	 * @see #getCubeSchema()
	 * @generated
	 */
	EReference getCubeSchema_BaseCube();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Cube_schemaFactory getCube_schemaFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.CubeSchemaImpl <em>Cube Schema</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.CubeSchemaImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl.Cube_schemaPackageImpl#getCubeSchema()
		 * @generated
		 */
		EClass CUBE_SCHEMA = eINSTANCE.getCubeSchema();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_SCHEMA__CUBE = eINSTANCE.getCubeSchema_Cube();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_SCHEMA__NAME = eINSTANCE.getCubeSchema_Name();

		/**
		 * The meta object literal for the '<em><b>Base Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_SCHEMA__BASE_CUBE = eINSTANCE.getCubeSchema_BaseCube();

	}

} //Cube_schemaPackage
