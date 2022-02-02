/**
 */
package cube_transformation_logic;

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
 *  This package describes the logic for cube transformations  
 * 
 * 
 * <!-- end-model-doc -->
 * @see cube_transformation_logic.Cube_transformation_logicFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Cube_transformation_logicPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "cube_transformation_logic";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/cube_transformation_logic";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "cube_transformation_logic";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Cube_transformation_logicPackage eINSTANCE = cube_transformation_logic.impl.Cube_transformation_logicPackageImpl.init();

	/**
	 * The meta object id for the '{@link cube_transformation_logic.impl.CubeTransformationLogicImpl <em>Cube Transformation Logic</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see cube_transformation_logic.impl.CubeTransformationLogicImpl
	 * @see cube_transformation_logic.impl.Cube_transformation_logicPackageImpl#getCubeTransformationLogic()
	 * @generated
	 */
	int CUBE_TRANSFORMATION_LOGIC = 0;

	/**
	 * The feature id for the '<em><b>Column Function Group</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TRANSFORMATION_LOGIC__COLUMN_FUNCTION_GROUP = 0;

	/**
	 * The feature id for the '<em><b>Row Creation Approach Forentity</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TRANSFORMATION_LOGIC__ROW_CREATION_APPROACH_FORENTITY = 1;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TRANSFORMATION_LOGIC__USED_IN_SUBSETS = 2;

	/**
	 * The number of structural features of the '<em>Cube Transformation Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TRANSFORMATION_LOGIC_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Cube Transformation Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_TRANSFORMATION_LOGIC_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link cube_transformation_logic.CubeTransformationLogic <em>Cube Transformation Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Transformation Logic</em>'.
	 * @see cube_transformation_logic.CubeTransformationLogic
	 * @generated
	 */
	EClass getCubeTransformationLogic();

	/**
	 * Returns the meta object for the containment reference '{@link cube_transformation_logic.CubeTransformationLogic#getColumnFunctionGroup <em>Column Function Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Column Function Group</em>'.
	 * @see cube_transformation_logic.CubeTransformationLogic#getColumnFunctionGroup()
	 * @see #getCubeTransformationLogic()
	 * @generated
	 */
	EReference getCubeTransformationLogic_ColumnFunctionGroup();

	/**
	 * Returns the meta object for the containment reference '{@link cube_transformation_logic.CubeTransformationLogic#getRowCreationApproachForentity <em>Row Creation Approach Forentity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Row Creation Approach Forentity</em>'.
	 * @see cube_transformation_logic.CubeTransformationLogic#getRowCreationApproachForentity()
	 * @see #getCubeTransformationLogic()
	 * @generated
	 */
	EReference getCubeTransformationLogic_RowCreationApproachForentity();

	/**
	 * Returns the meta object for the attribute '{@link cube_transformation_logic.CubeTransformationLogic#isUsedInSubsets <em>Used In Subsets</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Used In Subsets</em>'.
	 * @see cube_transformation_logic.CubeTransformationLogic#isUsedInSubsets()
	 * @see #getCubeTransformationLogic()
	 * @generated
	 */
	EAttribute getCubeTransformationLogic_UsedInSubsets();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Cube_transformation_logicFactory getCube_transformation_logicFactory();

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
		 * The meta object literal for the '{@link cube_transformation_logic.impl.CubeTransformationLogicImpl <em>Cube Transformation Logic</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see cube_transformation_logic.impl.CubeTransformationLogicImpl
		 * @see cube_transformation_logic.impl.Cube_transformation_logicPackageImpl#getCubeTransformationLogic()
		 * @generated
		 */
		EClass CUBE_TRANSFORMATION_LOGIC = eINSTANCE.getCubeTransformationLogic();

		/**
		 * The meta object literal for the '<em><b>Column Function Group</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_TRANSFORMATION_LOGIC__COLUMN_FUNCTION_GROUP = eINSTANCE.getCubeTransformationLogic_ColumnFunctionGroup();

		/**
		 * The meta object literal for the '<em><b>Row Creation Approach Forentity</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_TRANSFORMATION_LOGIC__ROW_CREATION_APPROACH_FORENTITY = eINSTANCE.getCubeTransformationLogic_RowCreationApproachForentity();

		/**
		 * The meta object literal for the '<em><b>Used In Subsets</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_TRANSFORMATION_LOGIC__USED_IN_SUBSETS = eINSTANCE.getCubeTransformationLogic_UsedInSubsets();

	}

} //Cube_transformation_logicPackage
