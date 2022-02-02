/**
 */
package advanced_row_transformation_logic;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import row_transformation_logic.Row_transformation_logicPackage;

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
 * @see advanced_row_transformation_logic.Advanced_row_transformation_logicFactory
 * @model kind="package"
 * @generated
 */
public interface Advanced_row_transformation_logicPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "advanced_row_transformation_logic";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/advanced_row_transformation_logic";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "advanced_row_transformation_logic";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Advanced_row_transformation_logicPackage eINSTANCE = advanced_row_transformation_logic.impl.Advanced_row_transformation_logicPackageImpl.init();

	/**
	 * The meta object id for the '{@link advanced_row_transformation_logic.impl.ExplodeArrayOfStructsRowFunctionImpl <em>Explode Array Of Structs Row Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see advanced_row_transformation_logic.impl.ExplodeArrayOfStructsRowFunctionImpl
	 * @see advanced_row_transformation_logic.impl.Advanced_row_transformation_logicPackageImpl#getExplodeArrayOfStructsRowFunction()
	 * @generated
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__DEPENDANT_CUBE_COLUMNS = Row_transformation_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__CREATED_CUBE_COLUMNS = Row_transformation_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__DEPENDANT_STRUCT_ITEM_COLUMNS = Row_transformation_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The feature id for the '<em><b>Array Source Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE = Row_transformation_logicPackage.ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Explode Array Of Structs Row Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION_FEATURE_COUNT = Row_transformation_logicPackage.ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Explode Array Of Structs Row Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION_OPERATION_COUNT = Row_transformation_logicPackage.ROW_CREATION_APPROACH_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction <em>Explode Array Of Structs Row Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Explode Array Of Structs Row Function</em>'.
	 * @see advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction
	 * @generated
	 */
	EClass getExplodeArrayOfStructsRowFunction();

	/**
	 * Returns the meta object for the reference '{@link advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable <em>Array Source Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Array Source Variable</em>'.
	 * @see advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable()
	 * @see #getExplodeArrayOfStructsRowFunction()
	 * @generated
	 */
	EReference getExplodeArrayOfStructsRowFunction_ArraySourceVariable();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Advanced_row_transformation_logicFactory getAdvanced_row_transformation_logicFactory();

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
		 * The meta object literal for the '{@link advanced_row_transformation_logic.impl.ExplodeArrayOfStructsRowFunctionImpl <em>Explode Array Of Structs Row Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see advanced_row_transformation_logic.impl.ExplodeArrayOfStructsRowFunctionImpl
		 * @see advanced_row_transformation_logic.impl.Advanced_row_transformation_logicPackageImpl#getExplodeArrayOfStructsRowFunction()
		 * @generated
		 */
		EClass EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION = eINSTANCE.getExplodeArrayOfStructsRowFunction();

		/**
		 * The meta object literal for the '<em><b>Array Source Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE = eINSTANCE.getExplodeArrayOfStructsRowFunction_ArraySourceVariable();

	}

} //Advanced_row_transformation_logicPackage
