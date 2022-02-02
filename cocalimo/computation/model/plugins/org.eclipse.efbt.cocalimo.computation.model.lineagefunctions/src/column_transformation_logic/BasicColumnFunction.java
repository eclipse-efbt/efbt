/**
 */
package column_transformation_logic;

import functions.BasicFunction;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Column Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A ColumnFunction represnted by a BasicFunction (i.e. not an aggregating function).
 * The parameters of this function will be other CubeColumns, which might be a CubeColumn of a Base Cube, or another ColumnFunction from a DerivedCube.
 * 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link column_transformation_logic.BasicColumnFunction#getBasicFunction <em>Basic Function</em>}</li>
 * </ul>
 *
 * @see column_transformation_logic.Column_transformation_logicPackage#getBasicColumnFunction()
 * @model extendedMetaData="name='BasicColumnFunction' kind='empty'"
 * @generated
 */
public interface BasicColumnFunction extends ColumnFunction {
	/**
	 * Returns the value of the '<em><b>Basic Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated function
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Basic Function</em>' containment reference.
	 * @see #setBasicFunction(BasicFunction)
	 * @see column_transformation_logic.Column_transformation_logicPackage#getBasicColumnFunction_BasicFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='basicFunction'"
	 * @generated
	 */
	BasicFunction getBasicFunction();

	/**
	 * Sets the value of the '{@link column_transformation_logic.BasicColumnFunction#getBasicFunction <em>Basic Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Basic Function</em>' containment reference.
	 * @see #getBasicFunction()
	 * @generated
	 */
	void setBasicFunction(BasicFunction value);

} // BasicColumnFunction
