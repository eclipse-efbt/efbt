/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;


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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction#getBasicFunction <em>Basic Function</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getBasicColumnFunction()
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
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getBasicColumnFunction_BasicFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='basicFunction'"
	 * @generated
	 */
	BasicFunction getBasicFunction();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction#getBasicFunction <em>Basic Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Basic Function</em>' containment reference.
	 * @see #getBasicFunction()
	 * @generated
	 */
	void setBasicFunction(BasicFunction value);

} // BasicColumnFunction
