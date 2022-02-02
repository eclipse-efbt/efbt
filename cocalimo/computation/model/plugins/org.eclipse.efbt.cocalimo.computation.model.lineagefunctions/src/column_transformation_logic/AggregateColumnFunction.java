/**
 */
package column_transformation_logic;

import functions.AggregateFunction;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Aggregate Column Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A ColumnFunction on rows where there is multiple  source row per source cube,such as summing a column from a large set of source rows. 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link column_transformation_logic.AggregateColumnFunction#getAggregateFunction <em>Aggregate Function</em>}</li>
 * </ul>
 *
 * @see column_transformation_logic.Column_transformation_logicPackage#getAggregateColumnFunction()
 * @model extendedMetaData="name='AggregateColumnFunction' kind='elementOnly'"
 * @generated
 */
public interface AggregateColumnFunction extends ColumnFunction {
	/**
	 * Returns the value of the '<em><b>Aggregate Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated AggregateFunction 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Aggregate Function</em>' containment reference.
	 * @see #setAggregateFunction(AggregateFunction)
	 * @see column_transformation_logic.Column_transformation_logicPackage#getAggregateColumnFunction_AggregateFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='aggregateFunction'"
	 * @generated
	 */
	AggregateFunction getAggregateFunction();

	/**
	 * Sets the value of the '{@link column_transformation_logic.AggregateColumnFunction#getAggregateFunction <em>Aggregate Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Aggregate Function</em>' containment reference.
	 * @see #getAggregateFunction()
	 * @generated
	 */
	void setAggregateFunction(AggregateFunction value);

} // AggregateColumnFunction
