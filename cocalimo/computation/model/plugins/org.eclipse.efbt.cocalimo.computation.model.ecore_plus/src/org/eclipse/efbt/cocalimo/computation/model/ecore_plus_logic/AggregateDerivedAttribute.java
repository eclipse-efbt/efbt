/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Aggregate Derived Attribute</b></em>'.
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
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute#getAggregateFunction <em>Aggregate Function</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getAggregateDerivedAttribute()
 * @model extendedMetaData="name='AggregateColumnFunction' kind='elementOnly'"
 * @generated
 */
public interface AggregateDerivedAttribute extends DerivedAttribute {
	/**
	 * Returns the value of the '<em><b>Aggregate Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated AggregateFunction 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Aggregate Function</em>' containment reference.
	 * @see #setAggregateFunction(AggregateFunction)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getAggregateDerivedAttribute_AggregateFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='aggregateFunction'"
	 * @generated
	 */
	AggregateFunction getAggregateFunction();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute#getAggregateFunction <em>Aggregate Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Aggregate Function</em>' containment reference.
	 * @see #getAggregateFunction()
	 * @generated
	 */
	void setAggregateFunction(AggregateFunction value);

} // AggregateDerivedAttribute
