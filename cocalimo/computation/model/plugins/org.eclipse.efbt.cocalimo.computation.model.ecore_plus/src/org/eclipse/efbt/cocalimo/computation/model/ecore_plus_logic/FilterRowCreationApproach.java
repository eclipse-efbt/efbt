/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Filter Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A RowCreationApproach which describes how single rows are created by filtering  single rows from a source cube according to a filter condition
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach#getFilterFunction <em>Filter Function</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getFilterRowCreationApproach()
 * @model extendedMetaData="name='FilterRowCreationApproach' kind='elementOnly'"
 * @generated
 */
public interface FilterRowCreationApproach extends RowCreationApproach {
	/**
	 * Returns the value of the '<em><b>Filter Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The function that defines the filter condition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Filter Function</em>' containment reference.
	 * @see #setFilterFunction(BooleanOperation)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getFilterRowCreationApproach_FilterFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='filterFunction'"
	 * @generated
	 */
	BooleanOperation getFilterFunction();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach#getFilterFunction <em>Filter Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Filter Function</em>' containment reference.
	 * @see #getFilterFunction()
	 * @generated
	 */
	void setFilterFunction(BooleanOperation value);

} // FilterRowCreationApproach
