/**
 */
package row_transformation_logic;

import functions.BooleanFunction;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Filter And Group To One Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A RowCreationFunction which describes how single rows are created by grouping  multiple rows from a source cube according to a grouping condition, while also filtering the source rows by a filter condition
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link row_transformation_logic.FilterAndGroupToOneRowCreationApproach#getFilterFunction <em>Filter Function</em>}</li>
 *   <li>{@link row_transformation_logic.FilterAndGroupToOneRowCreationApproach#getGroupByVariables <em>Group By Variables</em>}</li>
 * </ul>
 *
 * @see row_transformation_logic.Row_transformation_logicPackage#getFilterAndGroupToOneRowCreationApproach()
 * @model extendedMetaData="name='FilterAndGroupToOneRowCreationApproach' kind='elementOnly'"
 * @generated
 */
public interface FilterAndGroupToOneRowCreationApproach extends RowCreationApproach {
	/**
	 * Returns the value of the '<em><b>Filter Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The function that defines the filter condition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Filter Function</em>' containment reference.
	 * @see #setFilterFunction(BooleanFunction)
	 * @see row_transformation_logic.Row_transformation_logicPackage#getFilterAndGroupToOneRowCreationApproach_FilterFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='filterFunction'"
	 * @generated
	 */
	BooleanFunction getFilterFunction();

	/**
	 * Sets the value of the '{@link row_transformation_logic.FilterAndGroupToOneRowCreationApproach#getFilterFunction <em>Filter Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Filter Function</em>' containment reference.
	 * @see #getFilterFunction()
	 * @generated
	 */
	void setFilterFunction(BooleanFunction value);

	/**
	 * Returns the value of the '<em><b>Group By Variables</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EAttribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Group By Variables</em>' reference list.
	 * @see row_transformation_logic.Row_transformation_logicPackage#getFilterAndGroupToOneRowCreationApproach_GroupByVariables()
	 * @model
	 * @generated
	 */
	EList<EAttribute> getGroupByVariables();

} // FilterAndGroupToOneRowCreationApproach
