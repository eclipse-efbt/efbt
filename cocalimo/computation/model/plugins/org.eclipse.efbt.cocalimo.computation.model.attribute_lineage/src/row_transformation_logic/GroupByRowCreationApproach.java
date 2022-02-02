/**
 */
package row_transformation_logic;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Group By Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A RowCreationApproach which describes how single rows are created by grouping  multiple rows from a source cube according to a grouping condition
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link row_transformation_logic.GroupByRowCreationApproach#getGroupByCubeColumnNames <em>Group By Cube Column Names</em>}</li>
 *   <li>{@link row_transformation_logic.GroupByRowCreationApproach#getGroupByColumns <em>Group By Columns</em>}</li>
 * </ul>
 *
 * @see row_transformation_logic.Row_transformation_logicPackage#getGroupByRowCreationApproach()
 * @model extendedMetaData="name='GroupByRowCreationApproach' kind='elementOnly'"
 * @generated
 */
public interface GroupByRowCreationApproach extends RowCreationApproach {
	/**
	 * Returns the value of the '<em><b>Group By Cube Column Names</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EAttribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumns to use to group the source cube by, e.g. grouping a cube by a a column called Currency will lead to one row per unique currency in the source cube rows.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Group By Cube Column Names</em>' reference list.
	 * @see row_transformation_logic.Row_transformation_logicPackage#getGroupByRowCreationApproach_GroupByCubeColumnNames()
	 * @model extendedMetaData="kind='attribute' name='groupByCubeColumns'"
	 * @generated
	 */
	EList<EAttribute> getGroupByCubeColumnNames();

	/**
	 * Returns the value of the '<em><b>Group By Columns</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Group By Columns</em>' reference.
	 * @see #setGroupByColumns(EAttribute)
	 * @see row_transformation_logic.Row_transformation_logicPackage#getGroupByRowCreationApproach_GroupByColumns()
	 * @model
	 * @generated
	 */
	EAttribute getGroupByColumns();

	/**
	 * Sets the value of the '{@link row_transformation_logic.GroupByRowCreationApproach#getGroupByColumns <em>Group By Columns</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Group By Columns</em>' reference.
	 * @see #getGroupByColumns()
	 * @generated
	 */
	void setGroupByColumns(EAttribute value);

} // GroupByRowCreationApproach
