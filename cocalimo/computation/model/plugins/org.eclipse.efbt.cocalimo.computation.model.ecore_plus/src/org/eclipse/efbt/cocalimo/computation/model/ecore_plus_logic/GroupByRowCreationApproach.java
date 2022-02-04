/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.ETypedElement;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByColumns <em>Group By Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByCubeColumnNames <em>Group By Cube Column Names</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGroupByRowCreationApproach()
 * @model extendedMetaData="name='GroupByRowCreationApproach' kind='elementOnly'"
 * @generated
 */
public interface GroupByRowCreationApproach extends RowCreationApproach {
	/**
	 * Returns the value of the '<em><b>Group By Columns</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.ETypedElement}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Group By Columns</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGroupByRowCreationApproach_GroupByColumns()
	 * @model
	 * @generated
	 */
	EList<ETypedElement> getGroupByColumns();

	/**
	 * Returns the value of the '<em><b>Group By Cube Column Names</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.String}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Group By Cube Column Names</em>' attribute list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGroupByRowCreationApproach_GroupByCubeColumnNames()
	 * @model
	 * @generated
	 */
	EList<String> getGroupByCubeColumnNames();

} // GroupByRowCreationApproach
