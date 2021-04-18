/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Column Function Group</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A group of ColumnFunctions for a Cube.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunctionGroup#getColumnFunctions <em>Column Functions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicPackage#getColumnFunctionGroup()
 * @model
 * @generated
 */
public interface ColumnFunctionGroup extends EObject
{
	/**
	 * Returns the value of the '<em><b>Column Functions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunction}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The group of ColumnFunctions for a Cube.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column Functions</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicPackage#getColumnFunctionGroup_ColumnFunctions()
	 * @model containment="true"
	 * @generated
	 */
	EList<ColumnFunction> getColumnFunctions();

} // ColumnFunctionGroup
