/**
 */
package org.eclipse.efbt.openregspecs.model.sql_lite;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Select Clause</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.sql_lite.SelectClause#getColumns <em>Columns</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage#getSelectClause()
 * @model
 * @generated
 */
public interface SelectClause extends EObject {
	/**
	 * Returns the value of the '<em><b>Columns</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Columns</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage#getSelectClause_Columns()
	 * @model containment="true"
	 * @generated
	 */
	EList<SelectColumn> getColumns();

} // SelectClause
