/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute;
import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Column As Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.ColumnAsFunction#getColumn <em>Column</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getColumnAsFunction()
 * @model
 * @generated
 */
public interface ColumnAsFunction extends EObject {
	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(DerivedAttribute)
	 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getColumnAsFunction_Column()
	 * @model
	 * @generated
	 */
	DerivedAttribute getColumn();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.ColumnAsFunction#getColumn <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column</em>' reference.
	 * @see #getColumn()
	 * @generated
	 */
	void setColumn(DerivedAttribute value);

} // ColumnAsFunction
