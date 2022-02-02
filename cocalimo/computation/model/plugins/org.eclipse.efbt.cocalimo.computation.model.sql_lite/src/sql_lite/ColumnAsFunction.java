/**
 */
package sql_lite;

import column_transformation_logic.ColumnFunction;

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
 *   <li>{@link sql_lite.ColumnAsFunction#getColumn <em>Column</em>}</li>
 * </ul>
 *
 * @see sql_lite.Sql_litePackage#getColumnAsFunction()
 * @model
 * @generated
 */
public interface ColumnAsFunction extends EObject {
	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(ColumnFunction)
	 * @see sql_lite.Sql_litePackage#getColumnAsFunction_Column()
	 * @model
	 * @generated
	 */
	ColumnFunction getColumn();

	/**
	 * Sets the value of the '{@link sql_lite.ColumnAsFunction#getColumn <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column</em>' reference.
	 * @see #getColumn()
	 * @generated
	 */
	void setColumn(ColumnFunction value);

} // ColumnAsFunction
