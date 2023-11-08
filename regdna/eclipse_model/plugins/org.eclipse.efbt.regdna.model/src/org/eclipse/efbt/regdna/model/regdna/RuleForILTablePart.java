/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Rule For IL Table Part</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getColumns <em>Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getWhereClause <em>Where Clause</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRuleForILTablePart()
 * @model
 * @generated
 */
public interface RuleForILTablePart extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRuleForILTablePart_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Columns</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.SelectColumn}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Columns</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRuleForILTablePart_Columns()
	 * @model containment="true"
	 * @generated
	 */
	EList<SelectColumn> getColumns();

	/**
	 * Returns the value of the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Where Clause</em>' containment reference.
	 * @see #setWhereClause(TableFilter)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getRuleForILTablePart_WhereClause()
	 * @model containment="true"
	 * @generated
	 */
	TableFilter getWhereClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart#getWhereClause <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where Clause</em>' containment reference.
	 * @see #getWhereClause()
	 * @generated
	 */
	void setWhereClause(TableFilter value);

} // RuleForILTablePart
