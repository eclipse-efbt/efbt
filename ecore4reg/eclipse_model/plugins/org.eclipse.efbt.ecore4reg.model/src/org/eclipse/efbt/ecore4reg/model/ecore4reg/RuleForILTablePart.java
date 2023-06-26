/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

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
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getColumns <em>Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getWhereClause <em>Where Clause</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRuleForILTablePart()
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
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRuleForILTablePart_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Columns</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Columns</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRuleForILTablePart_Columns()
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
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRuleForILTablePart_WhereClause()
	 * @model containment="true"
	 * @generated
	 */
	TableFilter getWhereClause();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getWhereClause <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where Clause</em>' containment reference.
	 * @see #getWhereClause()
	 * @generated
	 */
	void setWhereClause(TableFilter value);

} // RuleForILTablePart
