/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Rules For IL Table</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable#getRulesForTablePart <em>Rules For Table Part</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable#getInputLayerTable <em>Input Layer Table</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRulesForILTable()
 * @model
 * @generated
 */
public interface RulesForILTable extends EObject {
	/**
	 * Returns the value of the '<em><b>Rules For Table Part</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Rules For Table Part</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRulesForILTable_RulesForTablePart()
	 * @model containment="true"
	 * @generated
	 */
	EList<RuleForILTablePart> getRulesForTablePart();

	/**
	 * Returns the value of the '<em><b>Input Layer Table</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Input Layer Table</em>' reference.
	 * @see #setInputLayerTable(ELClass)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRulesForILTable_InputLayerTable()
	 * @model
	 * @generated
	 */
	ELClass getInputLayerTable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable#getInputLayerTable <em>Input Layer Table</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Input Layer Table</em>' reference.
	 * @see #getInputLayerTable()
	 * @generated
	 */
	void setInputLayerTable(ELClass value);

} // RulesForILTable
