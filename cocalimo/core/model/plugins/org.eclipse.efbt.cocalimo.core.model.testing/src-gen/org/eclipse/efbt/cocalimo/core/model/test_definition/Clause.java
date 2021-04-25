/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Clause</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A cluase, i.e a given when or then clause.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getText <em>Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getParams <em>Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getRegulatoryTextLinks <em>Regulatory Text Links</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getClause()
 * @model
 * @generated
 */
public interface Clause extends EObject
{
	/**
	 * Returns the value of the '<em><b>Text</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The text of a clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Text</em>' reference.
	 * @see #setText(ClauseText)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getClause_Text()
	 * @model
	 * @generated
	 */
	ClauseText getText();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.Clause#getText <em>Text</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Text</em>' reference.
	 * @see #getText()
	 * @generated
	 */
	void setText(ClauseText value);

	/**
	 * Returns the value of the '<em><b>Params</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.test_definition.Param}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The parameters of a clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Params</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getClause_Params()
	 * @model containment="true"
	 * @generated
	 */
	EList<Param> getParams();

	/**
	 * Returns the value of the '<em><b>Regulatory Text Links</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The links to regulatory text for a clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Regulatory Text Links</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getClause_RegulatoryTextLinks()
	 * @model
	 * @generated
	 */
	EList<TitledRequirementsSection> getRegulatoryTextLinks();

} // Clause
