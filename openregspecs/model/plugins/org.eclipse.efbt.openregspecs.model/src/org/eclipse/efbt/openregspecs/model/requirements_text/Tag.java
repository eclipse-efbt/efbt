/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Tag</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Tag for linking things to Requirements, This is Subclassed per thing , e.g. ScenarioTag will link a Scenario to requirements.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.Tag#getDisplayName <em>Display Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.Tag#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.Tag#getRequirements <em>Requirements</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getTag()
 * @model
 * @generated
 */
public interface Tag extends EObject {
	/**
	 * Returns the value of the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the tags display name.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Display Name</em>' attribute.
	 * @see #setDisplayName(String)
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getTag_DisplayName()
	 * @model
	 * @generated
	 */
	String getDisplayName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.requirements_text.Tag#getDisplayName <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Display Name</em>' attribute.
	 * @see #getDisplayName()
	 * @generated
	 */
	void setDisplayName(String value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the tags name, which is also its unique id
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getTag_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.requirements_text.Tag#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Requirements</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.requirements_text.TitledRequirementsSection}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the tags name, which is also its unique id
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Requirements</em>' reference list.
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getTag_Requirements()
	 * @model
	 * @generated
	 */
	EList<TitledRequirementsSection> getRequirements();

} // Tag
