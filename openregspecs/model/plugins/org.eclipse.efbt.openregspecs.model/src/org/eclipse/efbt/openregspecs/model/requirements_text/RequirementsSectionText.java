/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Requirements Section Text</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A section of text
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSectionText#getText <em>Text</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getRequirementsSectionText()
 * @model extendedMetaData="name='RequirementsSectionText' kind='empty'"
 * @generated
 */
public interface RequirementsSectionText extends RequirementsSection {
	/**
	 * Returns the value of the '<em><b>Text</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The text
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Text</em>' attribute.
	 * @see #setText(String)
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getRequirementsSectionText_Text()
	 * @model extendedMetaData="kind='attribute' name='text'"
	 * @generated
	 */
	String getText();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSectionText#getText <em>Text</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Text</em>' attribute.
	 * @see #getText()
	 * @generated
	 */
	void setText(String value);

} // RequirementsSectionText
