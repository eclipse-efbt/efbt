/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Titled Requirements Section</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getSections <em>Sections</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getReferencingSections <em>Referencing Sections</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getRequirementsType <em>Requirements Type</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getTitle <em>Title</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getTitledRequirementsSection()
 * @model
 * @generated
 */
public interface TitledRequirementsSection extends RequirementsSection {
	/**
	 * Returns the value of the '<em><b>Sections</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sections</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getTitledRequirementsSection_Sections()
	 * @model containment="true"
	 * @generated
	 */
	EList<RequirementsSection> getSections();

	/**
	 * Returns the value of the '<em><b>Referencing Sections</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getLinkedRuleSection <em>Linked Rule Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Referencing Sections</em>' reference.
	 * @see #setReferencingSections(RequirementsSectionLinkWithText)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getTitledRequirementsSection_ReferencingSections()
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getLinkedRuleSection
	 * @model opposite="linkedRuleSection"
	 * @generated
	 */
	RequirementsSectionLinkWithText getReferencingSections();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getReferencingSections <em>Referencing Sections</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Referencing Sections</em>' reference.
	 * @see #getReferencingSections()
	 * @generated
	 */
	void setReferencingSections(RequirementsSectionLinkWithText value);

	/**
	 * Returns the value of the '<em><b>Requirements Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Requirements Type</em>' reference.
	 * @see #setRequirementsType(RequirementType)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getTitledRequirementsSection_RequirementsType()
	 * @model required="true"
	 * @generated
	 */
	RequirementType getRequirementsType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getRequirementsType <em>Requirements Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Requirements Type</em>' reference.
	 * @see #getRequirementsType()
	 * @generated
	 */
	void setRequirementsType(RequirementType value);

	/**
	 * Returns the value of the '<em><b>Title</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Title</em>' attribute.
	 * @see #setTitle(String)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getTitledRequirementsSection_Title()
	 * @model
	 * @generated
	 */
	String getTitle();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getTitle <em>Title</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Title</em>' attribute.
	 * @see #getTitle()
	 * @generated
	 */
	void setTitle(String value);

} // TitledRequirementsSection
