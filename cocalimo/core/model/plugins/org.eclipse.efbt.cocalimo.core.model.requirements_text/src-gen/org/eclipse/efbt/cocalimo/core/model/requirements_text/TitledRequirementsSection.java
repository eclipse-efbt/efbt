/**
 */
package org.eclipse.efbt.cocalimo.core.model.requirements_text;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Titled Requirements Section</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Requirements Section which has a title, such as an Article 321 or  Chapter 5.
 *        Note that this contains other requirements sections, which could in turn be other TitledRequirementsSections 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getSections <em>Sections</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getReferencingSections <em>Referencing Sections</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getRequirementsType <em>Requirements Type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getTitle <em>Title</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getTitledRequirementsSection()
 * @model extendedMetaData="name='TitledRequirementsSection' kind='elementOnly'"
 * @generated
 */
public interface TitledRequirementsSection extends RequirementsSection
{
	/**
	 * Returns the value of the '<em><b>Sections</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSection}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The sections contained in the titled section
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sections</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getTitledRequirementsSection_Sections()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='sections'"
	 * @generated
	 */
	EList<RequirementsSection> getSections();

	/**
	 * Returns the value of the '<em><b>Referencing Sections</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getLinkedRuleSection <em>Linked Rule Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 *  other requirement sections that link to this one (Optional)
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Referencing Sections</em>' reference.
	 * @see #setReferencingSections(RequirementsSectionLinkWithText)
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getTitledRequirementsSection_ReferencingSections()
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getLinkedRuleSection
	 * @model opposite="linkedRuleSection"
	 *        extendedMetaData="kind='attribute' name='referencingSections'"
	 * @generated
	 */
	RequirementsSectionLinkWithText getReferencingSections();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getReferencingSections <em>Referencing Sections</em>}' reference.
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
	 * <!-- begin-model-doc -->
	 * The type, from allowed types, such as Article, Chapter,Rulebook etc
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Requirements Type</em>' reference.
	 * @see #setRequirementsType(RequirementType)
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getTitledRequirementsSection_RequirementsType()
	 * @model required="true"
	 *        extendedMetaData="kind='attribute' name='requirementsType'"
	 * @generated
	 */
	RequirementType getRequirementsType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getRequirementsType <em>Requirements Type</em>}' reference.
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
	 * <!-- begin-model-doc -->
	 * the title of the requirements section
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Title</em>' attribute.
	 * @see #setTitle(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getTitledRequirementsSection_Title()
	 * @model extendedMetaData="kind='attribute' name='title'"
	 * @generated
	 */
	String getTitle();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getTitle <em>Title</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Title</em>' attribute.
	 * @see #getTitle()
	 * @generated
	 */
	void setTitle(String value);

} // TitledRequirementsSection
