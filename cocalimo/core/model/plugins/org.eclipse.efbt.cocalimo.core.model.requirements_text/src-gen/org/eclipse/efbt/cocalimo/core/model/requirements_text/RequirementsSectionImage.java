/**
 */
package org.eclipse.efbt.cocalimo.core.model.requirements_text;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Requirements Section Image</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Requirements Section represented by an image
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getStyle <em>Style</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getUri <em>Uri</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getRequirementsSectionImage()
 * @model extendedMetaData="name='RequirementsSectionImage' kind='empty'"
 * @generated
 */
public interface RequirementsSectionImage extends RequirementsSection
{
	/**
	 * Returns the value of the '<em><b>Style</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the style string including width and height
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Style</em>' attribute.
	 * @see #setStyle(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getRequirementsSectionImage_Style()
	 * @model extendedMetaData="kind='attribute' name='style'"
	 * @generated
	 */
	String getStyle();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getStyle <em>Style</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Style</em>' attribute.
	 * @see #getStyle()
	 * @generated
	 */
	void setStyle(String value);

	/**
	 * Returns the value of the '<em><b>Uri</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the location of the image file
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Uri</em>' attribute.
	 * @see #setUri(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage#getRequirementsSectionImage_Uri()
	 * @model extendedMetaData="kind='attribute' name='uri'"
	 * @generated
	 */
	String getUri();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getUri <em>Uri</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Uri</em>' attribute.
	 * @see #getUri()
	 * @generated
	 */
	void setUri(String value);

} // RequirementsSectionImage
