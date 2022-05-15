/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage
 * @generated
 */
public interface Requirements_textFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Requirements_textFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.requirements_text.impl.Requirements_textFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Allowed Types</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Allowed Types</em>'.
	 * @generated
	 */
	AllowedTypes createAllowedTypes();

	/**
	 * Returns a new object of class '<em>Requirements Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Requirements Module</em>'.
	 * @generated
	 */
	RequirementsModule createRequirementsModule();

	/**
	 * Returns a new object of class '<em>Requirements Section Image</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Requirements Section Image</em>'.
	 * @generated
	 */
	RequirementsSectionImage createRequirementsSectionImage();

	/**
	 * Returns a new object of class '<em>Requirements Section Link With Text</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Requirements Section Link With Text</em>'.
	 * @generated
	 */
	RequirementsSectionLinkWithText createRequirementsSectionLinkWithText();

	/**
	 * Returns a new object of class '<em>Requirements Section Text</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Requirements Section Text</em>'.
	 * @generated
	 */
	RequirementsSectionText createRequirementsSectionText();

	/**
	 * Returns a new object of class '<em>Requirement Type</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Requirement Type</em>'.
	 * @generated
	 */
	RequirementType createRequirementType();

	/**
	 * Returns a new object of class '<em>Titled Requirements Section</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Titled Requirements Section</em>'.
	 * @generated
	 */
	TitledRequirementsSection createTitledRequirementsSection();

	/**
	 * Returns a new object of class '<em>Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Tag</em>'.
	 * @generated
	 */
	Tag createTag();

	/**
	 * Returns a new object of class '<em>Tag Group</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Tag Group</em>'.
	 * @generated
	 */
	TagGroup createTagGroup();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Requirements_textPackage getRequirements_textPackage();

} //Requirements_textFactory
