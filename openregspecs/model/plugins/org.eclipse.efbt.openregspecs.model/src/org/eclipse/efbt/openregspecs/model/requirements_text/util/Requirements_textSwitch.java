/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text.util;

import org.eclipse.efbt.openregspecs.model.requirements_text.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage
 * @generated
 */
public class Requirements_textSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Requirements_textPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Requirements_textSwitch() {
		if (modelPackage == null) {
			modelPackage = Requirements_textPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case Requirements_textPackage.ALLOWED_TYPES: {
				AllowedTypes allowedTypes = (AllowedTypes)theEObject;
				T result = caseAllowedTypes(allowedTypes);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.REQUIREMENTS_MODULE: {
				RequirementsModule requirementsModule = (RequirementsModule)theEObject;
				T result = caseRequirementsModule(requirementsModule);
				if (result == null) result = caseModule(requirementsModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.REQUIREMENTS_SECTION: {
				RequirementsSection requirementsSection = (RequirementsSection)theEObject;
				T result = caseRequirementsSection(requirementsSection);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.REQUIREMENTS_SECTION_IMAGE: {
				RequirementsSectionImage requirementsSectionImage = (RequirementsSectionImage)theEObject;
				T result = caseRequirementsSectionImage(requirementsSectionImage);
				if (result == null) result = caseRequirementsSection(requirementsSectionImage);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT: {
				RequirementsSectionLinkWithText requirementsSectionLinkWithText = (RequirementsSectionLinkWithText)theEObject;
				T result = caseRequirementsSectionLinkWithText(requirementsSectionLinkWithText);
				if (result == null) result = caseRequirementsSection(requirementsSectionLinkWithText);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.REQUIREMENTS_SECTION_TEXT: {
				RequirementsSectionText requirementsSectionText = (RequirementsSectionText)theEObject;
				T result = caseRequirementsSectionText(requirementsSectionText);
				if (result == null) result = caseRequirementsSection(requirementsSectionText);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.REQUIREMENT_TYPE: {
				RequirementType requirementType = (RequirementType)theEObject;
				T result = caseRequirementType(requirementType);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.TITLED_REQUIREMENTS_SECTION: {
				TitledRequirementsSection titledRequirementsSection = (TitledRequirementsSection)theEObject;
				T result = caseTitledRequirementsSection(titledRequirementsSection);
				if (result == null) result = caseRequirementsSection(titledRequirementsSection);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.TAG: {
				Tag tag = (Tag)theEObject;
				T result = caseTag(tag);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Requirements_textPackage.TAG_GROUP: {
				TagGroup tagGroup = (TagGroup)theEObject;
				T result = caseTagGroup(tagGroup);
				if (result == null) result = caseModule(tagGroup);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Allowed Types</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Allowed Types</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAllowedTypes(AllowedTypes object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsModule(RequirementsModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSection(RequirementsSection object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section Image</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section Image</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSectionImage(RequirementsSectionImage object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section Link With Text</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section Link With Text</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSectionLinkWithText(RequirementsSectionLinkWithText object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section Text</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section Text</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSectionText(RequirementsSectionText object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirement Type</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirement Type</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementType(RequirementType object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Titled Requirements Section</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Titled Requirements Section</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTitledRequirementsSection(TitledRequirementsSection object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTag(Tag object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Tag Group</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Tag Group</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTagGroup(TagGroup object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.openregspecs.model.module_management.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //Requirements_textSwitch
