/**
 */
package org.eclipse.efbt.cocalimo.core.model.requirements_text.impl;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Requirements_textFactoryImpl extends EFactoryImpl implements Requirements_textFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Requirements_textFactory init()
	{
		try {
			Requirements_textFactory theRequirements_textFactory = (Requirements_textFactory)EPackage.Registry.INSTANCE.getEFactory(Requirements_textPackage.eNS_URI);
			if (theRequirements_textFactory != null) {
				return theRequirements_textFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Requirements_textFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Requirements_textFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID()) {
			case Requirements_textPackage.ALLOWED_TYPES: return createAllowedTypes();
			case Requirements_textPackage.REQUIREMENTS_MODULE: return createRequirementsModule();
			case Requirements_textPackage.REQUIREMENTS_SECTION_IMAGE: return createRequirementsSectionImage();
			case Requirements_textPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT: return createRequirementsSectionLinkWithText();
			case Requirements_textPackage.REQUIREMENTS_SECTION_TEXT: return createRequirementsSectionText();
			case Requirements_textPackage.REQUIREMENT_TYPE: return createRequirementType();
			case Requirements_textPackage.TITLED_REQUIREMENTS_SECTION: return createTitledRequirementsSection();
			case Requirements_textPackage.TAG: return createTag();
			case Requirements_textPackage.TAG_GROUP: return createTagGroup();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AllowedTypes createAllowedTypes()
	{
		AllowedTypesImpl allowedTypes = new AllowedTypesImpl();
		return allowedTypes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RequirementsModule createRequirementsModule()
	{
		RequirementsModuleImpl requirementsModule = new RequirementsModuleImpl();
		return requirementsModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RequirementsSectionImage createRequirementsSectionImage()
	{
		RequirementsSectionImageImpl requirementsSectionImage = new RequirementsSectionImageImpl();
		return requirementsSectionImage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RequirementsSectionLinkWithText createRequirementsSectionLinkWithText()
	{
		RequirementsSectionLinkWithTextImpl requirementsSectionLinkWithText = new RequirementsSectionLinkWithTextImpl();
		return requirementsSectionLinkWithText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RequirementsSectionText createRequirementsSectionText()
	{
		RequirementsSectionTextImpl requirementsSectionText = new RequirementsSectionTextImpl();
		return requirementsSectionText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RequirementType createRequirementType()
	{
		RequirementTypeImpl requirementType = new RequirementTypeImpl();
		return requirementType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TitledRequirementsSection createTitledRequirementsSection()
	{
		TitledRequirementsSectionImpl titledRequirementsSection = new TitledRequirementsSectionImpl();
		return titledRequirementsSection;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Tag createTag()
	{
		TagImpl tag = new TagImpl();
		return tag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TagGroup createTagGroup()
	{
		TagGroupImpl tagGroup = new TagGroupImpl();
		return tagGroup;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Requirements_textPackage getRequirements_textPackage()
	{
		return (Requirements_textPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Requirements_textPackage getPackage()
	{
		return Requirements_textPackage.eINSTANCE;
	}

} //Requirements_textFactoryImpl
