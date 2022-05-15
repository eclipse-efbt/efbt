/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text.impl;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.Bpmn_litePackageImpl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.efbt.openregspecs.model.module_management.impl.Module_managementPackageImpl;

import org.eclipse.efbt.openregspecs.model.requirements_text.AllowedTypes;
import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementType;
import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule;
import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSection;
import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSectionImage;
import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSectionLinkWithText;
import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSectionText;
import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textFactory;
import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;
import org.eclipse.efbt.openregspecs.model.requirements_text.Tag;
import org.eclipse.efbt.openregspecs.model.requirements_text.TagGroup;
import org.eclipse.efbt.openregspecs.model.requirements_text.TitledRequirementsSection;

import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage;

import org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl;

import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;

import org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Requirements_textPackageImpl extends EPackageImpl implements Requirements_textPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass allowedTypesEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionImageEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionLinkWithTextEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionTextEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementTypeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass titledRequirementsSectionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tagEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tagGroupEClass = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Requirements_textPackageImpl() {
		super(eNS_URI, Requirements_textFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link Requirements_textPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Requirements_textPackage init() {
		if (isInited) return (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredRequirements_textPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Requirements_textPackageImpl theRequirements_textPackage = registeredRequirements_textPackage instanceof Requirements_textPackageImpl ? (Requirements_textPackageImpl)registeredRequirements_textPackage : new Requirements_textPackageImpl();

		isInited = true;

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Bpmn_litePackageImpl theBpmn_litePackage = (Bpmn_litePackageImpl)(registeredPackage instanceof Bpmn_litePackageImpl ? registeredPackage : Bpmn_litePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Data_meta_modelPackageImpl theData_meta_modelPackage = (Data_meta_modelPackageImpl)(registeredPackage instanceof Data_meta_modelPackageImpl ? registeredPackage : Data_meta_modelPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Module_managementPackageImpl theModule_managementPackage = (Module_managementPackageImpl)(registeredPackage instanceof Module_managementPackageImpl ? registeredPackage : Module_managementPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Sql_litePackage.eNS_URI);
		Sql_litePackageImpl theSql_litePackage = (Sql_litePackageImpl)(registeredPackage instanceof Sql_litePackageImpl ? registeredPackage : Sql_litePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TestingPackage.eNS_URI);
		TestingPackageImpl theTestingPackage = (TestingPackageImpl)(registeredPackage instanceof TestingPackageImpl ? registeredPackage : TestingPackage.eINSTANCE);

		// Create package meta-data objects
		theRequirements_textPackage.createPackageContents();
		theBpmn_litePackage.createPackageContents();
		theData_meta_modelPackage.createPackageContents();
		theModule_managementPackage.createPackageContents();
		theSql_litePackage.createPackageContents();
		theTestingPackage.createPackageContents();

		// Initialize created meta-data
		theRequirements_textPackage.initializePackageContents();
		theBpmn_litePackage.initializePackageContents();
		theData_meta_modelPackage.initializePackageContents();
		theModule_managementPackage.initializePackageContents();
		theSql_litePackage.initializePackageContents();
		theTestingPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theRequirements_textPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Requirements_textPackage.eNS_URI, theRequirements_textPackage);
		return theRequirements_textPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAllowedTypes() {
		return allowedTypesEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAllowedTypes_AllowedTypes() {
		return (EReference)allowedTypesEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsModule() {
		return requirementsModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRequirementsModule_Rules() {
		return (EReference)requirementsModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRequirementsModule_Allowedtypes() {
		return (EReference)requirementsModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSection() {
		return requirementsSectionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSection_Name() {
		return (EAttribute)requirementsSectionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSectionImage() {
		return requirementsSectionImageEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionImage_Style() {
		return (EAttribute)requirementsSectionImageEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionImage_Uri() {
		return (EAttribute)requirementsSectionImageEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSectionLinkWithText() {
		return requirementsSectionLinkWithTextEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRequirementsSectionLinkWithText_LinkedRuleSection() {
		return (EReference)requirementsSectionLinkWithTextEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionLinkWithText_LinkText() {
		return (EAttribute)requirementsSectionLinkWithTextEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionLinkWithText_Subsection() {
		return (EAttribute)requirementsSectionLinkWithTextEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSectionText() {
		return requirementsSectionTextEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionText_Text() {
		return (EAttribute)requirementsSectionTextEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementType() {
		return requirementTypeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementType_Name() {
		return (EAttribute)requirementTypeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTitledRequirementsSection() {
		return titledRequirementsSectionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTitledRequirementsSection_Sections() {
		return (EReference)titledRequirementsSectionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTitledRequirementsSection_ReferencingSections() {
		return (EReference)titledRequirementsSectionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTitledRequirementsSection_RequirementsType() {
		return (EReference)titledRequirementsSectionEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTitledRequirementsSection_Title() {
		return (EAttribute)titledRequirementsSectionEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTag() {
		return tagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTag_DisplayName() {
		return (EAttribute)tagEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTag_Name() {
		return (EAttribute)tagEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTag_Requirements() {
		return (EReference)tagEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTagGroup() {
		return tagGroupEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTagGroup_Tags() {
		return (EReference)tagGroupEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Requirements_textFactory getRequirements_textFactory() {
		return (Requirements_textFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		allowedTypesEClass = createEClass(ALLOWED_TYPES);
		createEReference(allowedTypesEClass, ALLOWED_TYPES__ALLOWED_TYPES);

		requirementsModuleEClass = createEClass(REQUIREMENTS_MODULE);
		createEReference(requirementsModuleEClass, REQUIREMENTS_MODULE__RULES);
		createEReference(requirementsModuleEClass, REQUIREMENTS_MODULE__ALLOWEDTYPES);

		requirementsSectionEClass = createEClass(REQUIREMENTS_SECTION);
		createEAttribute(requirementsSectionEClass, REQUIREMENTS_SECTION__NAME);

		requirementsSectionImageEClass = createEClass(REQUIREMENTS_SECTION_IMAGE);
		createEAttribute(requirementsSectionImageEClass, REQUIREMENTS_SECTION_IMAGE__STYLE);
		createEAttribute(requirementsSectionImageEClass, REQUIREMENTS_SECTION_IMAGE__URI);

		requirementsSectionLinkWithTextEClass = createEClass(REQUIREMENTS_SECTION_LINK_WITH_TEXT);
		createEReference(requirementsSectionLinkWithTextEClass, REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION);
		createEAttribute(requirementsSectionLinkWithTextEClass, REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT);
		createEAttribute(requirementsSectionLinkWithTextEClass, REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION);

		requirementsSectionTextEClass = createEClass(REQUIREMENTS_SECTION_TEXT);
		createEAttribute(requirementsSectionTextEClass, REQUIREMENTS_SECTION_TEXT__TEXT);

		requirementTypeEClass = createEClass(REQUIREMENT_TYPE);
		createEAttribute(requirementTypeEClass, REQUIREMENT_TYPE__NAME);

		titledRequirementsSectionEClass = createEClass(TITLED_REQUIREMENTS_SECTION);
		createEReference(titledRequirementsSectionEClass, TITLED_REQUIREMENTS_SECTION__SECTIONS);
		createEReference(titledRequirementsSectionEClass, TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS);
		createEReference(titledRequirementsSectionEClass, TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE);
		createEAttribute(titledRequirementsSectionEClass, TITLED_REQUIREMENTS_SECTION__TITLE);

		tagEClass = createEClass(TAG);
		createEAttribute(tagEClass, TAG__DISPLAY_NAME);
		createEAttribute(tagEClass, TAG__NAME);
		createEReference(tagEClass, TAG__REQUIREMENTS);

		tagGroupEClass = createEClass(TAG_GROUP);
		createEReference(tagGroupEClass, TAG_GROUP__TAGS);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		requirementsModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		requirementsSectionImageEClass.getESuperTypes().add(this.getRequirementsSection());
		requirementsSectionLinkWithTextEClass.getESuperTypes().add(this.getRequirementsSection());
		requirementsSectionTextEClass.getESuperTypes().add(this.getRequirementsSection());
		titledRequirementsSectionEClass.getESuperTypes().add(this.getRequirementsSection());
		tagGroupEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(allowedTypesEClass, AllowedTypes.class, "AllowedTypes", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAllowedTypes_AllowedTypes(), this.getRequirementType(), null, "allowedTypes", null, 0, -1, AllowedTypes.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(requirementsModuleEClass, RequirementsModule.class, "RequirementsModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRequirementsModule_Rules(), this.getTitledRequirementsSection(), null, "rules", null, 0, -1, RequirementsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRequirementsModule_Allowedtypes(), this.getAllowedTypes(), null, "allowedtypes", null, 0, 1, RequirementsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(requirementsSectionEClass, RequirementsSection.class, "RequirementsSection", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getRequirementsSection_Name(), ecorePackage.getEString(), "name", null, 0, 1, RequirementsSection.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(requirementsSectionImageEClass, RequirementsSectionImage.class, "RequirementsSectionImage", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getRequirementsSectionImage_Style(), ecorePackage.getEString(), "style", null, 0, 1, RequirementsSectionImage.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getRequirementsSectionImage_Uri(), ecorePackage.getEString(), "uri", null, 0, 1, RequirementsSectionImage.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(requirementsSectionLinkWithTextEClass, RequirementsSectionLinkWithText.class, "RequirementsSectionLinkWithText", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRequirementsSectionLinkWithText_LinkedRuleSection(), this.getTitledRequirementsSection(), this.getTitledRequirementsSection_ReferencingSections(), "linkedRuleSection", null, 0, 1, RequirementsSectionLinkWithText.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getRequirementsSectionLinkWithText_LinkText(), ecorePackage.getEString(), "linkText", null, 0, 1, RequirementsSectionLinkWithText.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getRequirementsSectionLinkWithText_Subsection(), ecorePackage.getEString(), "subsection", null, 0, 1, RequirementsSectionLinkWithText.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(requirementsSectionTextEClass, RequirementsSectionText.class, "RequirementsSectionText", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getRequirementsSectionText_Text(), ecorePackage.getEString(), "text", null, 0, 1, RequirementsSectionText.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(requirementTypeEClass, RequirementType.class, "RequirementType", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getRequirementType_Name(), ecorePackage.getEString(), "name", null, 1, 1, RequirementType.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(titledRequirementsSectionEClass, TitledRequirementsSection.class, "TitledRequirementsSection", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTitledRequirementsSection_Sections(), this.getRequirementsSection(), null, "sections", null, 0, -1, TitledRequirementsSection.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTitledRequirementsSection_ReferencingSections(), this.getRequirementsSectionLinkWithText(), this.getRequirementsSectionLinkWithText_LinkedRuleSection(), "referencingSections", null, 0, 1, TitledRequirementsSection.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTitledRequirementsSection_RequirementsType(), this.getRequirementType(), null, "requirementsType", null, 1, 1, TitledRequirementsSection.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTitledRequirementsSection_Title(), ecorePackage.getEString(), "title", null, 0, 1, TitledRequirementsSection.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(tagEClass, Tag.class, "Tag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTag_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, Tag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTag_Name(), ecorePackage.getEString(), "name", null, 0, 1, Tag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTag_Requirements(), this.getTitledRequirementsSection(), null, "requirements", null, 0, -1, Tag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(tagGroupEClass, TagGroup.class, "TagGroup", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTagGroup_Tags(), this.getTag(), null, "tags", null, 0, -1, TagGroup.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
	}

	/**
	 * Initializes the annotations for <b>license</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createLicenseAnnotations() {
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (allowedTypesEClass,
		   source,
		   new String[] {
			   "name", "AllowedTypes",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAllowedTypes_AllowedTypes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "allowedTypes"
		   });
		addAnnotation
		  (requirementsModuleEClass,
		   source,
		   new String[] {
			   "name", "RequirementsModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getRequirementsModule_Rules(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "rules"
		   });
		addAnnotation
		  (getRequirementsModule_Allowedtypes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "allowedtypes"
		   });
		addAnnotation
		  (requirementsSectionEClass,
		   source,
		   new String[] {
			   "name", "RequirementsSection",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRequirementsSection_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "iD"
		   });
		addAnnotation
		  (requirementsSectionImageEClass,
		   source,
		   new String[] {
			   "name", "RequirementsSectionImage",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRequirementsSectionImage_Style(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "style"
		   });
		addAnnotation
		  (getRequirementsSectionImage_Uri(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "uri"
		   });
		addAnnotation
		  (requirementsSectionLinkWithTextEClass,
		   source,
		   new String[] {
			   "name", "RequirementsSectionLinkWithText",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRequirementsSectionLinkWithText_LinkedRuleSection(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "linkedRuleSection"
		   });
		addAnnotation
		  (getRequirementsSectionLinkWithText_LinkText(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "linkText"
		   });
		addAnnotation
		  (getRequirementsSectionLinkWithText_Subsection(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "subsection"
		   });
		addAnnotation
		  (requirementsSectionTextEClass,
		   source,
		   new String[] {
			   "name", "RequirementsSectionText",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRequirementsSectionText_Text(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "text"
		   });
		addAnnotation
		  (requirementTypeEClass,
		   source,
		   new String[] {
			   "name", "RequirementType",
			   "kind", "empty"
		   });
		addAnnotation
		  (getRequirementType_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "requirementType"
		   });
		addAnnotation
		  (titledRequirementsSectionEClass,
		   source,
		   new String[] {
			   "name", "TitledRequirementsSection",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getTitledRequirementsSection_Sections(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "sections"
		   });
		addAnnotation
		  (getTitledRequirementsSection_ReferencingSections(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "referencingSections"
		   });
		addAnnotation
		  (getTitledRequirementsSection_RequirementsType(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "requirementsType"
		   });
		addAnnotation
		  (getTitledRequirementsSection_Title(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "title"
		   });
	}

} //Requirements_textPackageImpl
