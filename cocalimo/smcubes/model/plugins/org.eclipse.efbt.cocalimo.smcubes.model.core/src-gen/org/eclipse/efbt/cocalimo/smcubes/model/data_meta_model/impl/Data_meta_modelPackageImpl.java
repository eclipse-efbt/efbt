/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.Cocalimo_smcubes_extensionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BasicEntity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Classifier;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Element;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Entity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.NamedElement;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.StructuralFeature;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.TypedElement;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl;

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
public class Data_meta_modelPackageImpl extends EPackageImpl implements Data_meta_modelPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass foreignKeyAttributeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass classifierEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elementEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass namedElementEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass entityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass structuralFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass typedElementEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass generatedEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass entityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicEntityEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Data_meta_modelPackageImpl() {
		super(eNS_URI, Data_meta_modelFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Data_meta_modelPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Data_meta_modelPackage init() {
		if (isInited) return (Data_meta_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredData_meta_modelPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Data_meta_modelPackageImpl theData_meta_modelPackage = registeredData_meta_modelPackage instanceof Data_meta_modelPackageImpl ? (Data_meta_modelPackageImpl)registeredData_meta_modelPackage : new Data_meta_modelPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		MappingPackageImpl theMappingPackage = (MappingPackageImpl)(registeredPackage instanceof MappingPackageImpl ? registeredPackage : MappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		RenderingPackageImpl theRenderingPackage = (RenderingPackageImpl)(registeredPackage instanceof RenderingPackageImpl ? registeredPackage : RenderingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cocalimo_smcubes_extensionPackage.eNS_URI);
		Cocalimo_smcubes_extensionPackageImpl theCocalimo_smcubes_extensionPackage = (Cocalimo_smcubes_extensionPackageImpl)(registeredPackage instanceof Cocalimo_smcubes_extensionPackageImpl ? registeredPackage : Cocalimo_smcubes_extensionPackage.eINSTANCE);

		// Create package meta-data objects
		theData_meta_modelPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theCocalimo_smcubes_extensionPackage.createPackageContents();

		// Initialize created meta-data
		theData_meta_modelPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theCocalimo_smcubes_extensionPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theData_meta_modelPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Data_meta_modelPackage.eNS_URI, theData_meta_modelPackage);
		return theData_meta_modelPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAttribute() {
		return attributeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getAttribute_IsPK() {
		return (EAttribute)attributeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAttribute_Variable() {
		return (EReference)attributeEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getForeignKeyAttribute() {
		return foreignKeyAttributeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getForeignKeyAttribute_Entity() {
		return (EReference)foreignKeyAttributeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getForeignKeyAttribute_Containment() {
		return (EAttribute)foreignKeyAttributeEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getForeignKeyAttribute_Cardinality() {
		return (EAttribute)foreignKeyAttributeEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getForeignKeyAttribute_Mandatory() {
		return (EAttribute)foreignKeyAttributeEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getClassifier() {
		return classifierEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getElement() {
		return elementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getNamedElement() {
		return namedElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getNamedElement_Name() {
		return (EAttribute)namedElementEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getEntityModule() {
		return entityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getEntityModule_NsURI() {
		return (EAttribute)entityModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getEntityModule_NsPrefix() {
		return (EAttribute)entityModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getEntityModule_Entities() {
		return (EReference)entityModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getStructuralFeature() {
		return structuralFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTypedElement() {
		return typedElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTypedElement_Classifier() {
		return (EReference)typedElementEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGeneratedEntity() {
		return generatedEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGeneratedEntity_Attributes() {
		return (EReference)generatedEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getEntity() {
		return entityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDerivedEntity() {
		return derivedEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDerivedEntity_Attributes() {
		return (EReference)derivedEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBasicEntity() {
		return basicEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBasicEntity_Attributes() {
		return (EReference)basicEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBasicEntity_SuperClass() {
		return (EReference)basicEntityEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Data_meta_modelFactory getData_meta_modelFactory() {
		return (Data_meta_modelFactory)getEFactoryInstance();
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
		attributeEClass = createEClass(ATTRIBUTE);
		createEAttribute(attributeEClass, ATTRIBUTE__IS_PK);
		createEReference(attributeEClass, ATTRIBUTE__VARIABLE);

		foreignKeyAttributeEClass = createEClass(FOREIGN_KEY_ATTRIBUTE);
		createEReference(foreignKeyAttributeEClass, FOREIGN_KEY_ATTRIBUTE__ENTITY);
		createEAttribute(foreignKeyAttributeEClass, FOREIGN_KEY_ATTRIBUTE__CONTAINMENT);
		createEAttribute(foreignKeyAttributeEClass, FOREIGN_KEY_ATTRIBUTE__CARDINALITY);
		createEAttribute(foreignKeyAttributeEClass, FOREIGN_KEY_ATTRIBUTE__MANDATORY);

		classifierEClass = createEClass(CLASSIFIER);

		elementEClass = createEClass(ELEMENT);

		namedElementEClass = createEClass(NAMED_ELEMENT);
		createEAttribute(namedElementEClass, NAMED_ELEMENT__NAME);

		entityModuleEClass = createEClass(ENTITY_MODULE);
		createEAttribute(entityModuleEClass, ENTITY_MODULE__NS_URI);
		createEAttribute(entityModuleEClass, ENTITY_MODULE__NS_PREFIX);
		createEReference(entityModuleEClass, ENTITY_MODULE__ENTITIES);

		structuralFeatureEClass = createEClass(STRUCTURAL_FEATURE);

		typedElementEClass = createEClass(TYPED_ELEMENT);
		createEReference(typedElementEClass, TYPED_ELEMENT__CLASSIFIER);

		generatedEntityEClass = createEClass(GENERATED_ENTITY);
		createEReference(generatedEntityEClass, GENERATED_ENTITY__ATTRIBUTES);

		entityEClass = createEClass(ENTITY);

		derivedEntityEClass = createEClass(DERIVED_ENTITY);
		createEReference(derivedEntityEClass, DERIVED_ENTITY__ATTRIBUTES);

		basicEntityEClass = createEClass(BASIC_ENTITY);
		createEReference(basicEntityEClass, BASIC_ENTITY__ATTRIBUTES);
		createEReference(basicEntityEClass, BASIC_ENTITY__SUPER_CLASS);
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
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		attributeEClass.getESuperTypes().add(this.getStructuralFeature());
		foreignKeyAttributeEClass.getESuperTypes().add(this.getAttribute());
		classifierEClass.getESuperTypes().add(this.getNamedElement());
		namedElementEClass.getESuperTypes().add(this.getElement());
		entityModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		structuralFeatureEClass.getESuperTypes().add(this.getTypedElement());
		typedElementEClass.getESuperTypes().add(this.getNamedElement());
		generatedEntityEClass.getESuperTypes().add(this.getEntity());
		entityEClass.getESuperTypes().add(this.getNamedElement());
		derivedEntityEClass.getESuperTypes().add(this.getEntity());
		basicEntityEClass.getESuperTypes().add(this.getEntity());

		// Initialize classes, features, and operations; add parameters
		initEClass(attributeEClass, Attribute.class, "Attribute", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getAttribute_IsPK(), ecorePackage.getEBoolean(), "isPK", null, 0, 1, Attribute.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAttribute_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, Attribute.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(foreignKeyAttributeEClass, ForeignKeyAttribute.class, "ForeignKeyAttribute", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getForeignKeyAttribute_Entity(), this.getBasicEntity(), null, "entity", null, 0, 1, ForeignKeyAttribute.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getForeignKeyAttribute_Containment(), ecorePackage.getEBoolean(), "containment", null, 0, 1, ForeignKeyAttribute.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getForeignKeyAttribute_Cardinality(), ecorePackage.getEInt(), "cardinality", null, 0, 1, ForeignKeyAttribute.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getForeignKeyAttribute_Mandatory(), ecorePackage.getEBoolean(), "mandatory", null, 0, 1, ForeignKeyAttribute.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(classifierEClass, Classifier.class, "Classifier", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(elementEClass, Element.class, "Element", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(namedElementEClass, NamedElement.class, "NamedElement", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getNamedElement_Name(), ecorePackage.getEString(), "name", null, 0, 1, NamedElement.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(entityModuleEClass, EntityModule.class, "EntityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getEntityModule_NsURI(), ecorePackage.getEString(), "nsURI", null, 0, 1, EntityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getEntityModule_NsPrefix(), ecorePackage.getEString(), "nsPrefix", null, 0, 1, EntityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getEntityModule_Entities(), this.getEntity(), null, "entities", null, 0, -1, EntityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(structuralFeatureEClass, StructuralFeature.class, "StructuralFeature", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(typedElementEClass, TypedElement.class, "TypedElement", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTypedElement_Classifier(), this.getClassifier(), null, "classifier", null, 0, 1, TypedElement.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(generatedEntityEClass, GeneratedEntity.class, "GeneratedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGeneratedEntity_Attributes(), this.getAttribute(), null, "attributes", null, 0, -1, GeneratedEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(entityEClass, Entity.class, "Entity", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(derivedEntityEClass, DerivedEntity.class, "DerivedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedEntity_Attributes(), this.getAttribute(), null, "attributes", null, 0, -1, DerivedEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicEntityEClass, BasicEntity.class, "BasicEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicEntity_Attributes(), this.getAttribute(), null, "attributes", null, 0, -1, BasicEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBasicEntity_SuperClass(), this.getBasicEntity(), null, "superClass", null, 0, 1, BasicEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);
	}

} //Data_meta_modelPackageImpl
