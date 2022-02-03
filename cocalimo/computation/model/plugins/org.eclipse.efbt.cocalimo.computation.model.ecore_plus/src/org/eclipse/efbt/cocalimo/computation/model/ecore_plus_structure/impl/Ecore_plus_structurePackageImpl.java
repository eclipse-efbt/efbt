/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Cell;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.CellsParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structureFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.EnumeratedDomain;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.HeirarchicalEnum;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.HeirarchicalEnumLiteral;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.SubDomainEnum;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_structurePackageImpl extends EPackageImpl implements Ecore_plus_structurePackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass heirarchicalEnumEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass heirarchicalEnumLiteralEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass enumeratedDomainEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass subDomainEnumEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellsParameterEClass = null;

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
	private EClass baseEntityFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseEntityModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Ecore_plus_structurePackageImpl() {
		super(eNS_URI, Ecore_plus_structureFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Ecore_plus_structurePackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Ecore_plus_structurePackage init() {
		if (isInited) return (Ecore_plus_structurePackage)EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_structurePackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEcore_plus_structurePackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Ecore_plus_structurePackageImpl theEcore_plus_structurePackage = registeredEcore_plus_structurePackage instanceof Ecore_plus_structurePackageImpl ? (Ecore_plus_structurePackageImpl)registeredEcore_plus_structurePackage : new Ecore_plus_structurePackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		Logical_transformationsPackage.eINSTANCE.eClass();
		Bpmn_litePackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_logicPackage.eNS_URI);
		Ecore_plus_logicPackageImpl theEcore_plus_logicPackage = (Ecore_plus_logicPackageImpl)(registeredPackage instanceof Ecore_plus_logicPackageImpl ? registeredPackage : Ecore_plus_logicPackage.eINSTANCE);

		// Create package meta-data objects
		theEcore_plus_structurePackage.createPackageContents();
		theEcore_plus_logicPackage.createPackageContents();

		// Initialize created meta-data
		theEcore_plus_structurePackage.initializePackageContents();
		theEcore_plus_logicPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theEcore_plus_structurePackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Ecore_plus_structurePackage.eNS_URI, theEcore_plus_structurePackage);
		return theEcore_plus_structurePackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBaseEntity() {
		return baseEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBaseEntity_BaseFeatures() {
		return (EReference)baseEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBaseEntity_Cube() {
		return (EReference)baseEntityEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCell() {
		return cellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCell_Row() {
		return (EReference)cellEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCell_Column() {
		return (EReference)cellEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getHeirarchicalEnum() {
		return heirarchicalEnumEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getHeirarchicalEnumLiteral() {
		return heirarchicalEnumLiteralEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getHeirarchicalEnumLiteral_ParentLiteral() {
		return (EReference)heirarchicalEnumLiteralEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getEnumeratedDomain() {
		return enumeratedDomainEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getEnumeratedDomain_Domain() {
		return (EReference)enumeratedDomainEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSubDomainEnum() {
		return subDomainEnumEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSubDomainEnum_PrimaryEnum() {
		return (EReference)subDomainEnumEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSubDomainEnum_SubDomain() {
		return (EReference)subDomainEnumEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCellsParameter() {
		return cellsParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCellsParameter_Cells() {
		return (EReference)cellsParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getEntity() {
		return entityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBaseEntityFeature() {
		return baseEntityFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getBaseEntityFeature_IsFK() {
		return (EAttribute)baseEntityFeatureEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBaseEntityFeature_FkEntity() {
		return (EReference)baseEntityFeatureEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBaseEntityFeature_Varaible() {
		return (EReference)baseEntityFeatureEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBaseEntityModule() {
		return baseEntityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBaseEntityModule_BaseEntities() {
		return (EReference)baseEntityModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_structureFactory getEcore_plus_structureFactory() {
		return (Ecore_plus_structureFactory)getEFactoryInstance();
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
		baseEntityEClass = createEClass(BASE_ENTITY);
		createEReference(baseEntityEClass, BASE_ENTITY__BASE_FEATURES);
		createEReference(baseEntityEClass, BASE_ENTITY__CUBE);

		cellEClass = createEClass(CELL);
		createEReference(cellEClass, CELL__ROW);
		createEReference(cellEClass, CELL__COLUMN);

		heirarchicalEnumEClass = createEClass(HEIRARCHICAL_ENUM);

		heirarchicalEnumLiteralEClass = createEClass(HEIRARCHICAL_ENUM_LITERAL);
		createEReference(heirarchicalEnumLiteralEClass, HEIRARCHICAL_ENUM_LITERAL__PARENT_LITERAL);

		enumeratedDomainEClass = createEClass(ENUMERATED_DOMAIN);
		createEReference(enumeratedDomainEClass, ENUMERATED_DOMAIN__DOMAIN);

		subDomainEnumEClass = createEClass(SUB_DOMAIN_ENUM);
		createEReference(subDomainEnumEClass, SUB_DOMAIN_ENUM__PRIMARY_ENUM);
		createEReference(subDomainEnumEClass, SUB_DOMAIN_ENUM__SUB_DOMAIN);

		cellsParameterEClass = createEClass(CELLS_PARAMETER);
		createEReference(cellsParameterEClass, CELLS_PARAMETER__CELLS);

		entityEClass = createEClass(ENTITY);

		baseEntityFeatureEClass = createEClass(BASE_ENTITY_FEATURE);
		createEAttribute(baseEntityFeatureEClass, BASE_ENTITY_FEATURE__IS_FK);
		createEReference(baseEntityFeatureEClass, BASE_ENTITY_FEATURE__FK_ENTITY);
		createEReference(baseEntityFeatureEClass, BASE_ENTITY_FEATURE__VARAIBLE);

		baseEntityModuleEClass = createEClass(BASE_ENTITY_MODULE);
		createEReference(baseEntityModuleEClass, BASE_ENTITY_MODULE__BASE_ENTITIES);
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
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		baseEntityEClass.getESuperTypes().add(this.getEntity());
		heirarchicalEnumEClass.getESuperTypes().add(theEcorePackage.getEEnum());
		heirarchicalEnumLiteralEClass.getESuperTypes().add(theEcorePackage.getEEnumLiteral());
		enumeratedDomainEClass.getESuperTypes().add(this.getHeirarchicalEnum());
		subDomainEnumEClass.getESuperTypes().add(this.getHeirarchicalEnum());
		entityEClass.getESuperTypes().add(theEcorePackage.getEClass());
		baseEntityFeatureEClass.getESuperTypes().add(theEcorePackage.getEAttribute());
		baseEntityModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(baseEntityEClass, BaseEntity.class, "BaseEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseEntity_BaseFeatures(), this.getBaseEntityFeature(), null, "baseFeatures", null, 0, -1, BaseEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBaseEntity_Cube(), theData_definitionPackage.getCUBE(), null, "cube", null, 0, 1, BaseEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cellEClass, Cell.class, "Cell", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCell_Row(), theEcorePackage.getEObject(), null, "row", null, 0, 1, Cell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCell_Column(), theEcorePackage.getEStructuralFeature(), null, "column", null, 0, 1, Cell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(heirarchicalEnumEClass, HeirarchicalEnum.class, "HeirarchicalEnum", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(heirarchicalEnumLiteralEClass, HeirarchicalEnumLiteral.class, "HeirarchicalEnumLiteral", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getHeirarchicalEnumLiteral_ParentLiteral(), theEcorePackage.getEEnumLiteral(), null, "parentLiteral", null, 0, 1, HeirarchicalEnumLiteral.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(enumeratedDomainEClass, EnumeratedDomain.class, "EnumeratedDomain", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getEnumeratedDomain_Domain(), theCorePackage.getDOMAIN(), null, "domain", null, 0, 1, EnumeratedDomain.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(subDomainEnumEClass, SubDomainEnum.class, "SubDomainEnum", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSubDomainEnum_PrimaryEnum(), this.getEnumeratedDomain(), null, "primaryEnum", null, 0, 1, SubDomainEnum.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSubDomainEnum_SubDomain(), theCorePackage.getSUBDOMAIN(), null, "subDomain", null, 0, 1, SubDomainEnum.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cellsParameterEClass, CellsParameter.class, "CellsParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCellsParameter_Cells(), this.getCell(), null, "cells", null, 0, 1, CellsParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(entityEClass, Entity.class, "Entity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(baseEntityFeatureEClass, BaseEntityFeature.class, "BaseEntityFeature", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getBaseEntityFeature_IsFK(), ecorePackage.getEBoolean(), "isFK", null, 0, 1, BaseEntityFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBaseEntityFeature_FkEntity(), this.getBaseEntity(), null, "fkEntity", null, 0, 1, BaseEntityFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBaseEntityFeature_Varaible(), theCorePackage.getVARIABLE(), null, "varaible", null, 0, 1, BaseEntityFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseEntityModuleEClass, BaseEntityModule.class, "BaseEntityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseEntityModule_BaseEntities(), this.getBaseEntity(), null, "baseEntities", null, 0, -1, BaseEntityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
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
		  (getCellsParameter_Cells(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cells"
		   });
	}

} //Ecore_plus_structurePackageImpl
