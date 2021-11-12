/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.Cocalimo_smcubes_extensionPackageImpl;
import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

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
public class MappingPackageImpl extends EPackageImpl implements MappingPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_MAPPINGEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass membeR_MAPPINGEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass membeR_MAPPING_ITEMEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variablE_MAPPING_ITEMEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variablE_MAPPINGEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass mappinG_TO_CUBEEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variablE_SET_MAPPINGEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass mappinG_DEFINITIONEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private MappingPackageImpl() {
		super(eNS_URI, MappingFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link MappingPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static MappingPackage init() {
		if (isInited) return (MappingPackage)EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredMappingPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		MappingPackageImpl theMappingPackage = registeredMappingPackage instanceof MappingPackageImpl ? (MappingPackageImpl)registeredMappingPackage : new MappingPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		RenderingPackageImpl theRenderingPackage = (RenderingPackageImpl)(registeredPackage instanceof RenderingPackageImpl ? registeredPackage : RenderingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cocalimo_smcubes_extensionPackage.eNS_URI);
		Cocalimo_smcubes_extensionPackageImpl theCocalimo_smcubes_extensionPackage = (Cocalimo_smcubes_extensionPackageImpl)(registeredPackage instanceof Cocalimo_smcubes_extensionPackageImpl ? registeredPackage : Cocalimo_smcubes_extensionPackage.eINSTANCE);

		// Create package meta-data objects
		theMappingPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theCocalimo_smcubes_extensionPackage.createPackageContents();

		// Initialize created meta-data
		theMappingPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theCocalimo_smcubes_extensionPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theMappingPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(MappingPackage.eNS_URI, theMappingPackage);
		return theMappingPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_MAPPING() {
		return cubE_MAPPINGEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_MAPPING_Cube_mapping_id() {
		return (EAttribute)cubE_MAPPINGEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_MAPPING_Description() {
		return (EAttribute)cubE_MAPPINGEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_MAPPING_Source_cube() {
		return (EReference)cubE_MAPPINGEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_MAPPING_Target_cube() {
		return (EReference)cubE_MAPPINGEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_MAPPING_Name() {
		return (EAttribute)cubE_MAPPINGEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_MAPPING_Code() {
		return (EAttribute)cubE_MAPPINGEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_MAPPING_Maintenance_agency_id() {
		return (EAttribute)cubE_MAPPINGEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_MAPPING_MappingsToCube() {
		return (EReference)cubE_MAPPINGEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMEMBER_MAPPING() {
		return membeR_MAPPINGEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_MAPPING_Name() {
		return (EAttribute)membeR_MAPPINGEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_MAPPING_Code() {
		return (EAttribute)membeR_MAPPINGEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_MAPPING_Maintenance_agency_id() {
		return (EAttribute)membeR_MAPPINGEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_MAPPING_Member_mapping_id() {
		return (EAttribute)membeR_MAPPINGEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_MAPPING_MemberMappingItems() {
		return (EReference)membeR_MAPPINGEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMEMBER_MAPPING_ITEM() {
		return membeR_MAPPING_ITEMEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_MAPPING_ITEM_IsSource() {
		return (EAttribute)membeR_MAPPING_ITEMEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_MAPPING_ITEM_Member() {
		return (EReference)membeR_MAPPING_ITEMEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_MAPPING_ITEM_Variable() {
		return (EReference)membeR_MAPPING_ITEMEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVARIABLE_MAPPING_ITEM() {
		return variablE_MAPPING_ITEMEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_MAPPING_ITEM_IsSource() {
		return (EAttribute)variablE_MAPPING_ITEMEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_MAPPING_ITEM_Variable() {
		return (EReference)variablE_MAPPING_ITEMEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVARIABLE_MAPPING() {
		return variablE_MAPPINGEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_MAPPING_VaraibleMappingItems() {
		return (EReference)variablE_MAPPINGEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_MAPPING_Name() {
		return (EAttribute)variablE_MAPPINGEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_MAPPING_Code() {
		return (EAttribute)variablE_MAPPINGEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_MAPPING_Maintenance_agency_id() {
		return (EAttribute)variablE_MAPPINGEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_MAPPING_Variable_mapping_id() {
		return (EAttribute)variablE_MAPPINGEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMAPPING_TO_CUBE() {
		return mappinG_TO_CUBEEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMAPPING_TO_CUBE_Mapping() {
		return (EReference)mappinG_TO_CUBEEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVARIABLE_SET_MAPPING() {
		return variablE_SET_MAPPINGEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_SET_MAPPING_SourceMapping() {
		return (EReference)variablE_SET_MAPPINGEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_SET_MAPPING_TargetMapping() {
		return (EReference)variablE_SET_MAPPINGEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMAPPING_DEFINITION() {
		return mappinG_DEFINITIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAPPING_DEFINITION_Name() {
		return (EAttribute)mappinG_DEFINITIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAPPING_DEFINITION_Code() {
		return (EAttribute)mappinG_DEFINITIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAPPING_DEFINITION_Maintenance_agency_id() {
		return (EAttribute)mappinG_DEFINITIONEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAPPING_DEFINITION_Mapping_id() {
		return (EAttribute)mappinG_DEFINITIONEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAPPING_DEFINITION_Mapping_type() {
		return (EAttribute)mappinG_DEFINITIONEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMAPPING_DEFINITION_MemberMapping() {
		return (EReference)mappinG_DEFINITIONEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMAPPING_DEFINITION_VariableMapping() {
		return (EReference)mappinG_DEFINITIONEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MappingFactory getMappingFactory() {
		return (MappingFactory)getEFactoryInstance();
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
		cubE_MAPPINGEClass = createEClass(CUBE_MAPPING);
		createEAttribute(cubE_MAPPINGEClass, CUBE_MAPPING__CUBE_MAPPING_ID);
		createEAttribute(cubE_MAPPINGEClass, CUBE_MAPPING__DESCRIPTION);
		createEReference(cubE_MAPPINGEClass, CUBE_MAPPING__SOURCE_CUBE);
		createEReference(cubE_MAPPINGEClass, CUBE_MAPPING__TARGET_CUBE);
		createEAttribute(cubE_MAPPINGEClass, CUBE_MAPPING__NAME);
		createEAttribute(cubE_MAPPINGEClass, CUBE_MAPPING__CODE);
		createEAttribute(cubE_MAPPINGEClass, CUBE_MAPPING__MAINTENANCE_AGENCY_ID);
		createEReference(cubE_MAPPINGEClass, CUBE_MAPPING__MAPPINGS_TO_CUBE);

		membeR_MAPPINGEClass = createEClass(MEMBER_MAPPING);
		createEAttribute(membeR_MAPPINGEClass, MEMBER_MAPPING__NAME);
		createEAttribute(membeR_MAPPINGEClass, MEMBER_MAPPING__CODE);
		createEAttribute(membeR_MAPPINGEClass, MEMBER_MAPPING__MAINTENANCE_AGENCY_ID);
		createEAttribute(membeR_MAPPINGEClass, MEMBER_MAPPING__MEMBER_MAPPING_ID);
		createEReference(membeR_MAPPINGEClass, MEMBER_MAPPING__MEMBER_MAPPING_ITEMS);

		membeR_MAPPING_ITEMEClass = createEClass(MEMBER_MAPPING_ITEM);
		createEAttribute(membeR_MAPPING_ITEMEClass, MEMBER_MAPPING_ITEM__IS_SOURCE);
		createEReference(membeR_MAPPING_ITEMEClass, MEMBER_MAPPING_ITEM__MEMBER);
		createEReference(membeR_MAPPING_ITEMEClass, MEMBER_MAPPING_ITEM__VARIABLE);

		variablE_MAPPING_ITEMEClass = createEClass(VARIABLE_MAPPING_ITEM);
		createEAttribute(variablE_MAPPING_ITEMEClass, VARIABLE_MAPPING_ITEM__IS_SOURCE);
		createEReference(variablE_MAPPING_ITEMEClass, VARIABLE_MAPPING_ITEM__VARIABLE);

		variablE_MAPPINGEClass = createEClass(VARIABLE_MAPPING);
		createEReference(variablE_MAPPINGEClass, VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS);
		createEAttribute(variablE_MAPPINGEClass, VARIABLE_MAPPING__NAME);
		createEAttribute(variablE_MAPPINGEClass, VARIABLE_MAPPING__CODE);
		createEAttribute(variablE_MAPPINGEClass, VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID);
		createEAttribute(variablE_MAPPINGEClass, VARIABLE_MAPPING__VARIABLE_MAPPING_ID);

		mappinG_TO_CUBEEClass = createEClass(MAPPING_TO_CUBE);
		createEReference(mappinG_TO_CUBEEClass, MAPPING_TO_CUBE__MAPPING);

		variablE_SET_MAPPINGEClass = createEClass(VARIABLE_SET_MAPPING);
		createEReference(variablE_SET_MAPPINGEClass, VARIABLE_SET_MAPPING__SOURCE_MAPPING);
		createEReference(variablE_SET_MAPPINGEClass, VARIABLE_SET_MAPPING__TARGET_MAPPING);

		mappinG_DEFINITIONEClass = createEClass(MAPPING_DEFINITION);
		createEAttribute(mappinG_DEFINITIONEClass, MAPPING_DEFINITION__NAME);
		createEAttribute(mappinG_DEFINITIONEClass, MAPPING_DEFINITION__CODE);
		createEAttribute(mappinG_DEFINITIONEClass, MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID);
		createEAttribute(mappinG_DEFINITIONEClass, MAPPING_DEFINITION__MAPPING_ID);
		createEAttribute(mappinG_DEFINITIONEClass, MAPPING_DEFINITION__MAPPING_TYPE);
		createEReference(mappinG_DEFINITIONEClass, MAPPING_DEFINITION__MEMBER_MAPPING);
		createEReference(mappinG_DEFINITIONEClass, MAPPING_DEFINITION__VARIABLE_MAPPING);
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
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(cubE_MAPPINGEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, "CUBE_MAPPING", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCUBE_MAPPING_Cube_mapping_id(), ecorePackage.getEString(), "cube_mapping_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_MAPPING_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_MAPPING_Source_cube(), theData_definitionPackage.getCUBE(), null, "source_cube", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_MAPPING_Target_cube(), theData_definitionPackage.getCUBE(), null, "target_cube", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_MAPPING_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_MAPPING_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_MAPPING_Maintenance_agency_id(), ecorePackage.getEString(), "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_MAPPING_MappingsToCube(), this.getMAPPING_TO_CUBE(), null, "mappingsToCube", null, 0, -1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(membeR_MAPPINGEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING.class, "MEMBER_MAPPING", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getMEMBER_MAPPING_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_MAPPING_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_MAPPING_Maintenance_agency_id(), ecorePackage.getEString(), "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_MAPPING_Member_mapping_id(), ecorePackage.getEString(), "member_mapping_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_MAPPING_MemberMappingItems(), this.getMEMBER_MAPPING_ITEM(), null, "memberMappingItems", null, 0, -1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(membeR_MAPPING_ITEMEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM.class, "MEMBER_MAPPING_ITEM", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getMEMBER_MAPPING_ITEM_IsSource(), ecorePackage.getEString(), "isSource", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_MAPPING_ITEM_Member(), theCorePackage.getMEMBER(), null, "member", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_MAPPING_ITEM_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variablE_MAPPING_ITEMEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM.class, "VARIABLE_MAPPING_ITEM", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getVARIABLE_MAPPING_ITEM_IsSource(), ecorePackage.getEString(), "isSource", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_MAPPING_ITEM_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variablE_MAPPINGEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING.class, "VARIABLE_MAPPING", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVARIABLE_MAPPING_VaraibleMappingItems(), this.getVARIABLE_MAPPING_ITEM(), null, "varaibleMappingItems", null, 0, -1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_MAPPING_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_MAPPING_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_MAPPING_Maintenance_agency_id(), ecorePackage.getEString(), "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_MAPPING_Variable_mapping_id(), ecorePackage.getEString(), "variable_mapping_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(mappinG_TO_CUBEEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE.class, "MAPPING_TO_CUBE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMAPPING_TO_CUBE_Mapping(), this.getMAPPING_DEFINITION(), null, "mapping", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variablE_SET_MAPPINGEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING.class, "VARIABLE_SET_MAPPING", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVARIABLE_SET_MAPPING_SourceMapping(), this.getMAPPING_DEFINITION(), null, "sourceMapping", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_SET_MAPPING_TargetMapping(), this.getMAPPING_DEFINITION(), null, "targetMapping", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(mappinG_DEFINITIONEClass, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, "MAPPING_DEFINITION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getMAPPING_DEFINITION_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMAPPING_DEFINITION_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMAPPING_DEFINITION_Maintenance_agency_id(), ecorePackage.getEString(), "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMAPPING_DEFINITION_Mapping_id(), ecorePackage.getEString(), "mapping_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMAPPING_DEFINITION_Mapping_type(), ecorePackage.getEString(), "mapping_type", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMAPPING_DEFINITION_MemberMapping(), this.getMEMBER_MAPPING(), null, "memberMapping", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMAPPING_DEFINITION_VariableMapping(), this.getVARIABLE_MAPPING(), null, "variableMapping", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the data definition package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html"
		   });
	}

} //MappingPackageImpl
