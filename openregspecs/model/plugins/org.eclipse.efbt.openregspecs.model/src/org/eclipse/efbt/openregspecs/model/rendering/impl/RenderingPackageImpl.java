/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.impl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl.openregspecs_smcubes_extra_extensionPackageImpl;

import org.eclipse.efbt.openregspecs.model.core.CorePackage;

import org.eclipse.efbt.openregspecs.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.openregspecs.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.openregspecs.model.mapping.MappingPackage;

import org.eclipse.efbt.openregspecs.model.mapping.impl.MappingPackageImpl;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.efbt.openregspecs.model.rendering.RenderingFactory;
import org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage;

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
public class RenderingPackageImpl extends EPackageImpl implements RenderingPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass axisEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass axiS_ORDINATEEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass celL_POSITIONEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass ordinatE_ITEMEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tablE_CELLEClass = null;

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
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private RenderingPackageImpl() {
		super(eNS_URI, RenderingFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link RenderingPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static RenderingPackage init() {
		if (isInited) return (RenderingPackage)EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredRenderingPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		RenderingPackageImpl theRenderingPackage = registeredRenderingPackage instanceof RenderingPackageImpl ? (RenderingPackageImpl)registeredRenderingPackage : new RenderingPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Data_meta_modelPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		MappingPackageImpl theMappingPackage = (MappingPackageImpl)(registeredPackage instanceof MappingPackageImpl ? registeredPackage : MappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(openregspecs_smcubes_core_extensionPackage.eNS_URI);
		openregspecs_smcubes_core_extensionPackageImpl theopenregspecs_smcubes_core_extensionPackage = (openregspecs_smcubes_core_extensionPackageImpl)(registeredPackage instanceof openregspecs_smcubes_core_extensionPackageImpl ? registeredPackage : openregspecs_smcubes_core_extensionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(openregspecs_smcubes_extra_extensionPackage.eNS_URI);
		openregspecs_smcubes_extra_extensionPackageImpl theopenregspecs_smcubes_extra_extensionPackage = (openregspecs_smcubes_extra_extensionPackageImpl)(registeredPackage instanceof openregspecs_smcubes_extra_extensionPackageImpl ? registeredPackage : openregspecs_smcubes_extra_extensionPackage.eINSTANCE);

		// Create package meta-data objects
		theRenderingPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theopenregspecs_smcubes_core_extensionPackage.createPackageContents();
		theopenregspecs_smcubes_extra_extensionPackage.createPackageContents();

		// Initialize created meta-data
		theRenderingPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theopenregspecs_smcubes_core_extensionPackage.initializePackageContents();
		theopenregspecs_smcubes_extra_extensionPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theRenderingPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(RenderingPackage.eNS_URI, theRenderingPackage);
		return theRenderingPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAXIS() {
		return axisEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_Axis_id() {
		return (EAttribute)axisEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_Code() {
		return (EAttribute)axisEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_Description() {
		return (EAttribute)axisEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_Is_open_axis() {
		return (EAttribute)axisEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_Name() {
		return (EAttribute)axisEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_Order() {
		return (EAttribute)axisEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_Orientation() {
		return (EAttribute)axisEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAXIS_Table_id() {
		return (EReference)axisEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAXIS_ORDINATE() {
		return axiS_ORDINATEEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAXIS_ORDINATE_Axis_id() {
		return (EReference)axiS_ORDINATEEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Axis_ordinate_id() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Code() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Description() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Is_abstract_header() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Level() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Name() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Order() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAXIS_ORDINATE_Parent_axis_ordinate_id() {
		return (EReference)axiS_ORDINATEEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAXIS_ORDINATE_Path() {
		return (EAttribute)axiS_ORDINATEEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCELL_POSITION() {
		return celL_POSITIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCELL_POSITION_Axis_ordinate_id() {
		return (EReference)celL_POSITIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCELL_POSITION_Cell_id() {
		return (EReference)celL_POSITIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getORDINATE_ITEM() {
		return ordinatE_ITEMEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getORDINATE_ITEM_Axis_ordinate_id() {
		return (EReference)ordinatE_ITEMEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getORDINATE_ITEM_Is_starting_member_included() {
		return (EAttribute)ordinatE_ITEMEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getORDINATE_ITEM_Member_hierarchy_id() {
		return (EReference)ordinatE_ITEMEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getORDINATE_ITEM_Member_id() {
		return (EReference)ordinatE_ITEMEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getORDINATE_ITEM_Starting_member_id() {
		return (EReference)ordinatE_ITEMEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getORDINATE_ITEM_Variable_id() {
		return (EReference)ordinatE_ITEMEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTABLE() {
		return tableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_Code() {
		return (EAttribute)tableEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_Description() {
		return (EAttribute)tableEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTABLE_Maintenance_agency_id() {
		return (EReference)tableEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_Name() {
		return (EAttribute)tableEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_Table_id() {
		return (EAttribute)tableEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_Valid_from() {
		return (EAttribute)tableEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_Valid_to() {
		return (EAttribute)tableEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_Version() {
		return (EAttribute)tableEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTABLE_CELL() {
		return tablE_CELLEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_CELL_Cell_id() {
		return (EAttribute)tablE_CELLEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTABLE_CELL_Combination_id() {
		return (EReference)tablE_CELLEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTABLE_CELL_Is_shaded() {
		return (EAttribute)tablE_CELLEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTABLE_CELL_Table_id() {
		return (EReference)tablE_CELLEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RenderingFactory getRenderingFactory() {
		return (RenderingFactory)getEFactoryInstance();
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
		axisEClass = createEClass(AXIS);
		createEAttribute(axisEClass, AXIS__AXIS_ID);
		createEAttribute(axisEClass, AXIS__CODE);
		createEAttribute(axisEClass, AXIS__DESCRIPTION);
		createEAttribute(axisEClass, AXIS__IS_OPEN_AXIS);
		createEAttribute(axisEClass, AXIS__NAME);
		createEAttribute(axisEClass, AXIS__ORDER);
		createEAttribute(axisEClass, AXIS__ORIENTATION);
		createEReference(axisEClass, AXIS__TABLE_ID);

		axiS_ORDINATEEClass = createEClass(AXIS_ORDINATE);
		createEReference(axiS_ORDINATEEClass, AXIS_ORDINATE__AXIS_ID);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__AXIS_ORDINATE_ID);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__CODE);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__DESCRIPTION);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__IS_ABSTRACT_HEADER);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__LEVEL);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__NAME);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__ORDER);
		createEReference(axiS_ORDINATEEClass, AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID);
		createEAttribute(axiS_ORDINATEEClass, AXIS_ORDINATE__PATH);

		celL_POSITIONEClass = createEClass(CELL_POSITION);
		createEReference(celL_POSITIONEClass, CELL_POSITION__AXIS_ORDINATE_ID);
		createEReference(celL_POSITIONEClass, CELL_POSITION__CELL_ID);

		ordinatE_ITEMEClass = createEClass(ORDINATE_ITEM);
		createEReference(ordinatE_ITEMEClass, ORDINATE_ITEM__AXIS_ORDINATE_ID);
		createEAttribute(ordinatE_ITEMEClass, ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED);
		createEReference(ordinatE_ITEMEClass, ORDINATE_ITEM__MEMBER_HIERARCHY_ID);
		createEReference(ordinatE_ITEMEClass, ORDINATE_ITEM__MEMBER_ID);
		createEReference(ordinatE_ITEMEClass, ORDINATE_ITEM__STARTING_MEMBER_ID);
		createEReference(ordinatE_ITEMEClass, ORDINATE_ITEM__VARIABLE_ID);

		tableEClass = createEClass(TABLE);
		createEAttribute(tableEClass, TABLE__CODE);
		createEAttribute(tableEClass, TABLE__DESCRIPTION);
		createEReference(tableEClass, TABLE__MAINTENANCE_AGENCY_ID);
		createEAttribute(tableEClass, TABLE__NAME);
		createEAttribute(tableEClass, TABLE__TABLE_ID);
		createEAttribute(tableEClass, TABLE__VALID_FROM);
		createEAttribute(tableEClass, TABLE__VALID_TO);
		createEAttribute(tableEClass, TABLE__VERSION);

		tablE_CELLEClass = createEClass(TABLE_CELL);
		createEAttribute(tablE_CELLEClass, TABLE_CELL__CELL_ID);
		createEReference(tablE_CELLEClass, TABLE_CELL__COMBINATION_ID);
		createEAttribute(tablE_CELLEClass, TABLE_CELL__IS_SHADED);
		createEReference(tablE_CELLEClass, TABLE_CELL__TABLE_ID);
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
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(axisEClass, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, "AXIS", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getAXIS_Axis_id(), ecorePackage.getEString(), "axis_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_Is_open_axis(), ecorePackage.getEBooleanObject(), "is_open_axis", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_Order(), ecorePackage.getEInt(), "order", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_Orientation(), ecorePackage.getEString(), "orientation", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAXIS_Table_id(), this.getTABLE(), null, "table_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(axiS_ORDINATEEClass, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, "AXIS_ORDINATE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAXIS_ORDINATE_Axis_id(), this.getAXIS(), null, "axis_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Axis_ordinate_id(), ecorePackage.getEString(), "axis_ordinate_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Is_abstract_header(), ecorePackage.getEBooleanObject(), "is_abstract_header", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Level(), ecorePackage.getEInt(), "level", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Order(), ecorePackage.getEInt(), "order", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAXIS_ORDINATE_Parent_axis_ordinate_id(), this.getAXIS_ORDINATE(), null, "parent_axis_ordinate_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAXIS_ORDINATE_Path(), ecorePackage.getEString(), "path", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(celL_POSITIONEClass, org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION.class, "CELL_POSITION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCELL_POSITION_Axis_ordinate_id(), this.getAXIS_ORDINATE(), null, "axis_ordinate_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCELL_POSITION_Cell_id(), this.getTABLE_CELL(), null, "cell_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(ordinatE_ITEMEClass, org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM.class, "ORDINATE_ITEM", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getORDINATE_ITEM_Axis_ordinate_id(), this.getAXIS_ORDINATE(), null, "axis_ordinate_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getORDINATE_ITEM_Is_starting_member_included(), ecorePackage.getEBooleanObject(), "is_starting_member_included", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getORDINATE_ITEM_Member_hierarchy_id(), theCorePackage.getMEMBER_HIERARCHY(), null, "member_hierarchy_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getORDINATE_ITEM_Member_id(), theCorePackage.getMEMBER(), null, "member_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getORDINATE_ITEM_Starting_member_id(), theCorePackage.getMEMBER(), null, "starting_member_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getORDINATE_ITEM_Variable_id(), theCorePackage.getVARIABLE(), null, "variable_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(tableEClass, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, "TABLE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTABLE_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTABLE_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTABLE_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTABLE_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTABLE_Table_id(), ecorePackage.getEString(), "table_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTABLE_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTABLE_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTABLE_Version(), ecorePackage.getEString(), "version", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(tablE_CELLEClass, org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL.class, "TABLE_CELL", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTABLE_CELL_Cell_id(), ecorePackage.getEString(), "cell_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTABLE_CELL_Combination_id(), theData_definitionPackage.getCOMBINATION(), null, "combination_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTABLE_CELL_Is_shaded(), ecorePackage.getEBooleanObject(), "is_shaded", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTABLE_CELL_Table_id(), this.getTABLE(), null, "table_id", null, 0, 1, org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the rendering package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html"
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
		  (axisEClass,
		   source,
		   new String[] {
			   "name", "AXIS",
			   "kind", "empty"
		   });
		addAnnotation
		  (getAXIS_Axis_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "axis_id"
		   });
		addAnnotation
		  (getAXIS_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getAXIS_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getAXIS_Is_open_axis(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_open_axis"
		   });
		addAnnotation
		  (getAXIS_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getAXIS_Order(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "order"
		   });
		addAnnotation
		  (getAXIS_Orientation(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "orientation"
		   });
		addAnnotation
		  (getAXIS_Table_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "table_id"
		   });
		addAnnotation
		  (axiS_ORDINATEEClass,
		   source,
		   new String[] {
			   "name", "AXIS_ORDINATE",
			   "kind", "empty"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Axis_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "axis_id"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Axis_ordinate_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "axis_ordinate_id"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Is_abstract_header(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_abstract_header"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Level(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "level"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Order(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "order"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Parent_axis_ordinate_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "parent_axis_ordinate_id"
		   });
		addAnnotation
		  (getAXIS_ORDINATE_Path(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "path"
		   });
		addAnnotation
		  (celL_POSITIONEClass,
		   source,
		   new String[] {
			   "name", "CELL_POSITION",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCELL_POSITION_Axis_ordinate_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "axis_ordinate_id"
		   });
		addAnnotation
		  (getCELL_POSITION_Cell_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cell_id"
		   });
		addAnnotation
		  (ordinatE_ITEMEClass,
		   source,
		   new String[] {
			   "name", "ORDINATE_ITEM",
			   "kind", "empty"
		   });
		addAnnotation
		  (getORDINATE_ITEM_Axis_ordinate_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "axis_ordinate_id"
		   });
		addAnnotation
		  (getORDINATE_ITEM_Is_starting_member_included(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_starting_member_included"
		   });
		addAnnotation
		  (getORDINATE_ITEM_Member_hierarchy_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_hierarchy_id"
		   });
		addAnnotation
		  (getORDINATE_ITEM_Member_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_id"
		   });
		addAnnotation
		  (getORDINATE_ITEM_Starting_member_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "starting_member_id"
		   });
		addAnnotation
		  (getORDINATE_ITEM_Variable_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_id"
		   });
		addAnnotation
		  (tableEClass,
		   source,
		   new String[] {
			   "name", "TABLE",
			   "kind", "empty"
		   });
		addAnnotation
		  (getTABLE_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getTABLE_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getTABLE_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getTABLE_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getTABLE_Table_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "table_id"
		   });
		addAnnotation
		  (getTABLE_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getTABLE_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (getTABLE_Version(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "version"
		   });
		addAnnotation
		  (tablE_CELLEClass,
		   source,
		   new String[] {
			   "name", "TABLE_CELL",
			   "kind", "empty"
		   });
		addAnnotation
		  (getTABLE_CELL_Cell_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cell_id"
		   });
		addAnnotation
		  (getTABLE_CELL_Combination_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "combination_id"
		   });
		addAnnotation
		  (getTABLE_CELL_Is_shaded(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_shaded"
		   });
		addAnnotation
		  (getTABLE_CELL_Table_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "table_id"
		   });
	}

} //RenderingPackageImpl
