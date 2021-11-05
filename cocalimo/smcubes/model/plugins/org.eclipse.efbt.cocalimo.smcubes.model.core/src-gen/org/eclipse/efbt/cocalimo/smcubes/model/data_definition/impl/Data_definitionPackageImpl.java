/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl.Efbt_mappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.ReportsPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.Smcubes_modelPackageImpl;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Data_definitionPackageImpl extends EPackageImpl implements Data_definitionPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass combinationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass combinatioN_ITEMEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_GROUPEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_GROUP_ENUMERATIONEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_HIERARCHYEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_HIERARCHY_NODEEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_RELATIONSHIPEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_STRUCTUREEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_STRUCTURE_ITEMEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubE_TO_COMBINATIONEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass frameworkEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass frameworK_SUBDOMAINEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass frameworK_VARIABLE_SETEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EEnum tyP_DMNSNEEnum = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EEnum tyP_RLEEnum = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EDataType tyP_DMNSNObjectEDataType = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EDataType tyP_RLObjectEDataType = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Data_definitionPackageImpl() {
		super(eNS_URI, Data_definitionFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Data_definitionPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Data_definitionPackage init() {
		if (isInited) return (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredData_definitionPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = registeredData_definitionPackage instanceof Data_definitionPackageImpl ? (Data_definitionPackageImpl)registeredData_definitionPackage : new Data_definitionPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CorePackageImpl theCorePackage = (CorePackageImpl)(registeredPackage instanceof CorePackageImpl ? registeredPackage : CorePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_data_definitionPackage.eNS_URI);
		Efbt_data_definitionPackageImpl theEfbt_data_definitionPackage = (Efbt_data_definitionPackageImpl)(registeredPackage instanceof Efbt_data_definitionPackageImpl ? registeredPackage : Efbt_data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Efbt_mappingPackage.eNS_URI);
		Efbt_mappingPackageImpl theEfbt_mappingPackage = (Efbt_mappingPackageImpl)(registeredPackage instanceof Efbt_mappingPackageImpl ? registeredPackage : Efbt_mappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		MappingPackageImpl theMappingPackage = (MappingPackageImpl)(registeredPackage instanceof MappingPackageImpl ? registeredPackage : MappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		RenderingPackageImpl theRenderingPackage = (RenderingPackageImpl)(registeredPackage instanceof RenderingPackageImpl ? registeredPackage : RenderingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);
		ReportsPackageImpl theReportsPackage = (ReportsPackageImpl)(registeredPackage instanceof ReportsPackageImpl ? registeredPackage : ReportsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Smcubes_modelPackage.eNS_URI);
		Smcubes_modelPackageImpl theSmcubes_modelPackage = (Smcubes_modelPackageImpl)(registeredPackage instanceof Smcubes_modelPackageImpl ? registeredPackage : Smcubes_modelPackage.eINSTANCE);

		// Create package meta-data objects
		theData_definitionPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theEfbt_data_definitionPackage.createPackageContents();
		theEfbt_mappingPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theReportsPackage.createPackageContents();
		theSmcubes_modelPackage.createPackageContents();

		// Initialize created meta-data
		theData_definitionPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theEfbt_data_definitionPackage.initializePackageContents();
		theEfbt_mappingPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theReportsPackage.initializePackageContents();
		theSmcubes_modelPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theData_definitionPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Data_definitionPackage.eNS_URI, theData_definitionPackage);
		return theData_definitionPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCOMBINATION() {
		return combinationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCOMBINATION_Code() {
		return (EAttribute)combinationEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCOMBINATION_Combination_id() {
		return (EAttribute)combinationEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCOMBINATION_Maintenance_agency_id() {
		return (EReference)combinationEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCOMBINATION_Name() {
		return (EAttribute)combinationEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCOMBINATION_Valid_from() {
		return (EAttribute)combinationEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCOMBINATION_Valid_to() {
		return (EAttribute)combinationEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCOMBINATION_Version() {
		return (EAttribute)combinationEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCOMBINATION_Combination_items() {
		return (EReference)combinationEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCOMBINATION_ITEM() {
		return combinatioN_ITEMEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCOMBINATION_ITEM_Member_id() {
		return (EReference)combinatioN_ITEMEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCOMBINATION_ITEM_Subdomain_id() {
		return (EReference)combinatioN_ITEMEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCOMBINATION_ITEM_Variable_id() {
		return (EReference)combinatioN_ITEMEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCOMBINATION_ITEM_Variable_set_id() {
		return (EReference)combinatioN_ITEMEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE() {
		return cubeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Code() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Cube_id() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_Cube_structure_id() {
		return (EReference)cubeEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Cube_type() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Description() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_Framework_id() {
		return (EReference)cubeEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Is_allowed() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_Maintenance_agency_id() {
		return (EReference)cubeEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Name() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Published() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Valid_from() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(10);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Valid_to() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(11);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_Version() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(12);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_DisplayName() {
		return (EAttribute)cubeEClass.getEStructuralFeatures().get(13);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_GROUP() {
		return cubE_GROUPEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_GROUP_Code() {
		return (EAttribute)cubE_GROUPEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_GROUP_Cube_group_id() {
		return (EAttribute)cubE_GROUPEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_GROUP_Description() {
		return (EAttribute)cubE_GROUPEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_GROUP_Maintenance_agency_id() {
		return (EReference)cubE_GROUPEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_GROUP_Name() {
		return (EAttribute)cubE_GROUPEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_GROUP_ENUMERATION() {
		return cubE_GROUP_ENUMERATIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_GROUP_ENUMERATION_Cube_group_id() {
		return (EReference)cubE_GROUP_ENUMERATIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_GROUP_ENUMERATION_Cube_id() {
		return (EReference)cubE_GROUP_ENUMERATIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_GROUP_ENUMERATION_Order() {
		return (EAttribute)cubE_GROUP_ENUMERATIONEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_GROUP_ENUMERATION_Valid_from() {
		return (EAttribute)cubE_GROUP_ENUMERATIONEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_GROUP_ENUMERATION_Valid_to() {
		return (EAttribute)cubE_GROUP_ENUMERATIONEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_HIERARCHY() {
		return cubE_HIERARCHYEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_Code() {
		return (EAttribute)cubE_HIERARCHYEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_Cube_hierarchy_id() {
		return (EAttribute)cubE_HIERARCHYEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_Cube_hierarchy_type() {
		return (EAttribute)cubE_HIERARCHYEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_HIERARCHY_Framework_id() {
		return (EReference)cubE_HIERARCHYEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_HIERARCHY_Maintenance_agency_id() {
		return (EReference)cubE_HIERARCHYEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_Name() {
		return (EAttribute)cubE_HIERARCHYEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_HIERARCHY_NODE() {
		return cubE_HIERARCHY_NODEEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_HIERARCHY_NODE_Cube_group_id() {
		return (EReference)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_HIERARCHY_NODE_Cube_hierarchy_id() {
		return (EReference)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_NODE_Level() {
		return (EAttribute)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_NODE_NODE_CODE() {
		return (EAttribute)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_NODE_Node_name() {
		return (EAttribute)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_NODE_Order() {
		return (EAttribute)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_HIERARCHY_NODE_Parent_node_code() {
		return (EReference)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_NODE_Valid_from() {
		return (EAttribute)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_HIERARCHY_NODE_Valid_to() {
		return (EAttribute)cubE_HIERARCHY_NODEEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_RELATIONSHIP() {
		return cubE_RELATIONSHIPEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Code() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Cube_relationship_id() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Description() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Establishes_integrity() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_RELATIONSHIP_Foreign_cube_id() {
		return (EReference)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_RELATIONSHIP_Foreign_cube_variable_code() {
		return (EReference)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_RELATIONSHIP_Maintenance_agency_id() {
		return (EReference)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Name() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_RELATIONSHIP_Primary_cube_id() {
		return (EReference)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_RELATIONSHIP_Primary_cube_variable_id() {
		return (EReference)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Valid_from() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(10);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Valid_to() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(11);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_RELATIONSHIP_Version() {
		return (EAttribute)cubE_RELATIONSHIPEClass.getEStructuralFeatures().get(12);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_STRUCTURE() {
		return cubE_STRUCTUREEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_Code() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_Cube_structure_id() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_Description() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_STRUCTURE_Maintenance_agency_id() {
		return (EReference)cubE_STRUCTUREEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_Name() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_Valid_from() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_Valid_to() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_Version() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_DisplayName() {
		return (EAttribute)cubE_STRUCTUREEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_STRUCTURE_ITEM() {
		return cubE_STRUCTURE_ITEMEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_STRUCTURE_ITEM_Attribute_associated_variable() {
		return (EReference)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_STRUCTURE_ITEM_Cube_structure_id() {
		return (EReference)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_Description() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_Dimension_type() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_Is_flow() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_Is_mandatory() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_STRUCTURE_ITEM_Member_id() {
		return (EReference)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_Order() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_Role() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_STRUCTURE_ITEM_Subdomain_id() {
		return (EReference)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_STRUCTURE_ITEM_Variable_id() {
		return (EReference)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(10);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_STRUCTURE_ITEM_Variable_set_id() {
		return (EReference)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(11);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_IsIdentifier() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(12);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCUBE_STRUCTURE_ITEM_Cube_variable_code() {
		return (EAttribute)cubE_STRUCTURE_ITEMEClass.getEStructuralFeatures().get(13);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCUBE_TO_COMBINATION() {
		return cubE_TO_COMBINATIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_TO_COMBINATION_Combination_id() {
		return (EReference)cubE_TO_COMBINATIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCUBE_TO_COMBINATION_Cube_id() {
		return (EReference)cubE_TO_COMBINATIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFRAMEWORK() {
		return frameworkEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFRAMEWORK_Code() {
		return (EAttribute)frameworkEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFRAMEWORK_Description() {
		return (EAttribute)frameworkEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFRAMEWORK_Framework_id() {
		return (EAttribute)frameworkEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFRAMEWORK_Maintenance_agency_id() {
		return (EReference)frameworkEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFRAMEWORK_Name() {
		return (EAttribute)frameworkEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFRAMEWORK_SUBDOMAIN() {
		return frameworK_SUBDOMAINEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFRAMEWORK_SUBDOMAIN_Framework_id() {
		return (EReference)frameworK_SUBDOMAINEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFRAMEWORK_SUBDOMAIN_Subdomain_id() {
		return (EReference)frameworK_SUBDOMAINEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFRAMEWORK_VARIABLE_SET() {
		return frameworK_VARIABLE_SETEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFRAMEWORK_VARIABLE_SET_Framework_id() {
		return (EReference)frameworK_VARIABLE_SETEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFRAMEWORK_VARIABLE_SET_Variable_set_id() {
		return (EReference)frameworK_VARIABLE_SETEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EEnum getTYP_DMNSN() {
		return tyP_DMNSNEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EEnum getTYP_RL() {
		return tyP_RLEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EDataType getTYP_DMNSNObject() {
		return tyP_DMNSNObjectEDataType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EDataType getTYP_RLObject() {
		return tyP_RLObjectEDataType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Data_definitionFactory getData_definitionFactory() {
		return (Data_definitionFactory)getEFactoryInstance();
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
		combinationEClass = createEClass(COMBINATION);
		createEAttribute(combinationEClass, COMBINATION__CODE);
		createEAttribute(combinationEClass, COMBINATION__COMBINATION_ID);
		createEReference(combinationEClass, COMBINATION__MAINTENANCE_AGENCY_ID);
		createEAttribute(combinationEClass, COMBINATION__NAME);
		createEAttribute(combinationEClass, COMBINATION__VALID_FROM);
		createEAttribute(combinationEClass, COMBINATION__VALID_TO);
		createEAttribute(combinationEClass, COMBINATION__VERSION);
		createEReference(combinationEClass, COMBINATION__COMBINATION_ITEMS);

		combinatioN_ITEMEClass = createEClass(COMBINATION_ITEM);
		createEReference(combinatioN_ITEMEClass, COMBINATION_ITEM__MEMBER_ID);
		createEReference(combinatioN_ITEMEClass, COMBINATION_ITEM__SUBDOMAIN_ID);
		createEReference(combinatioN_ITEMEClass, COMBINATION_ITEM__VARIABLE_ID);
		createEReference(combinatioN_ITEMEClass, COMBINATION_ITEM__VARIABLE_SET_ID);

		cubeEClass = createEClass(CUBE);
		createEAttribute(cubeEClass, CUBE__CODE);
		createEAttribute(cubeEClass, CUBE__CUBE_ID);
		createEReference(cubeEClass, CUBE__CUBE_STRUCTURE_ID);
		createEAttribute(cubeEClass, CUBE__CUBE_TYPE);
		createEAttribute(cubeEClass, CUBE__DESCRIPTION);
		createEReference(cubeEClass, CUBE__FRAMEWORK_ID);
		createEAttribute(cubeEClass, CUBE__IS_ALLOWED);
		createEReference(cubeEClass, CUBE__MAINTENANCE_AGENCY_ID);
		createEAttribute(cubeEClass, CUBE__NAME);
		createEAttribute(cubeEClass, CUBE__PUBLISHED);
		createEAttribute(cubeEClass, CUBE__VALID_FROM);
		createEAttribute(cubeEClass, CUBE__VALID_TO);
		createEAttribute(cubeEClass, CUBE__VERSION);
		createEAttribute(cubeEClass, CUBE__DISPLAY_NAME);

		cubE_GROUPEClass = createEClass(CUBE_GROUP);
		createEAttribute(cubE_GROUPEClass, CUBE_GROUP__CODE);
		createEAttribute(cubE_GROUPEClass, CUBE_GROUP__CUBE_GROUP_ID);
		createEAttribute(cubE_GROUPEClass, CUBE_GROUP__DESCRIPTION);
		createEReference(cubE_GROUPEClass, CUBE_GROUP__MAINTENANCE_AGENCY_ID);
		createEAttribute(cubE_GROUPEClass, CUBE_GROUP__NAME);

		cubE_GROUP_ENUMERATIONEClass = createEClass(CUBE_GROUP_ENUMERATION);
		createEReference(cubE_GROUP_ENUMERATIONEClass, CUBE_GROUP_ENUMERATION__CUBE_GROUP_ID);
		createEReference(cubE_GROUP_ENUMERATIONEClass, CUBE_GROUP_ENUMERATION__CUBE_ID);
		createEAttribute(cubE_GROUP_ENUMERATIONEClass, CUBE_GROUP_ENUMERATION__ORDER);
		createEAttribute(cubE_GROUP_ENUMERATIONEClass, CUBE_GROUP_ENUMERATION__VALID_FROM);
		createEAttribute(cubE_GROUP_ENUMERATIONEClass, CUBE_GROUP_ENUMERATION__VALID_TO);

		cubE_HIERARCHYEClass = createEClass(CUBE_HIERARCHY);
		createEAttribute(cubE_HIERARCHYEClass, CUBE_HIERARCHY__CODE);
		createEAttribute(cubE_HIERARCHYEClass, CUBE_HIERARCHY__CUBE_HIERARCHY_ID);
		createEAttribute(cubE_HIERARCHYEClass, CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE);
		createEReference(cubE_HIERARCHYEClass, CUBE_HIERARCHY__FRAMEWORK_ID);
		createEReference(cubE_HIERARCHYEClass, CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID);
		createEAttribute(cubE_HIERARCHYEClass, CUBE_HIERARCHY__NAME);

		cubE_HIERARCHY_NODEEClass = createEClass(CUBE_HIERARCHY_NODE);
		createEReference(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__CUBE_GROUP_ID);
		createEReference(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID);
		createEAttribute(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__LEVEL);
		createEAttribute(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__NODE_CODE);
		createEAttribute(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__NODE_NAME);
		createEAttribute(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__ORDER);
		createEReference(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__PARENT_NODE_CODE);
		createEAttribute(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__VALID_FROM);
		createEAttribute(cubE_HIERARCHY_NODEEClass, CUBE_HIERARCHY_NODE__VALID_TO);

		cubE_RELATIONSHIPEClass = createEClass(CUBE_RELATIONSHIP);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__CODE);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__DESCRIPTION);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY);
		createEReference(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__FOREIGN_CUBE_ID);
		createEReference(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE);
		createEReference(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__NAME);
		createEReference(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__PRIMARY_CUBE_ID);
		createEReference(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__VALID_FROM);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__VALID_TO);
		createEAttribute(cubE_RELATIONSHIPEClass, CUBE_RELATIONSHIP__VERSION);

		cubE_STRUCTUREEClass = createEClass(CUBE_STRUCTURE);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__CODE);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__CUBE_STRUCTURE_ID);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__DESCRIPTION);
		createEReference(cubE_STRUCTUREEClass, CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__NAME);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__VALID_FROM);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__VALID_TO);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__VERSION);
		createEAttribute(cubE_STRUCTUREEClass, CUBE_STRUCTURE__DISPLAY_NAME);

		cubE_STRUCTURE_ITEMEClass = createEClass(CUBE_STRUCTURE_ITEM);
		createEReference(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE);
		createEReference(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__DESCRIPTION);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__DIMENSION_TYPE);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__IS_FLOW);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__IS_MANDATORY);
		createEReference(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__MEMBER_ID);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__ORDER);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__ROLE);
		createEReference(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID);
		createEReference(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__VARIABLE_ID);
		createEReference(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__IS_IDENTIFIER);
		createEAttribute(cubE_STRUCTURE_ITEMEClass, CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE);

		cubE_TO_COMBINATIONEClass = createEClass(CUBE_TO_COMBINATION);
		createEReference(cubE_TO_COMBINATIONEClass, CUBE_TO_COMBINATION__COMBINATION_ID);
		createEReference(cubE_TO_COMBINATIONEClass, CUBE_TO_COMBINATION__CUBE_ID);

		frameworkEClass = createEClass(FRAMEWORK);
		createEAttribute(frameworkEClass, FRAMEWORK__CODE);
		createEAttribute(frameworkEClass, FRAMEWORK__DESCRIPTION);
		createEAttribute(frameworkEClass, FRAMEWORK__FRAMEWORK_ID);
		createEReference(frameworkEClass, FRAMEWORK__MAINTENANCE_AGENCY_ID);
		createEAttribute(frameworkEClass, FRAMEWORK__NAME);

		frameworK_SUBDOMAINEClass = createEClass(FRAMEWORK_SUBDOMAIN);
		createEReference(frameworK_SUBDOMAINEClass, FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID);
		createEReference(frameworK_SUBDOMAINEClass, FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID);

		frameworK_VARIABLE_SETEClass = createEClass(FRAMEWORK_VARIABLE_SET);
		createEReference(frameworK_VARIABLE_SETEClass, FRAMEWORK_VARIABLE_SET__FRAMEWORK_ID);
		createEReference(frameworK_VARIABLE_SETEClass, FRAMEWORK_VARIABLE_SET__VARIABLE_SET_ID);

		// Create enums
		tyP_DMNSNEEnum = createEEnum(TYP_DMNSN);
		tyP_RLEEnum = createEEnum(TYP_RL);

		// Create data types
		tyP_DMNSNObjectEDataType = createEDataType(TYP_DMNSN_OBJECT);
		tyP_RLObjectEDataType = createEDataType(TYP_RL_OBJECT);
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

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(combinationEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, "COMBINATION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCOMBINATION_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCOMBINATION_Combination_id(), ecorePackage.getEString(), "combination_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCOMBINATION_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCOMBINATION_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCOMBINATION_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCOMBINATION_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCOMBINATION_Version(), ecorePackage.getEString(), "version", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCOMBINATION_Combination_items(), this.getCOMBINATION_ITEM(), null, "combination_items", null, 0, -1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(combinatioN_ITEMEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM.class, "COMBINATION_ITEM", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCOMBINATION_ITEM_Member_id(), theCorePackage.getMEMBER(), null, "member_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCOMBINATION_ITEM_Subdomain_id(), theCorePackage.getSUBDOMAIN(), null, "subdomain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCOMBINATION_ITEM_Variable_id(), theCorePackage.getVARIABLE(), null, "variable_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCOMBINATION_ITEM_Variable_set_id(), theCorePackage.getVARIABLE_SET(), null, "variable_set_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubeEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, "CUBE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCUBE_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Cube_id(), ecorePackage.getEString(), "cube_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_Cube_structure_id(), this.getCUBE_STRUCTURE(), null, "cube_structure_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Cube_type(), ecorePackage.getEString(), "cube_type", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_Framework_id(), this.getFRAMEWORK(), null, "framework_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Is_allowed(), ecorePackage.getEBoolean(), "is_allowed", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Published(), ecorePackage.getEBoolean(), "published", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_Version(), ecorePackage.getEString(), "version", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_GROUPEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP.class, "CUBE_GROUP", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCUBE_GROUP_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_GROUP_Cube_group_id(), ecorePackage.getEString(), "cube_group_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_GROUP_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_GROUP_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_GROUP_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_GROUP_ENUMERATIONEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION.class, "CUBE_GROUP_ENUMERATION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCUBE_GROUP_ENUMERATION_Cube_group_id(), this.getCUBE_GROUP(), null, "cube_group_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_GROUP_ENUMERATION_Cube_id(), this.getCUBE(), null, "cube_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_GROUP_ENUMERATION_Order(), ecorePackage.getEInt(), "order", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_GROUP_ENUMERATION_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_GROUP_ENUMERATION_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_HIERARCHYEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY.class, "CUBE_HIERARCHY", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCUBE_HIERARCHY_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_Cube_hierarchy_id(), ecorePackage.getEString(), "cube_hierarchy_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_Cube_hierarchy_type(), ecorePackage.getEString(), "cube_hierarchy_type", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_HIERARCHY_Framework_id(), this.getFRAMEWORK(), null, "framework_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_HIERARCHY_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_HIERARCHY_NODEEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, "CUBE_HIERARCHY_NODE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCUBE_HIERARCHY_NODE_Cube_group_id(), this.getCUBE_GROUP(), null, "cube_group_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_HIERARCHY_NODE_Cube_hierarchy_id(), this.getCUBE_HIERARCHY(), null, "cube_hierarchy_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_NODE_Level(), ecorePackage.getEInt(), "level", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_NODE_NODE_CODE(), ecorePackage.getEString(), "nODE_CODE", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_NODE_Node_name(), ecorePackage.getEString(), "node_name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_NODE_Order(), ecorePackage.getEInt(), "order", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_HIERARCHY_NODE_Parent_node_code(), this.getCUBE_HIERARCHY_NODE(), null, "parent_node_code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_NODE_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_HIERARCHY_NODE_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_RELATIONSHIPEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, "CUBE_RELATIONSHIP", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCUBE_RELATIONSHIP_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_RELATIONSHIP_Cube_relationship_id(), ecorePackage.getEString(), "cube_relationship_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_RELATIONSHIP_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_RELATIONSHIP_Establishes_integrity(), ecorePackage.getEBoolean(), "establishes_integrity", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_RELATIONSHIP_Foreign_cube_id(), this.getCUBE(), null, "foreign_cube_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_RELATIONSHIP_Foreign_cube_variable_code(), theCorePackage.getVARIABLE(), null, "foreign_cube_variable_code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_RELATIONSHIP_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_RELATIONSHIP_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_RELATIONSHIP_Primary_cube_id(), this.getCUBE(), null, "primary_cube_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_RELATIONSHIP_Primary_cube_variable_id(), theCorePackage.getVARIABLE(), null, "primary_cube_variable_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_RELATIONSHIP_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_RELATIONSHIP_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_RELATIONSHIP_Version(), ecorePackage.getEString(), "version", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_STRUCTUREEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, "CUBE_STRUCTURE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCUBE_STRUCTURE_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_Cube_structure_id(), ecorePackage.getEString(), "cube_structure_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_STRUCTURE_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_Version(), ecorePackage.getEString(), "version", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_STRUCTURE_ITEMEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, "CUBE_STRUCTURE_ITEM", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCUBE_STRUCTURE_ITEM_Attribute_associated_variable(), theCorePackage.getVARIABLE(), null, "attribute_associated_variable", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_STRUCTURE_ITEM_Cube_structure_id(), this.getCUBE_STRUCTURE(), null, "cube_structure_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_Dimension_type(), this.getTYP_DMNSN(), "dimension_type", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_Is_flow(), ecorePackage.getEBoolean(), "is_flow", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_Is_mandatory(), ecorePackage.getEBoolean(), "is_mandatory", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_STRUCTURE_ITEM_Member_id(), theCorePackage.getMEMBER(), null, "member_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_Order(), ecorePackage.getEInt(), "order", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_Role(), this.getTYP_RL(), "role", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_STRUCTURE_ITEM_Subdomain_id(), theCorePackage.getSUBDOMAIN(), null, "subdomain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_STRUCTURE_ITEM_Variable_id(), theCorePackage.getVARIABLE(), null, "variable_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_STRUCTURE_ITEM_Variable_set_id(), theCorePackage.getVARIABLE_SET(), null, "variable_set_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_IsIdentifier(), ecorePackage.getEBoolean(), "isIdentifier", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCUBE_STRUCTURE_ITEM_Cube_variable_code(), ecorePackage.getEString(), "cube_variable_code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubE_TO_COMBINATIONEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION.class, "CUBE_TO_COMBINATION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCUBE_TO_COMBINATION_Combination_id(), this.getCOMBINATION(), null, "combination_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCUBE_TO_COMBINATION_Cube_id(), this.getCUBE(), null, "cube_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(frameworkEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK.class, "FRAMEWORK", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getFRAMEWORK_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFRAMEWORK_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFRAMEWORK_Framework_id(), ecorePackage.getEString(), "framework_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFRAMEWORK_Maintenance_agency_id(), theCorePackage.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFRAMEWORK_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(frameworK_SUBDOMAINEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN.class, "FRAMEWORK_SUBDOMAIN", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFRAMEWORK_SUBDOMAIN_Framework_id(), this.getFRAMEWORK(), null, "framework_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFRAMEWORK_SUBDOMAIN_Subdomain_id(), theCorePackage.getSUBDOMAIN(), null, "subdomain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(frameworK_VARIABLE_SETEClass, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET.class, "FRAMEWORK_VARIABLE_SET", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFRAMEWORK_VARIABLE_SET_Framework_id(), this.getFRAMEWORK(), null, "framework_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFRAMEWORK_VARIABLE_SET_Variable_set_id(), theCorePackage.getVARIABLE_SET(), null, "variable_set_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_VARIABLE_SET.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Initialize enums and add enum literals
		initEEnum(tyP_DMNSNEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN.class, "TYP_DMNSN");
		addEEnumLiteral(tyP_DMNSNEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN.B);
		addEEnumLiteral(tyP_DMNSNEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN.M);
		addEEnumLiteral(tyP_DMNSNEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN.T);
		addEEnumLiteral(tyP_DMNSNEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN.U);

		initEEnum(tyP_RLEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL.class, "TYP_RL");
		addEEnumLiteral(tyP_RLEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL.O);
		addEEnumLiteral(tyP_RLEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL.A);
		addEEnumLiteral(tyP_RLEEnum, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL.D);

		// Initialize data types
		initEDataType(tyP_DMNSNObjectEDataType, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN.class, "TYP_DMNSNObject", IS_SERIALIZABLE, IS_GENERATED_INSTANCE_CLASS);
		initEDataType(tyP_RLObjectEDataType, org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL.class, "TYP_RLObject", IS_SERIALIZABLE, IS_GENERATED_INSTANCE_CLASS);

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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the data definition package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html"
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
		  (combinationEClass,
		   source,
		   new String[] {
			   "name", "COMBINATION",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCOMBINATION_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getCOMBINATION_Combination_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "combination_id"
		   });
		addAnnotation
		  (getCOMBINATION_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getCOMBINATION_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getCOMBINATION_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getCOMBINATION_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (getCOMBINATION_Version(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "version"
		   });
		addAnnotation
		  (combinatioN_ITEMEClass,
		   source,
		   new String[] {
			   "name", "COMBINATION_ITEM",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCOMBINATION_ITEM_Member_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_id"
		   });
		addAnnotation
		  (getCOMBINATION_ITEM_Subdomain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "subdomain_id"
		   });
		addAnnotation
		  (getCOMBINATION_ITEM_Variable_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_id"
		   });
		addAnnotation
		  (getCOMBINATION_ITEM_Variable_set_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_set_id"
		   });
		addAnnotation
		  (cubeEClass,
		   source,
		   new String[] {
			   "name", "CUBE",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getCUBE_Cube_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_id"
		   });
		addAnnotation
		  (getCUBE_Cube_structure_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_structure_id"
		   });
		addAnnotation
		  (getCUBE_Cube_type(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_type"
		   });
		addAnnotation
		  (getCUBE_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getCUBE_Framework_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "framework_id"
		   });
		addAnnotation
		  (getCUBE_Is_allowed(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_allowed"
		   });
		addAnnotation
		  (getCUBE_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getCUBE_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getCUBE_Published(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "published"
		   });
		addAnnotation
		  (getCUBE_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getCUBE_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (getCUBE_Version(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "version"
		   });
		addAnnotation
		  (cubE_GROUPEClass,
		   source,
		   new String[] {
			   "name", "CUBE_GROUP",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_GROUP_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getCUBE_GROUP_Cube_group_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_group_id"
		   });
		addAnnotation
		  (getCUBE_GROUP_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getCUBE_GROUP_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getCUBE_GROUP_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (cubE_GROUP_ENUMERATIONEClass,
		   source,
		   new String[] {
			   "name", "CUBE_GROUP_ENUMERATION",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_GROUP_ENUMERATION_Cube_group_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_group_id"
		   });
		addAnnotation
		  (getCUBE_GROUP_ENUMERATION_Cube_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_id"
		   });
		addAnnotation
		  (getCUBE_GROUP_ENUMERATION_Order(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "order"
		   });
		addAnnotation
		  (getCUBE_GROUP_ENUMERATION_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getCUBE_GROUP_ENUMERATION_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (cubE_HIERARCHYEClass,
		   source,
		   new String[] {
			   "name", "CUBE_HIERARCHY",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_Cube_hierarchy_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_hierarchy_id"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_Cube_hierarchy_type(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_hierarchy_type"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_Framework_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "framework_id"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (cubE_HIERARCHY_NODEEClass,
		   source,
		   new String[] {
			   "name", "CUBE_HIERARCHY_NODE",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Cube_group_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_group_id"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Cube_hierarchy_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_hierarchy_id"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Level(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "level"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_NODE_CODE(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "NODE_CODE"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Node_name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "node_name"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Order(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "order"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Parent_node_code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "parent_node_code"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getCUBE_HIERARCHY_NODE_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (cubE_RELATIONSHIPEClass,
		   source,
		   new String[] {
			   "name", "CUBE_RELATIONSHIP",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Cube_relationship_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_relationship_id"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Establishes_integrity(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "establishes_integrity"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Foreign_cube_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "foreign_cube_id"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Foreign_cube_variable_code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "foreign_cube_variable_code"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Primary_cube_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "primary_cube_id"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Primary_cube_variable_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "primary_cube_variable_id"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (getCUBE_RELATIONSHIP_Version(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "version"
		   });
		addAnnotation
		  (cubE_STRUCTUREEClass,
		   source,
		   new String[] {
			   "name", "CUBE_STRUCTURE",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Cube_structure_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_structure_id"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_Version(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "version"
		   });
		addAnnotation
		  (cubE_STRUCTURE_ITEMEClass,
		   source,
		   new String[] {
			   "name", "CUBE_STRUCTURE_ITEM",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Attribute_associated_variable(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "attribute_associated_variable"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Cube_structure_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_structure_id"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Dimension_type(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "dimension_type"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Is_flow(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_flow"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Is_mandatory(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_mandatory"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Member_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_id"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Order(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "order"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Role(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "role"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Subdomain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "subdomain_id"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Variable_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_id"
		   });
		addAnnotation
		  (getCUBE_STRUCTURE_ITEM_Variable_set_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_set_id"
		   });
		addAnnotation
		  (cubE_TO_COMBINATIONEClass,
		   source,
		   new String[] {
			   "name", "CUBE_TO_COMBINATION",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCUBE_TO_COMBINATION_Combination_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "combination_id"
		   });
		addAnnotation
		  (getCUBE_TO_COMBINATION_Cube_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube_id"
		   });
		addAnnotation
		  (frameworkEClass,
		   source,
		   new String[] {
			   "name", "FRAMEWORK",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFRAMEWORK_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getFRAMEWORK_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getFRAMEWORK_Framework_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "framework_id"
		   });
		addAnnotation
		  (getFRAMEWORK_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getFRAMEWORK_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (frameworK_SUBDOMAINEClass,
		   source,
		   new String[] {
			   "name", "FRAMEWORK_SUBDOMAIN",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFRAMEWORK_SUBDOMAIN_Framework_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "framework_id"
		   });
		addAnnotation
		  (getFRAMEWORK_SUBDOMAIN_Subdomain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "subdomain_id"
		   });
		addAnnotation
		  (frameworK_VARIABLE_SETEClass,
		   source,
		   new String[] {
			   "name", "FRAMEWORK_VARIABLE_SET",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFRAMEWORK_VARIABLE_SET_Framework_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "framework_id"
		   });
		addAnnotation
		  (getFRAMEWORK_VARIABLE_SET_Variable_set_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_set_id"
		   });
		addAnnotation
		  (tyP_DMNSNEEnum,
		   source,
		   new String[] {
			   "name", "TYP_DMNSN"
		   });
		addAnnotation
		  (tyP_DMNSNObjectEDataType,
		   source,
		   new String[] {
			   "name", "TYP_DMNSN:Object",
			   "baseType", "TYP_DMNSN"
		   });
		addAnnotation
		  (tyP_RLEEnum,
		   source,
		   new String[] {
			   "name", "TYP_RL"
		   });
		addAnnotation
		  (tyP_RLObjectEDataType,
		   source,
		   new String[] {
			   "name", "TYP_RL:Object",
			   "baseType", "TYP_RL"
		   });
	}

} //Data_definitionPackageImpl
