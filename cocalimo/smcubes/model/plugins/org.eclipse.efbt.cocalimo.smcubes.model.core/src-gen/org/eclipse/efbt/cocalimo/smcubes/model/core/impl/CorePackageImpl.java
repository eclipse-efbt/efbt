/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.Cocalimo_smcubes_core_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.impl.Cocalimo_smcubes_core_extensionPackageImpl;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl;
import org.eclipse.efbt.cocalimo.smcubes.model.core.CoreFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.core.facet_type;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MappingPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl;

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
public class CorePackageImpl extends EPackageImpl implements CorePackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass domainEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass faceT_COLLECTIONEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass faceT_ENUMERATIONEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass facet_typeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass maintenancE_AGENCYEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass membeR_HIERARCHYEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass membeR_HIERARCHY_NODEEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass subdomainEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass subdomaiN_ENUMERATIONEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variablE_SETEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variablE_SET_ENUMERATIONEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EEnum faceT_VALUE_TYPEEEnum = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EDataType faceT_VALUE_TYPEObjectEDataType = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private CorePackageImpl() {
		super(eNS_URI, CoreFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link CorePackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static CorePackage init() {
		if (isInited) return (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);

		// Obtain or create and register package
		Object registeredCorePackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		CorePackageImpl theCorePackage = registeredCorePackage instanceof CorePackageImpl ? (CorePackageImpl)registeredCorePackage : new CorePackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Data_definitionPackageImpl theData_definitionPackage = (Data_definitionPackageImpl)(registeredPackage instanceof Data_definitionPackageImpl ? registeredPackage : Data_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		MappingPackageImpl theMappingPackage = (MappingPackageImpl)(registeredPackage instanceof MappingPackageImpl ? registeredPackage : MappingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);
		RenderingPackageImpl theRenderingPackage = (RenderingPackageImpl)(registeredPackage instanceof RenderingPackageImpl ? registeredPackage : RenderingPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cocalimo_smcubes_core_extensionPackage.eNS_URI);
		Cocalimo_smcubes_core_extensionPackageImpl theCocalimo_smcubes_core_extensionPackage = (Cocalimo_smcubes_core_extensionPackageImpl)(registeredPackage instanceof Cocalimo_smcubes_core_extensionPackageImpl ? registeredPackage : Cocalimo_smcubes_core_extensionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cocalimo_smcubes_extra_extensionPackage.eNS_URI);
		Cocalimo_smcubes_extra_extensionPackageImpl theCocalimo_smcubes_extra_extensionPackage = (Cocalimo_smcubes_extra_extensionPackageImpl)(registeredPackage instanceof Cocalimo_smcubes_extra_extensionPackageImpl ? registeredPackage : Cocalimo_smcubes_extra_extensionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Data_meta_modelPackageImpl theData_meta_modelPackage = (Data_meta_modelPackageImpl)(registeredPackage instanceof Data_meta_modelPackageImpl ? registeredPackage : Data_meta_modelPackage.eINSTANCE);

		// Create package meta-data objects
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();
		theCocalimo_smcubes_core_extensionPackage.createPackageContents();
		theCocalimo_smcubes_extra_extensionPackage.createPackageContents();
		theData_meta_modelPackage.createPackageContents();

		// Initialize created meta-data
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();
		theCocalimo_smcubes_core_extensionPackage.initializePackageContents();
		theCocalimo_smcubes_extra_extensionPackage.initializePackageContents();
		theData_meta_modelPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theCorePackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(CorePackage.eNS_URI, theCorePackage);
		return theCorePackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDOMAIN() {
		return domainEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_Code() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_Data_type() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_Description() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_Domain_id() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDOMAIN_Facet_id() {
		return (EReference)domainEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_Is_enumerated() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_Is_reference() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDOMAIN_Maintenance_agency_id() {
		return (EReference)domainEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_Name() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getDOMAIN_DisplayName() {
		return (EAttribute)domainEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFACET_COLLECTION() {
		return faceT_COLLECTIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFACET_COLLECTION_Code() {
		return (EAttribute)faceT_COLLECTIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFACET_COLLECTION_Facet_id() {
		return (EAttribute)faceT_COLLECTIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFACET_COLLECTION_Facet_value_type() {
		return (EAttribute)faceT_COLLECTIONEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFACET_COLLECTION_Maintenance_agency_id() {
		return (EReference)faceT_COLLECTIONEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFACET_COLLECTION_Name() {
		return (EAttribute)faceT_COLLECTIONEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFACET_ENUMERATION() {
		return faceT_ENUMERATIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFACET_ENUMERATION_Facet_id() {
		return (EReference)faceT_ENUMERATIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFACET_ENUMERATION_Facet_type() {
		return (EReference)faceT_ENUMERATIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getFACET_ENUMERATION_Observation_value() {
		return (EAttribute)faceT_ENUMERATIONEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getfacet_type() {
		return facet_typeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_Decimals() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_EndTime() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_EndValue() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_Interval() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_IsSequence() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_MaxLength() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_MaxValue() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_MinLength() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_MinValue() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_Pattern() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(9);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_StartTime() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(10);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_StartValue() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(11);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getfacet_type_TimeInterval() {
		return (EAttribute)facet_typeEClass.getEStructuralFeatures().get(12);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMAINTENANCE_AGENCY() {
		return maintenancE_AGENCYEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAINTENANCE_AGENCY_Code() {
		return (EAttribute)maintenancE_AGENCYEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAINTENANCE_AGENCY_Maintenance_agency_id() {
		return (EAttribute)maintenancE_AGENCYEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMAINTENANCE_AGENCY_Name() {
		return (EAttribute)maintenancE_AGENCYEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMEMBER() {
		return memberEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_Code() {
		return (EAttribute)memberEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_Description() {
		return (EAttribute)memberEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_Domain_id() {
		return (EReference)memberEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_Maintenance_agency_id() {
		return (EReference)memberEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_Member_id() {
		return (EAttribute)memberEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_Name() {
		return (EAttribute)memberEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_DisplayName() {
		return (EAttribute)memberEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMEMBER_HIERARCHY() {
		return membeR_HIERARCHYEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_Code() {
		return (EAttribute)membeR_HIERARCHYEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_Description() {
		return (EAttribute)membeR_HIERARCHYEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_HIERARCHY_Domain_id() {
		return (EReference)membeR_HIERARCHYEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_HIERARCHY_Maintenance_agency_id() {
		return (EReference)membeR_HIERARCHYEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_Member_hierarchy_id() {
		return (EAttribute)membeR_HIERARCHYEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_Name() {
		return (EAttribute)membeR_HIERARCHYEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMEMBER_HIERARCHY_NODE() {
		return membeR_HIERARCHY_NODEEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_NODE_Comparator() {
		return (EAttribute)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_NODE_Level() {
		return (EAttribute)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_HIERARCHY_NODE_Member_hierarchy_id() {
		return (EReference)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_HIERARCHY_NODE_Member_id() {
		return (EReference)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_NODE_Operator() {
		return (EAttribute)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMEMBER_HIERARCHY_NODE_Parent_member_id() {
		return (EReference)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_NODE_Valid_from() {
		return (EAttribute)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMEMBER_HIERARCHY_NODE_Valid_to() {
		return (EAttribute)membeR_HIERARCHY_NODEEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSUBDOMAIN() {
		return subdomainEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_Code() {
		return (EAttribute)subdomainEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_Description() {
		return (EAttribute)subdomainEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSUBDOMAIN_Domain_id() {
		return (EReference)subdomainEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSUBDOMAIN_Facet_id() {
		return (EReference)subdomainEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_Is_listed() {
		return (EAttribute)subdomainEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_Is_natural() {
		return (EAttribute)subdomainEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSUBDOMAIN_Maintenance_agency_id() {
		return (EReference)subdomainEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_Subdomain_id() {
		return (EAttribute)subdomainEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSUBDOMAIN_Items() {
		return (EReference)subdomainEClass.getEStructuralFeatures().get(8);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSUBDOMAIN_ENUMERATION() {
		return subdomaiN_ENUMERATIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSUBDOMAIN_ENUMERATION_Member_id() {
		return (EReference)subdomaiN_ENUMERATIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_ENUMERATION_Order() {
		return (EAttribute)subdomaiN_ENUMERATIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_ENUMERATION_Valid_from() {
		return (EAttribute)subdomaiN_ENUMERATIONEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSUBDOMAIN_ENUMERATION_Valid_to() {
		return (EAttribute)subdomaiN_ENUMERATIONEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVARIABLE() {
		return variableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_Code() {
		return (EAttribute)variableEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_Description() {
		return (EAttribute)variableEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_Domain_id() {
		return (EReference)variableEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_Maintenance_agency_id() {
		return (EReference)variableEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_Primary_concept() {
		return (EAttribute)variableEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_Variable_id() {
		return (EAttribute)variableEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_Name() {
		return (EAttribute)variableEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_DisplayName() {
		return (EAttribute)variableEClass.getEStructuralFeatures().get(7);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVARIABLE_SET() {
		return variablE_SETEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_Code() {
		return (EAttribute)variablE_SETEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_Description() {
		return (EAttribute)variablE_SETEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_SET_Maintenance_agency_id() {
		return (EReference)variablE_SETEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_Name() {
		return (EAttribute)variablE_SETEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_Variable_set_id() {
		return (EAttribute)variablE_SETEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVARIABLE_SET_ENUMERATION() {
		return variablE_SET_ENUMERATIONEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_ENUMERATION_Is_flow() {
		return (EAttribute)variablE_SET_ENUMERATIONEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_ENUMERATION_Order() {
		return (EAttribute)variablE_SET_ENUMERATIONEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_SET_ENUMERATION_Subdomain_id() {
		return (EReference)variablE_SET_ENUMERATIONEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_ENUMERATION_Valid_from() {
		return (EAttribute)variablE_SET_ENUMERATIONEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getVARIABLE_SET_ENUMERATION_Valid_to() {
		return (EAttribute)variablE_SET_ENUMERATIONEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_SET_ENUMERATION_Variable_id() {
		return (EReference)variablE_SET_ENUMERATIONEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVARIABLE_SET_ENUMERATION_Variable_set_id() {
		return (EReference)variablE_SET_ENUMERATIONEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EEnum getFACET_VALUE_TYPE() {
		return faceT_VALUE_TYPEEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EDataType getFACET_VALUE_TYPEObject() {
		return faceT_VALUE_TYPEObjectEDataType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CoreFactory getCoreFactory() {
		return (CoreFactory)getEFactoryInstance();
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
		domainEClass = createEClass(DOMAIN);
		createEAttribute(domainEClass, DOMAIN__CODE);
		createEAttribute(domainEClass, DOMAIN__DATA_TYPE);
		createEAttribute(domainEClass, DOMAIN__DESCRIPTION);
		createEAttribute(domainEClass, DOMAIN__DOMAIN_ID);
		createEReference(domainEClass, DOMAIN__FACET_ID);
		createEAttribute(domainEClass, DOMAIN__IS_ENUMERATED);
		createEAttribute(domainEClass, DOMAIN__IS_REFERENCE);
		createEReference(domainEClass, DOMAIN__MAINTENANCE_AGENCY_ID);
		createEAttribute(domainEClass, DOMAIN__NAME);
		createEAttribute(domainEClass, DOMAIN__DISPLAY_NAME);

		faceT_COLLECTIONEClass = createEClass(FACET_COLLECTION);
		createEAttribute(faceT_COLLECTIONEClass, FACET_COLLECTION__CODE);
		createEAttribute(faceT_COLLECTIONEClass, FACET_COLLECTION__FACET_ID);
		createEAttribute(faceT_COLLECTIONEClass, FACET_COLLECTION__FACET_VALUE_TYPE);
		createEReference(faceT_COLLECTIONEClass, FACET_COLLECTION__MAINTENANCE_AGENCY_ID);
		createEAttribute(faceT_COLLECTIONEClass, FACET_COLLECTION__NAME);

		faceT_ENUMERATIONEClass = createEClass(FACET_ENUMERATION);
		createEReference(faceT_ENUMERATIONEClass, FACET_ENUMERATION__FACET_ID);
		createEReference(faceT_ENUMERATIONEClass, FACET_ENUMERATION__FACET_TYPE);
		createEAttribute(faceT_ENUMERATIONEClass, FACET_ENUMERATION__OBSERVATION_VALUE);

		facet_typeEClass = createEClass(FACET_TYPE);
		createEAttribute(facet_typeEClass, FACET_TYPE__DECIMALS);
		createEAttribute(facet_typeEClass, FACET_TYPE__END_TIME);
		createEAttribute(facet_typeEClass, FACET_TYPE__END_VALUE);
		createEAttribute(facet_typeEClass, FACET_TYPE__INTERVAL);
		createEAttribute(facet_typeEClass, FACET_TYPE__IS_SEQUENCE);
		createEAttribute(facet_typeEClass, FACET_TYPE__MAX_LENGTH);
		createEAttribute(facet_typeEClass, FACET_TYPE__MAX_VALUE);
		createEAttribute(facet_typeEClass, FACET_TYPE__MIN_LENGTH);
		createEAttribute(facet_typeEClass, FACET_TYPE__MIN_VALUE);
		createEAttribute(facet_typeEClass, FACET_TYPE__PATTERN);
		createEAttribute(facet_typeEClass, FACET_TYPE__START_TIME);
		createEAttribute(facet_typeEClass, FACET_TYPE__START_VALUE);
		createEAttribute(facet_typeEClass, FACET_TYPE__TIME_INTERVAL);

		maintenancE_AGENCYEClass = createEClass(MAINTENANCE_AGENCY);
		createEAttribute(maintenancE_AGENCYEClass, MAINTENANCE_AGENCY__CODE);
		createEAttribute(maintenancE_AGENCYEClass, MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID);
		createEAttribute(maintenancE_AGENCYEClass, MAINTENANCE_AGENCY__NAME);

		memberEClass = createEClass(MEMBER);
		createEAttribute(memberEClass, MEMBER__CODE);
		createEAttribute(memberEClass, MEMBER__DESCRIPTION);
		createEReference(memberEClass, MEMBER__DOMAIN_ID);
		createEReference(memberEClass, MEMBER__MAINTENANCE_AGENCY_ID);
		createEAttribute(memberEClass, MEMBER__MEMBER_ID);
		createEAttribute(memberEClass, MEMBER__NAME);
		createEAttribute(memberEClass, MEMBER__DISPLAY_NAME);

		membeR_HIERARCHYEClass = createEClass(MEMBER_HIERARCHY);
		createEAttribute(membeR_HIERARCHYEClass, MEMBER_HIERARCHY__CODE);
		createEAttribute(membeR_HIERARCHYEClass, MEMBER_HIERARCHY__DESCRIPTION);
		createEReference(membeR_HIERARCHYEClass, MEMBER_HIERARCHY__DOMAIN_ID);
		createEReference(membeR_HIERARCHYEClass, MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID);
		createEAttribute(membeR_HIERARCHYEClass, MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID);
		createEAttribute(membeR_HIERARCHYEClass, MEMBER_HIERARCHY__NAME);

		membeR_HIERARCHY_NODEEClass = createEClass(MEMBER_HIERARCHY_NODE);
		createEAttribute(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__COMPARATOR);
		createEAttribute(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__LEVEL);
		createEReference(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID);
		createEReference(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__MEMBER_ID);
		createEAttribute(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__OPERATOR);
		createEReference(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID);
		createEAttribute(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__VALID_FROM);
		createEAttribute(membeR_HIERARCHY_NODEEClass, MEMBER_HIERARCHY_NODE__VALID_TO);

		subdomainEClass = createEClass(SUBDOMAIN);
		createEAttribute(subdomainEClass, SUBDOMAIN__CODE);
		createEAttribute(subdomainEClass, SUBDOMAIN__DESCRIPTION);
		createEReference(subdomainEClass, SUBDOMAIN__DOMAIN_ID);
		createEReference(subdomainEClass, SUBDOMAIN__FACET_ID);
		createEAttribute(subdomainEClass, SUBDOMAIN__IS_LISTED);
		createEAttribute(subdomainEClass, SUBDOMAIN__IS_NATURAL);
		createEReference(subdomainEClass, SUBDOMAIN__MAINTENANCE_AGENCY_ID);
		createEAttribute(subdomainEClass, SUBDOMAIN__SUBDOMAIN_ID);
		createEReference(subdomainEClass, SUBDOMAIN__ITEMS);

		subdomaiN_ENUMERATIONEClass = createEClass(SUBDOMAIN_ENUMERATION);
		createEReference(subdomaiN_ENUMERATIONEClass, SUBDOMAIN_ENUMERATION__MEMBER_ID);
		createEAttribute(subdomaiN_ENUMERATIONEClass, SUBDOMAIN_ENUMERATION__ORDER);
		createEAttribute(subdomaiN_ENUMERATIONEClass, SUBDOMAIN_ENUMERATION__VALID_FROM);
		createEAttribute(subdomaiN_ENUMERATIONEClass, SUBDOMAIN_ENUMERATION__VALID_TO);

		variableEClass = createEClass(VARIABLE);
		createEAttribute(variableEClass, VARIABLE__CODE);
		createEAttribute(variableEClass, VARIABLE__DESCRIPTION);
		createEReference(variableEClass, VARIABLE__DOMAIN_ID);
		createEReference(variableEClass, VARIABLE__MAINTENANCE_AGENCY_ID);
		createEAttribute(variableEClass, VARIABLE__PRIMARY_CONCEPT);
		createEAttribute(variableEClass, VARIABLE__VARIABLE_ID);
		createEAttribute(variableEClass, VARIABLE__NAME);
		createEAttribute(variableEClass, VARIABLE__DISPLAY_NAME);

		variablE_SETEClass = createEClass(VARIABLE_SET);
		createEAttribute(variablE_SETEClass, VARIABLE_SET__CODE);
		createEAttribute(variablE_SETEClass, VARIABLE_SET__DESCRIPTION);
		createEReference(variablE_SETEClass, VARIABLE_SET__MAINTENANCE_AGENCY_ID);
		createEAttribute(variablE_SETEClass, VARIABLE_SET__NAME);
		createEAttribute(variablE_SETEClass, VARIABLE_SET__VARIABLE_SET_ID);

		variablE_SET_ENUMERATIONEClass = createEClass(VARIABLE_SET_ENUMERATION);
		createEAttribute(variablE_SET_ENUMERATIONEClass, VARIABLE_SET_ENUMERATION__IS_FLOW);
		createEAttribute(variablE_SET_ENUMERATIONEClass, VARIABLE_SET_ENUMERATION__ORDER);
		createEReference(variablE_SET_ENUMERATIONEClass, VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID);
		createEAttribute(variablE_SET_ENUMERATIONEClass, VARIABLE_SET_ENUMERATION__VALID_FROM);
		createEAttribute(variablE_SET_ENUMERATIONEClass, VARIABLE_SET_ENUMERATION__VALID_TO);
		createEReference(variablE_SET_ENUMERATIONEClass, VARIABLE_SET_ENUMERATION__VARIABLE_ID);
		createEReference(variablE_SET_ENUMERATIONEClass, VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID);

		// Create enums
		faceT_VALUE_TYPEEEnum = createEEnum(FACET_VALUE_TYPE);

		// Create data types
		faceT_VALUE_TYPEObjectEDataType = createEDataType(FACET_VALUE_TYPE_OBJECT);
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
		Data_meta_modelPackage theData_meta_modelPackage = (Data_meta_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		subdomainEClass.getESuperTypes().add(theData_meta_modelPackage.getClassifier());

		// Initialize classes, features, and operations; add parameters
		initEClass(domainEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, "DOMAIN", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getDOMAIN_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDOMAIN_Data_type(), this.getFACET_VALUE_TYPE(), "data_type", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDOMAIN_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDOMAIN_Domain_id(), ecorePackage.getEString(), "domain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDOMAIN_Facet_id(), this.getFACET_COLLECTION(), null, "facet_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDOMAIN_Is_enumerated(), ecorePackage.getEBoolean(), "is_enumerated", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDOMAIN_Is_reference(), ecorePackage.getEBoolean(), "is_reference", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDOMAIN_Maintenance_agency_id(), this.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDOMAIN_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDOMAIN_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(faceT_COLLECTIONEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION.class, "FACET_COLLECTION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getFACET_COLLECTION_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFACET_COLLECTION_Facet_id(), ecorePackage.getEString(), "facet_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFACET_COLLECTION_Facet_value_type(), this.getFACET_VALUE_TYPE(), "facet_value_type", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFACET_COLLECTION_Maintenance_agency_id(), this.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFACET_COLLECTION_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(faceT_ENUMERATIONEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION.class, "FACET_ENUMERATION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFACET_ENUMERATION_Facet_id(), this.getFACET_COLLECTION(), null, "facet_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFACET_ENUMERATION_Facet_type(), this.getfacet_type(), null, "facet_type", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFACET_ENUMERATION_Observation_value(), ecorePackage.getEString(), "observation_value", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(facet_typeEClass, facet_type.class, "facet_type", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getfacet_type_Decimals(), ecorePackage.getEInt(), "decimals", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_EndTime(), ecorePackage.getEString(), "endTime", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_EndValue(), ecorePackage.getEInt(), "endValue", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_Interval(), ecorePackage.getEString(), "interval", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_IsSequence(), ecorePackage.getEString(), "isSequence", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_MaxLength(), ecorePackage.getEInt(), "maxLength", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_MaxValue(), ecorePackage.getEInt(), "maxValue", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_MinLength(), ecorePackage.getEInt(), "minLength", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_MinValue(), ecorePackage.getEInt(), "minValue", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_Pattern(), ecorePackage.getEString(), "pattern", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_StartTime(), ecorePackage.getEString(), "startTime", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_StartValue(), ecorePackage.getEInt(), "startValue", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getfacet_type_TimeInterval(), ecorePackage.getEString(), "timeInterval", null, 0, 1, facet_type.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(maintenancE_AGENCYEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY.class, "MAINTENANCE_AGENCY", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getMAINTENANCE_AGENCY_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMAINTENANCE_AGENCY_Maintenance_agency_id(), ecorePackage.getEString(), "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMAINTENANCE_AGENCY_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, "MEMBER", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getMEMBER_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_Domain_id(), this.getDOMAIN(), null, "domain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_Maintenance_agency_id(), this.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_Member_id(), ecorePackage.getEString(), "member_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(membeR_HIERARCHYEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY.class, "MEMBER_HIERARCHY", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getMEMBER_HIERARCHY_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_HIERARCHY_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_HIERARCHY_Domain_id(), this.getDOMAIN(), null, "domain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_HIERARCHY_Maintenance_agency_id(), this.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_HIERARCHY_Member_hierarchy_id(), ecorePackage.getEString(), "member_hierarchy_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_HIERARCHY_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(membeR_HIERARCHY_NODEEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, "MEMBER_HIERARCHY_NODE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getMEMBER_HIERARCHY_NODE_Comparator(), ecorePackage.getEString(), "comparator", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_HIERARCHY_NODE_Level(), ecorePackage.getEInt(), "level", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_HIERARCHY_NODE_Member_hierarchy_id(), this.getMEMBER_HIERARCHY(), null, "member_hierarchy_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_HIERARCHY_NODE_Member_id(), this.getMEMBER(), null, "member_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_HIERARCHY_NODE_Operator(), ecorePackage.getEString(), "operator", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMEMBER_HIERARCHY_NODE_Parent_member_id(), this.getMEMBER(), null, "parent_member_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_HIERARCHY_NODE_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMEMBER_HIERARCHY_NODE_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(subdomainEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, "SUBDOMAIN", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getSUBDOMAIN_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSUBDOMAIN_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSUBDOMAIN_Domain_id(), this.getDOMAIN(), null, "domain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSUBDOMAIN_Facet_id(), this.getFACET_COLLECTION(), null, "facet_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSUBDOMAIN_Is_listed(), ecorePackage.getEBoolean(), "is_listed", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSUBDOMAIN_Is_natural(), ecorePackage.getEBoolean(), "is_natural", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSUBDOMAIN_Maintenance_agency_id(), this.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSUBDOMAIN_Subdomain_id(), ecorePackage.getEString(), "subdomain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSUBDOMAIN_Items(), this.getSUBDOMAIN_ENUMERATION(), null, "items", null, 0, -1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(subdomaiN_ENUMERATIONEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION.class, "SUBDOMAIN_ENUMERATION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSUBDOMAIN_ENUMERATION_Member_id(), this.getMEMBER(), null, "member_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSUBDOMAIN_ENUMERATION_Order(), ecorePackage.getEInt(), "order", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSUBDOMAIN_ENUMERATION_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSUBDOMAIN_ENUMERATION_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variableEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, "VARIABLE", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getVARIABLE_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_Domain_id(), this.getDOMAIN(), null, "domain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_Maintenance_agency_id(), this.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_Primary_concept(), ecorePackage.getEString(), "primary_concept", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_Variable_id(), ecorePackage.getEString(), "variable_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_DisplayName(), ecorePackage.getEString(), "displayName", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variablE_SETEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET.class, "VARIABLE_SET", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getVARIABLE_SET_Code(), ecorePackage.getEString(), "code", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_SET_Description(), ecorePackage.getEString(), "description", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_SET_Maintenance_agency_id(), this.getMAINTENANCE_AGENCY(), null, "maintenance_agency_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_SET_Name(), ecorePackage.getEString(), "name", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_SET_Variable_set_id(), ecorePackage.getEString(), "variable_set_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variablE_SET_ENUMERATIONEClass, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, "VARIABLE_SET_ENUMERATION", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getVARIABLE_SET_ENUMERATION_Is_flow(), ecorePackage.getEBoolean(), "is_flow", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_SET_ENUMERATION_Order(), ecorePackage.getEInt(), "order", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_SET_ENUMERATION_Subdomain_id(), this.getSUBDOMAIN(), null, "subdomain_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_SET_ENUMERATION_Valid_from(), ecorePackage.getEDate(), "valid_from", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getVARIABLE_SET_ENUMERATION_Valid_to(), ecorePackage.getEDate(), "valid_to", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_SET_ENUMERATION_Variable_id(), this.getVARIABLE(), null, "variable_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getVARIABLE_SET_ENUMERATION_Variable_set_id(), this.getVARIABLE_SET(), null, "variable_set_id", null, 0, 1, org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET_ENUMERATION.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Initialize enums and add enum literals
		initEEnum(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.class, "FACET_VALUE_TYPE");
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.BIG_INTEGER);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.BOOLEAN);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.DATE_TIME);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.DAY_MONTH_DAY_MONTH);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.DECIMAL);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.DOUBLE);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.DURATION);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.FLOAT);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.GREGORIAN_DAY);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.GREGORIAN_MONTH);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.GREGORIAN_YEAR);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.INTEGER);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.LONG);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.SHORT);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.STRING);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.TIME);
		addEEnumLiteral(faceT_VALUE_TYPEEEnum, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.URI);

		// Initialize data types
		initEDataType(faceT_VALUE_TYPEObjectEDataType, org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE.class, "FACET_VALUE_TYPEObject", IS_SERIALIZABLE, IS_GENERATED_INSTANCE_CLASS);

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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r This file contains a representation of the core package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html"
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
		  (domainEClass,
		   source,
		   new String[] {
			   "name", "DOMAIN",
			   "kind", "empty"
		   });
		addAnnotation
		  (getDOMAIN_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getDOMAIN_Data_type(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "data_type"
		   });
		addAnnotation
		  (getDOMAIN_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getDOMAIN_Domain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "domain_id"
		   });
		addAnnotation
		  (getDOMAIN_Facet_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "facet_id"
		   });
		addAnnotation
		  (getDOMAIN_Is_enumerated(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_enumerated"
		   });
		addAnnotation
		  (getDOMAIN_Is_reference(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_reference"
		   });
		addAnnotation
		  (getDOMAIN_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getDOMAIN_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (faceT_COLLECTIONEClass,
		   source,
		   new String[] {
			   "name", "FACET_COLLECTION",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFACET_COLLECTION_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getFACET_COLLECTION_Facet_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "facet_id"
		   });
		addAnnotation
		  (getFACET_COLLECTION_Facet_value_type(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "facet_value_type"
		   });
		addAnnotation
		  (getFACET_COLLECTION_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getFACET_COLLECTION_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (faceT_ENUMERATIONEClass,
		   source,
		   new String[] {
			   "name", "FACET_ENUMERATION",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFACET_ENUMERATION_Facet_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "facet_id"
		   });
		addAnnotation
		  (getFACET_ENUMERATION_Facet_type(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "facet_type"
		   });
		addAnnotation
		  (getFACET_ENUMERATION_Observation_value(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "observation_value"
		   });
		addAnnotation
		  (facet_typeEClass,
		   source,
		   new String[] {
			   "name", "facet_type",
			   "kind", "empty"
		   });
		addAnnotation
		  (getfacet_type_Decimals(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "decimals"
		   });
		addAnnotation
		  (getfacet_type_EndTime(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "endTime"
		   });
		addAnnotation
		  (getfacet_type_EndValue(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "endValue"
		   });
		addAnnotation
		  (getfacet_type_Interval(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "interval"
		   });
		addAnnotation
		  (getfacet_type_IsSequence(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "isSequence"
		   });
		addAnnotation
		  (getfacet_type_MaxLength(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maxLength"
		   });
		addAnnotation
		  (getfacet_type_MaxValue(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maxValue"
		   });
		addAnnotation
		  (getfacet_type_MinLength(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "minLength"
		   });
		addAnnotation
		  (getfacet_type_MinValue(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "minValue"
		   });
		addAnnotation
		  (getfacet_type_Pattern(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "pattern"
		   });
		addAnnotation
		  (getfacet_type_StartTime(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "startTime"
		   });
		addAnnotation
		  (getfacet_type_StartValue(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "startValue"
		   });
		addAnnotation
		  (getfacet_type_TimeInterval(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "timeInterval"
		   });
		addAnnotation
		  (faceT_VALUE_TYPEEEnum,
		   source,
		   new String[] {
			   "name", "FACET_VALUE_TYPE"
		   });
		addAnnotation
		  (faceT_VALUE_TYPEObjectEDataType,
		   source,
		   new String[] {
			   "name", "FACET_VALUE_TYPE:Object",
			   "baseType", "FACET_VALUE_TYPE"
		   });
		addAnnotation
		  (maintenancE_AGENCYEClass,
		   source,
		   new String[] {
			   "name", "MAINTENANCE_AGENCY",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMAINTENANCE_AGENCY_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getMAINTENANCE_AGENCY_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getMAINTENANCE_AGENCY_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (memberEClass,
		   source,
		   new String[] {
			   "name", "MEMBER",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMEMBER_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getMEMBER_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getMEMBER_Domain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "domain_id"
		   });
		addAnnotation
		  (getMEMBER_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (membeR_HIERARCHYEClass,
		   source,
		   new String[] {
			   "name", "MEMBER_HIERARCHY",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_Domain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "domain_id"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_Member_hierarchy_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_hierarchy_id"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (membeR_HIERARCHY_NODEEClass,
		   source,
		   new String[] {
			   "name", "MEMBER_HIERARCHY_NODE",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Comparator(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "comparator"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Level(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "level"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Member_hierarchy_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_hierarchy_id"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Member_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_id"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Operator(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "operator"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Parent_member_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "parent_member_id"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getMEMBER_HIERARCHY_NODE_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (subdomainEClass,
		   source,
		   new String[] {
			   "name", "SUBDOMAIN",
			   "kind", "empty"
		   });
		addAnnotation
		  (getSUBDOMAIN_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getSUBDOMAIN_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getSUBDOMAIN_Domain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "domain_id"
		   });
		addAnnotation
		  (getSUBDOMAIN_Facet_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "facet_id"
		   });
		addAnnotation
		  (getSUBDOMAIN_Is_listed(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "IS_LISTED"
		   });
		addAnnotation
		  (getSUBDOMAIN_Is_natural(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_natural"
		   });
		addAnnotation
		  (getSUBDOMAIN_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getSUBDOMAIN_Subdomain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "subdomain_id"
		   });
		addAnnotation
		  (getSUBDOMAIN_ENUMERATION_Member_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "member_id"
		   });
		addAnnotation
		  (getSUBDOMAIN_ENUMERATION_Order(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "order"
		   });
		addAnnotation
		  (getSUBDOMAIN_ENUMERATION_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getSUBDOMAIN_ENUMERATION_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (variableEClass,
		   source,
		   new String[] {
			   "name", "VARIABLE",
			   "kind", "empty"
		   });
		addAnnotation
		  (getVARIABLE_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getVARIABLE_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getVARIABLE_Domain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "domain_id"
		   });
		addAnnotation
		  (getVARIABLE_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getVARIABLE_Primary_concept(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "primary_concept"
		   });
		addAnnotation
		  (variablE_SETEClass,
		   source,
		   new String[] {
			   "name", "VARIABLE_SET",
			   "kind", "empty"
		   });
		addAnnotation
		  (getVARIABLE_SET_Code(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "code"
		   });
		addAnnotation
		  (getVARIABLE_SET_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
		addAnnotation
		  (getVARIABLE_SET_Maintenance_agency_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "maintenance_agency_id"
		   });
		addAnnotation
		  (getVARIABLE_SET_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getVARIABLE_SET_Variable_set_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_set_id"
		   });
		addAnnotation
		  (variablE_SET_ENUMERATIONEClass,
		   source,
		   new String[] {
			   "name", "VARIABLE_SET_ENUMERATION",
			   "kind", "empty"
		   });
		addAnnotation
		  (getVARIABLE_SET_ENUMERATION_Is_flow(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "is_flow"
		   });
		addAnnotation
		  (getVARIABLE_SET_ENUMERATION_Order(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "order"
		   });
		addAnnotation
		  (getVARIABLE_SET_ENUMERATION_Subdomain_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "subdomain_id"
		   });
		addAnnotation
		  (getVARIABLE_SET_ENUMERATION_Valid_from(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_from"
		   });
		addAnnotation
		  (getVARIABLE_SET_ENUMERATION_Valid_to(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "valid_to"
		   });
		addAnnotation
		  (getVARIABLE_SET_ENUMERATION_Variable_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_id"
		   });
		addAnnotation
		  (getVARIABLE_SET_ENUMERATION_Variable_set_id(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "variable_set_id"
		   });
	}

} //CorePackageImpl
