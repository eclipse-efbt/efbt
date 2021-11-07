/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.AxisModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.AxisOrdinateModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.CellPositionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.CombinationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.CubeMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MappingDefinitionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MemberHierarchyModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MemberMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.OrdinateItemModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.ReportCell;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.ReportCellValue;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.ReportTableModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.TableCellModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.VariableMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.VariableModule;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.impl.CorePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.Data_definitionPackageImpl;

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
public class Cocalimo_smcubes_extensionPackageImpl extends EPackageImpl implements Cocalimo_smcubes_extensionPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass smcubesModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass combinationModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass domainModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberHierarchyModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variableModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass mappingDefinitionModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeMappingModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberMappingModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass variableMappingModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass axisModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass axisOrdinateModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellPositionModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass ordinateItemModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellValueEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportTableModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tableCellModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Cocalimo_smcubes_extensionPackageImpl() {
		super(eNS_URI, Cocalimo_smcubes_extensionFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Cocalimo_smcubes_extensionPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Cocalimo_smcubes_extensionPackage init() {
		if (isInited) return (Cocalimo_smcubes_extensionPackage)EPackage.Registry.INSTANCE.getEPackage(Cocalimo_smcubes_extensionPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredCocalimo_smcubes_extensionPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Cocalimo_smcubes_extensionPackageImpl theCocalimo_smcubes_extensionPackage = registeredCocalimo_smcubes_extensionPackage instanceof Cocalimo_smcubes_extensionPackageImpl ? (Cocalimo_smcubes_extensionPackageImpl)registeredCocalimo_smcubes_extensionPackage : new Cocalimo_smcubes_extensionPackageImpl();

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

		// Create package meta-data objects
		theCocalimo_smcubes_extensionPackage.createPackageContents();
		theCorePackage.createPackageContents();
		theData_definitionPackage.createPackageContents();
		theMappingPackage.createPackageContents();
		theRenderingPackage.createPackageContents();

		// Initialize created meta-data
		theCocalimo_smcubes_extensionPackage.initializePackageContents();
		theCorePackage.initializePackageContents();
		theData_definitionPackage.initializePackageContents();
		theMappingPackage.initializePackageContents();
		theRenderingPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theCocalimo_smcubes_extensionPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Cocalimo_smcubes_extensionPackage.eNS_URI, theCocalimo_smcubes_extensionPackage);
		return theCocalimo_smcubes_extensionPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSmcubesModel() {
		return smcubesModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSmcubesModel_Domains() {
		return (EReference)smcubesModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSmcubesModel_Members() {
		return (EReference)smcubesModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSmcubesModel_Variables() {
		return (EReference)smcubesModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSmcubesModel_Cubes() {
		return (EReference)smcubesModelEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSmcubesModel_Combinations() {
		return (EReference)smcubesModelEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSmcubesModel_Mappings() {
		return (EReference)smcubesModelEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSmcubesModel_Name() {
		return (EAttribute)smcubesModelEClass.getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCombinationModule() {
		return combinationModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCombinationModule_Combinations() {
		return (EReference)combinationModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCombinationModule_Combination_items() {
		return (EReference)combinationModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCombinationModule_CubeToCombinationLinks() {
		return (EReference)combinationModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCubeModule() {
		return cubeModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeModule_Cubes() {
		return (EReference)cubeModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeModule_CubeStructures() {
		return (EReference)cubeModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeModule_CubeStructureItems() {
		return (EReference)cubeModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDomainModule() {
		return domainModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDomainModule_Domains() {
		return (EReference)domainModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberHierarchyModule() {
		return memberHierarchyModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberHierarchyModule_MemberHierarchies() {
		return (EReference)memberHierarchyModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberHierarchyModule_MemberHierarchiesNodes() {
		return (EReference)memberHierarchyModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberModule() {
		return memberModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberModule_Members() {
		return (EReference)memberModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVariableModule() {
		return variableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVariableModule_Variables() {
		return (EReference)variableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMappingDefinitionModule() {
		return mappingDefinitionModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMappingDefinitionModule_Mappings() {
		return (EReference)mappingDefinitionModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCubeMappingModule() {
		return cubeMappingModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeMappingModule_CubeMappings() {
		return (EReference)cubeMappingModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberMappingModule() {
		return memberMappingModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberMappingModule_MemberMappings() {
		return (EReference)memberMappingModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getVariableMappingModule() {
		return variableMappingModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getVariableMappingModule_VariableMappings() {
		return (EReference)variableMappingModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAxisModule() {
		return axisModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAxisModule_Axes() {
		return (EReference)axisModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAxisOrdinateModule() {
		return axisOrdinateModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAxisOrdinateModule_Axis_ordinates() {
		return (EReference)axisOrdinateModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCellPositionModule() {
		return cellPositionModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCellPositionModule_ReportTables() {
		return (EReference)cellPositionModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getOrdinateItemModule() {
		return ordinateItemModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getOrdinateItemModule_OrdinateItems() {
		return (EReference)ordinateItemModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCell() {
		return reportCellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCell_Combination() {
		return (EReference)reportCellEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getReportCell_Name() {
		return (EAttribute)reportCellEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellValue() {
		return reportCellValueEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCellValue_ReportCell() {
		return (EReference)reportCellValueEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getReportCellValue_Value() {
		return (EAttribute)reportCellValueEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportTableModule() {
		return reportTableModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportTableModule_ReportTables() {
		return (EReference)reportTableModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTableCellModule() {
		return tableCellModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTableCellModule_TableCells() {
		return (EReference)tableCellModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Cocalimo_smcubes_extensionFactory getCocalimo_smcubes_extensionFactory() {
		return (Cocalimo_smcubes_extensionFactory)getEFactoryInstance();
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
		smcubesModelEClass = createEClass(SMCUBES_MODEL);
		createEReference(smcubesModelEClass, SMCUBES_MODEL__DOMAINS);
		createEReference(smcubesModelEClass, SMCUBES_MODEL__MEMBERS);
		createEReference(smcubesModelEClass, SMCUBES_MODEL__VARIABLES);
		createEReference(smcubesModelEClass, SMCUBES_MODEL__CUBES);
		createEReference(smcubesModelEClass, SMCUBES_MODEL__COMBINATIONS);
		createEReference(smcubesModelEClass, SMCUBES_MODEL__MAPPINGS);
		createEAttribute(smcubesModelEClass, SMCUBES_MODEL__NAME);

		combinationModuleEClass = createEClass(COMBINATION_MODULE);
		createEReference(combinationModuleEClass, COMBINATION_MODULE__COMBINATIONS);
		createEReference(combinationModuleEClass, COMBINATION_MODULE__COMBINATION_ITEMS);
		createEReference(combinationModuleEClass, COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS);

		cubeModuleEClass = createEClass(CUBE_MODULE);
		createEReference(cubeModuleEClass, CUBE_MODULE__CUBES);
		createEReference(cubeModuleEClass, CUBE_MODULE__CUBE_STRUCTURES);
		createEReference(cubeModuleEClass, CUBE_MODULE__CUBE_STRUCTURE_ITEMS);

		domainModuleEClass = createEClass(DOMAIN_MODULE);
		createEReference(domainModuleEClass, DOMAIN_MODULE__DOMAINS);

		memberHierarchyModuleEClass = createEClass(MEMBER_HIERARCHY_MODULE);
		createEReference(memberHierarchyModuleEClass, MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES);
		createEReference(memberHierarchyModuleEClass, MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES);

		memberModuleEClass = createEClass(MEMBER_MODULE);
		createEReference(memberModuleEClass, MEMBER_MODULE__MEMBERS);

		variableModuleEClass = createEClass(VARIABLE_MODULE);
		createEReference(variableModuleEClass, VARIABLE_MODULE__VARIABLES);

		mappingDefinitionModuleEClass = createEClass(MAPPING_DEFINITION_MODULE);
		createEReference(mappingDefinitionModuleEClass, MAPPING_DEFINITION_MODULE__MAPPINGS);

		cubeMappingModuleEClass = createEClass(CUBE_MAPPING_MODULE);
		createEReference(cubeMappingModuleEClass, CUBE_MAPPING_MODULE__CUBE_MAPPINGS);

		memberMappingModuleEClass = createEClass(MEMBER_MAPPING_MODULE);
		createEReference(memberMappingModuleEClass, MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS);

		variableMappingModuleEClass = createEClass(VARIABLE_MAPPING_MODULE);
		createEReference(variableMappingModuleEClass, VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS);

		axisModuleEClass = createEClass(AXIS_MODULE);
		createEReference(axisModuleEClass, AXIS_MODULE__AXES);

		axisOrdinateModuleEClass = createEClass(AXIS_ORDINATE_MODULE);
		createEReference(axisOrdinateModuleEClass, AXIS_ORDINATE_MODULE__AXIS_ORDINATES);

		cellPositionModuleEClass = createEClass(CELL_POSITION_MODULE);
		createEReference(cellPositionModuleEClass, CELL_POSITION_MODULE__REPORT_TABLES);

		ordinateItemModuleEClass = createEClass(ORDINATE_ITEM_MODULE);
		createEReference(ordinateItemModuleEClass, ORDINATE_ITEM_MODULE__ORDINATE_ITEMS);

		reportCellEClass = createEClass(REPORT_CELL);
		createEReference(reportCellEClass, REPORT_CELL__COMBINATION);
		createEAttribute(reportCellEClass, REPORT_CELL__NAME);

		reportCellValueEClass = createEClass(REPORT_CELL_VALUE);
		createEReference(reportCellValueEClass, REPORT_CELL_VALUE__REPORT_CELL);
		createEAttribute(reportCellValueEClass, REPORT_CELL_VALUE__VALUE);

		reportTableModuleEClass = createEClass(REPORT_TABLE_MODULE);
		createEReference(reportTableModuleEClass, REPORT_TABLE_MODULE__REPORT_TABLES);

		tableCellModuleEClass = createEClass(TABLE_CELL_MODULE);
		createEReference(tableCellModuleEClass, TABLE_CELL_MODULE__TABLE_CELLS);
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
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		MappingPackage theMappingPackage = (MappingPackage)EPackage.Registry.INSTANCE.getEPackage(MappingPackage.eNS_URI);
		RenderingPackage theRenderingPackage = (RenderingPackage)EPackage.Registry.INSTANCE.getEPackage(RenderingPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		combinationModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		cubeModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		domainModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberHierarchyModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		variableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		mappingDefinitionModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		cubeMappingModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberMappingModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		variableMappingModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		axisModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		axisOrdinateModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		cellPositionModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		ordinateItemModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		reportTableModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		tableCellModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(smcubesModelEClass, SmcubesModel.class, "SmcubesModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSmcubesModel_Domains(), this.getDomainModule(), null, "domains", null, 0, -1, SmcubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSmcubesModel_Members(), this.getMemberModule(), null, "members", null, 0, -1, SmcubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSmcubesModel_Variables(), this.getVariableModule(), null, "variables", null, 0, -1, SmcubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSmcubesModel_Cubes(), this.getCubeModule(), null, "cubes", null, 0, -1, SmcubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSmcubesModel_Combinations(), this.getCombinationModule(), null, "combinations", null, 0, -1, SmcubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSmcubesModel_Mappings(), this.getMappingDefinitionModule(), null, "mappings", null, 0, -1, SmcubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSmcubesModel_Name(), ecorePackage.getEString(), "name", null, 0, 1, SmcubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(combinationModuleEClass, CombinationModule.class, "CombinationModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCombinationModule_Combinations(), theData_definitionPackage.getCOMBINATION(), null, "combinations", null, 0, -1, CombinationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCombinationModule_Combination_items(), theData_definitionPackage.getCOMBINATION_ITEM(), null, "combination_items", null, 0, -1, CombinationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCombinationModule_CubeToCombinationLinks(), theData_definitionPackage.getCUBE_TO_COMBINATION(), null, "cubeToCombinationLinks", null, 0, -1, CombinationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubeModuleEClass, CubeModule.class, "CubeModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCubeModule_Cubes(), theData_definitionPackage.getCUBE(), null, "cubes", null, 0, -1, CubeModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeModule_CubeStructures(), theData_definitionPackage.getCUBE_STRUCTURE(), null, "cubeStructures", null, 0, -1, CubeModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeModule_CubeStructureItems(), theData_definitionPackage.getCUBE_STRUCTURE_ITEM(), null, "cubeStructureItems", null, 0, -1, CubeModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(domainModuleEClass, DomainModule.class, "DomainModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDomainModule_Domains(), theCorePackage.getDOMAIN(), null, "domains", null, 0, -1, DomainModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberHierarchyModuleEClass, MemberHierarchyModule.class, "MemberHierarchyModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberHierarchyModule_MemberHierarchies(), theCorePackage.getMEMBER_HIERARCHY(), null, "memberHierarchies", null, 0, -1, MemberHierarchyModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getMemberHierarchyModule_MemberHierarchiesNodes(), theCorePackage.getMEMBER_HIERARCHY_NODE(), null, "memberHierarchiesNodes", null, 0, -1, MemberHierarchyModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberModuleEClass, MemberModule.class, "MemberModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberModule_Members(), theCorePackage.getMEMBER(), null, "members", null, 0, -1, MemberModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variableModuleEClass, VariableModule.class, "VariableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVariableModule_Variables(), theCorePackage.getVARIABLE(), null, "variables", null, 0, -1, VariableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(mappingDefinitionModuleEClass, MappingDefinitionModule.class, "MappingDefinitionModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMappingDefinitionModule_Mappings(), theMappingPackage.getMAPPING_DEFINITION(), null, "mappings", null, 0, -1, MappingDefinitionModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubeMappingModuleEClass, CubeMappingModule.class, "CubeMappingModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCubeMappingModule_CubeMappings(), theMappingPackage.getCUBE_MAPPING(), null, "cubeMappings", null, 0, -1, CubeMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(memberMappingModuleEClass, MemberMappingModule.class, "MemberMappingModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberMappingModule_MemberMappings(), theMappingPackage.getMEMBER_MAPPING(), null, "memberMappings", null, 0, -1, MemberMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(variableMappingModuleEClass, VariableMappingModule.class, "VariableMappingModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getVariableMappingModule_VariableMappings(), theMappingPackage.getVARIABLE_MAPPING(), null, "variableMappings", null, 0, -1, VariableMappingModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(axisModuleEClass, AxisModule.class, "AxisModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAxisModule_Axes(), theRenderingPackage.getAXIS(), null, "axes", null, 0, -1, AxisModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(axisOrdinateModuleEClass, AxisOrdinateModule.class, "AxisOrdinateModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAxisOrdinateModule_Axis_ordinates(), theRenderingPackage.getAXIS_ORDINATE(), null, "axis_ordinates", null, 0, -1, AxisOrdinateModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cellPositionModuleEClass, CellPositionModule.class, "CellPositionModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCellPositionModule_ReportTables(), theRenderingPackage.getCELL_POSITION(), null, "reportTables", null, 0, -1, CellPositionModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(ordinateItemModuleEClass, OrdinateItemModule.class, "OrdinateItemModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getOrdinateItemModule_OrdinateItems(), theRenderingPackage.getORDINATE_ITEM(), null, "ordinateItems", null, 0, -1, OrdinateItemModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportCellEClass, ReportCell.class, "ReportCell", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportCell_Combination(), theData_definitionPackage.getCOMBINATION(), null, "combination", null, 0, 1, ReportCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getReportCell_Name(), ecorePackage.getEString(), "name", null, 0, 1, ReportCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportCellValueEClass, ReportCellValue.class, "ReportCellValue", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportCellValue_ReportCell(), this.getReportCell(), null, "reportCell", null, 0, 1, ReportCellValue.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getReportCellValue_Value(), ecorePackage.getEString(), "value", null, 0, 1, ReportCellValue.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportTableModuleEClass, ReportTableModule.class, "ReportTableModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportTableModule_ReportTables(), theRenderingPackage.getTABLE(), null, "reportTables", null, 0, -1, ReportTableModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(tableCellModuleEClass, TableCellModule.class, "TableCellModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTableCellModule_TableCells(), theRenderingPackage.getTABLE_CELL(), null, "tableCells", null, 0, -1, TableCellModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
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
		  (combinationModuleEClass,
		   source,
		   new String[] {
			   "name", "CombinationModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getCombinationModule_Combinations(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "combinations"
		   });
		addAnnotation
		  (getCombinationModule_Combination_items(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "combination_items"
		   });
		addAnnotation
		  (getCombinationModule_CubeToCombinationLinks(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubeToCombinationLinks"
		   });
		addAnnotation
		  (cubeModuleEClass,
		   source,
		   new String[] {
			   "name", "CubeModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getCubeModule_Cubes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubes"
		   });
		addAnnotation
		  (getCubeModule_CubeStructures(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubeStructures"
		   });
		addAnnotation
		  (getCubeModule_CubeStructureItems(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "cubeStructureItems"
		   });
		addAnnotation
		  (domainModuleEClass,
		   source,
		   new String[] {
			   "name", "DomainModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getDomainModule_Domains(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "domains"
		   });
		addAnnotation
		  (memberHierarchyModuleEClass,
		   source,
		   new String[] {
			   "name", "MemberHierarchyModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getMemberHierarchyModule_MemberHierarchies(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "memberHierarchies"
		   });
		addAnnotation
		  (getMemberHierarchyModule_MemberHierarchiesNodes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "memberHierarchiesNodes"
		   });
		addAnnotation
		  (memberModuleEClass,
		   source,
		   new String[] {
			   "name", "MemberModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getMemberModule_Members(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "members"
		   });
		addAnnotation
		  (variableModuleEClass,
		   source,
		   new String[] {
			   "name", "VariableModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getVariableModule_Variables(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "variables"
		   });
		addAnnotation
		  (axisModuleEClass,
		   source,
		   new String[] {
			   "name", "AxisModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAxisModule_Axes(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "axes"
		   });
		addAnnotation
		  (axisOrdinateModuleEClass,
		   source,
		   new String[] {
			   "name", "AxisOrdinateModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAxisOrdinateModule_Axis_ordinates(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "axis_ordinates"
		   });
		addAnnotation
		  (cellPositionModuleEClass,
		   source,
		   new String[] {
			   "name", "CellPositionModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getCellPositionModule_ReportTables(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "reportTables"
		   });
		addAnnotation
		  (ordinateItemModuleEClass,
		   source,
		   new String[] {
			   "name", "OrdinateItemModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getOrdinateItemModule_OrdinateItems(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "ordinateItems"
		   });
		addAnnotation
		  (reportCellEClass,
		   source,
		   new String[] {
			   "name", "ReportCell",
			   "kind", "empty"
		   });
		addAnnotation
		  (getReportCell_Combination(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "combination"
		   });
		addAnnotation
		  (reportCellValueEClass,
		   source,
		   new String[] {
			   "name", "ReportCellValue",
			   "kind", "empty"
		   });
		addAnnotation
		  (getReportCellValue_ReportCell(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "reportCell"
		   });
		addAnnotation
		  (reportTableModuleEClass,
		   source,
		   new String[] {
			   "name", "ReportTableModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getReportTableModule_ReportTables(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "reportTables"
		   });
		addAnnotation
		  (tableCellModuleEClass,
		   source,
		   new String[] {
			   "name", "TableCellModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getTableCellModule_TableCells(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "tableCells"
		   });
	}

} //Cocalimo_smcubes_extensionPackageImpl
