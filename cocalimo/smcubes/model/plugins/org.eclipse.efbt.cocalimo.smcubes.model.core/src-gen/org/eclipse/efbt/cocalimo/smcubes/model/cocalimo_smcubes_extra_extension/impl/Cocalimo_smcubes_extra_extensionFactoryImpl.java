/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.*;

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
public class Cocalimo_smcubes_extra_extensionFactoryImpl extends EFactoryImpl implements Cocalimo_smcubes_extra_extensionFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Cocalimo_smcubes_extra_extensionFactory init() {
		try {
			Cocalimo_smcubes_extra_extensionFactory theCocalimo_smcubes_extra_extensionFactory = (Cocalimo_smcubes_extra_extensionFactory)EPackage.Registry.INSTANCE.getEFactory(Cocalimo_smcubes_extra_extensionPackage.eNS_URI);
			if (theCocalimo_smcubes_extra_extensionFactory != null) {
				return theCocalimo_smcubes_extra_extensionFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Cocalimo_smcubes_extra_extensionFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cocalimo_smcubes_extra_extensionFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Cocalimo_smcubes_extra_extensionPackage.SMCUBES_EXTRA_MODEL: return createSmcubesExtraModel();
			case Cocalimo_smcubes_extra_extensionPackage.COMBINATION_MODULE: return createCombinationModule();
			case Cocalimo_smcubes_extra_extensionPackage.CUBE_MODULE: return createCubeModule();
			case Cocalimo_smcubes_extra_extensionPackage.MAPPING_DEFINITION_MODULE: return createMappingDefinitionModule();
			case Cocalimo_smcubes_extra_extensionPackage.CUBE_MAPPING_MODULE: return createCubeMappingModule();
			case Cocalimo_smcubes_extra_extensionPackage.MEMBER_MAPPING_MODULE: return createMemberMappingModule();
			case Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE: return createVariableMappingModule();
			case Cocalimo_smcubes_extra_extensionPackage.AXIS_MODULE: return createAxisModule();
			case Cocalimo_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE: return createAxisOrdinateModule();
			case Cocalimo_smcubes_extra_extensionPackage.CELL_POSITION_MODULE: return createCellPositionModule();
			case Cocalimo_smcubes_extra_extensionPackage.ORDINATE_ITEM_MODULE: return createOrdinateItemModule();
			case Cocalimo_smcubes_extra_extensionPackage.REPORT_CELL: return createReportCell();
			case Cocalimo_smcubes_extra_extensionPackage.REPORT_CELL_VALUE: return createReportCellValue();
			case Cocalimo_smcubes_extra_extensionPackage.REPORT_TABLE_MODULE: return createReportTableModule();
			case Cocalimo_smcubes_extra_extensionPackage.TABLE_CELL_MODULE: return createTableCellModule();
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
	public SmcubesExtraModel createSmcubesExtraModel() {
		SmcubesExtraModelImpl smcubesExtraModel = new SmcubesExtraModelImpl();
		return smcubesExtraModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CombinationModule createCombinationModule() {
		CombinationModuleImpl combinationModule = new CombinationModuleImpl();
		return combinationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeModule createCubeModule() {
		CubeModuleImpl cubeModule = new CubeModuleImpl();
		return cubeModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MappingDefinitionModule createMappingDefinitionModule() {
		MappingDefinitionModuleImpl mappingDefinitionModule = new MappingDefinitionModuleImpl();
		return mappingDefinitionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeMappingModule createCubeMappingModule() {
		CubeMappingModuleImpl cubeMappingModule = new CubeMappingModuleImpl();
		return cubeMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MemberMappingModule createMemberMappingModule() {
		MemberMappingModuleImpl memberMappingModule = new MemberMappingModuleImpl();
		return memberMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VariableMappingModule createVariableMappingModule() {
		VariableMappingModuleImpl variableMappingModule = new VariableMappingModuleImpl();
		return variableMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AxisModule createAxisModule() {
		AxisModuleImpl axisModule = new AxisModuleImpl();
		return axisModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AxisOrdinateModule createAxisOrdinateModule() {
		AxisOrdinateModuleImpl axisOrdinateModule = new AxisOrdinateModuleImpl();
		return axisOrdinateModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CellPositionModule createCellPositionModule() {
		CellPositionModuleImpl cellPositionModule = new CellPositionModuleImpl();
		return cellPositionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public OrdinateItemModule createOrdinateItemModule() {
		OrdinateItemModuleImpl ordinateItemModule = new OrdinateItemModuleImpl();
		return ordinateItemModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCell createReportCell() {
		ReportCellImpl reportCell = new ReportCellImpl();
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCellValue createReportCellValue() {
		ReportCellValueImpl reportCellValue = new ReportCellValueImpl();
		return reportCellValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportTableModule createReportTableModule() {
		ReportTableModuleImpl reportTableModule = new ReportTableModuleImpl();
		return reportTableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TableCellModule createTableCellModule() {
		TableCellModuleImpl tableCellModule = new TableCellModuleImpl();
		return tableCellModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Cocalimo_smcubes_extra_extensionPackage getCocalimo_smcubes_extra_extensionPackage() {
		return (Cocalimo_smcubes_extra_extensionPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Cocalimo_smcubes_extra_extensionPackage getPackage() {
		return Cocalimo_smcubes_extra_extensionPackage.eINSTANCE;
	}

} //Cocalimo_smcubes_extra_extensionFactoryImpl
