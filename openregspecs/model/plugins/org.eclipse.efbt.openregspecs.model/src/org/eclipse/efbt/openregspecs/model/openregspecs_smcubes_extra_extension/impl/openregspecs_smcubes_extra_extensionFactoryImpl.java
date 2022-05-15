/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.*;

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
public class openregspecs_smcubes_extra_extensionFactoryImpl extends EFactoryImpl implements openregspecs_smcubes_extra_extensionFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static openregspecs_smcubes_extra_extensionFactory init() {
		try {
			openregspecs_smcubes_extra_extensionFactory theopenregspecs_smcubes_extra_extensionFactory = (openregspecs_smcubes_extra_extensionFactory)EPackage.Registry.INSTANCE.getEFactory(openregspecs_smcubes_extra_extensionPackage.eNS_URI);
			if (theopenregspecs_smcubes_extra_extensionFactory != null) {
				return theopenregspecs_smcubes_extra_extensionFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new openregspecs_smcubes_extra_extensionFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public openregspecs_smcubes_extra_extensionFactoryImpl() {
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
			case openregspecs_smcubes_extra_extensionPackage.SMCUBES_EXTRA_MODEL: return createSmcubesExtraModel();
			case openregspecs_smcubes_extra_extensionPackage.COMBINATION_MODULE: return createCombinationModule();
			case openregspecs_smcubes_extra_extensionPackage.CUBE_MODULE: return createCubeModule();
			case openregspecs_smcubes_extra_extensionPackage.MAPPING_DEFINITION_MODULE: return createMappingDefinitionModule();
			case openregspecs_smcubes_extra_extensionPackage.CUBE_MAPPING_MODULE: return createCubeMappingModule();
			case openregspecs_smcubes_extra_extensionPackage.MEMBER_MAPPING_MODULE: return createMemberMappingModule();
			case openregspecs_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE: return createVariableMappingModule();
			case openregspecs_smcubes_extra_extensionPackage.AXIS_MODULE: return createAxisModule();
			case openregspecs_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE: return createAxisOrdinateModule();
			case openregspecs_smcubes_extra_extensionPackage.CELL_POSITION_MODULE: return createCellPositionModule();
			case openregspecs_smcubes_extra_extensionPackage.ORDINATE_ITEM_MODULE: return createOrdinateItemModule();
			case openregspecs_smcubes_extra_extensionPackage.REPORT_CELL: return createReportCell();
			case openregspecs_smcubes_extra_extensionPackage.REPORT_CELL_VALUE: return createReportCellValue();
			case openregspecs_smcubes_extra_extensionPackage.REPORT_TABLE_MODULE: return createReportTableModule();
			case openregspecs_smcubes_extra_extensionPackage.TABLE_CELL_MODULE: return createTableCellModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SmcubesExtraModel createSmcubesExtraModel() {
		SmcubesExtraModelImpl smcubesExtraModel = new SmcubesExtraModelImpl();
		return smcubesExtraModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CombinationModule createCombinationModule() {
		CombinationModuleImpl combinationModule = new CombinationModuleImpl();
		return combinationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubeModule createCubeModule() {
		CubeModuleImpl cubeModule = new CubeModuleImpl();
		return cubeModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MappingDefinitionModule createMappingDefinitionModule() {
		MappingDefinitionModuleImpl mappingDefinitionModule = new MappingDefinitionModuleImpl();
		return mappingDefinitionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubeMappingModule createCubeMappingModule() {
		CubeMappingModuleImpl cubeMappingModule = new CubeMappingModuleImpl();
		return cubeMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberMappingModule createMemberMappingModule() {
		MemberMappingModuleImpl memberMappingModule = new MemberMappingModuleImpl();
		return memberMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VariableMappingModule createVariableMappingModule() {
		VariableMappingModuleImpl variableMappingModule = new VariableMappingModuleImpl();
		return variableMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AxisModule createAxisModule() {
		AxisModuleImpl axisModule = new AxisModuleImpl();
		return axisModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AxisOrdinateModule createAxisOrdinateModule() {
		AxisOrdinateModuleImpl axisOrdinateModule = new AxisOrdinateModuleImpl();
		return axisOrdinateModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CellPositionModule createCellPositionModule() {
		CellPositionModuleImpl cellPositionModule = new CellPositionModuleImpl();
		return cellPositionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OrdinateItemModule createOrdinateItemModule() {
		OrdinateItemModuleImpl ordinateItemModule = new OrdinateItemModuleImpl();
		return ordinateItemModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportCell createReportCell() {
		ReportCellImpl reportCell = new ReportCellImpl();
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportCellValue createReportCellValue() {
		ReportCellValueImpl reportCellValue = new ReportCellValueImpl();
		return reportCellValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportTableModule createReportTableModule() {
		ReportTableModuleImpl reportTableModule = new ReportTableModuleImpl();
		return reportTableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TableCellModule createTableCellModule() {
		TableCellModuleImpl tableCellModule = new TableCellModuleImpl();
		return tableCellModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public openregspecs_smcubes_extra_extensionPackage getopenregspecs_smcubes_extra_extensionPackage() {
		return (openregspecs_smcubes_extra_extensionPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static openregspecs_smcubes_extra_extensionPackage getPackage() {
		return openregspecs_smcubes_extra_extensionPackage.eINSTANCE;
	}

} //openregspecs_smcubes_extra_extensionFactoryImpl
