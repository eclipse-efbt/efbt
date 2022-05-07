/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.util;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionPackage
 * @generated
 */
public class Cocalimo_smcubes_extra_extensionAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Cocalimo_smcubes_extra_extensionPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cocalimo_smcubes_extra_extensionAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Cocalimo_smcubes_extra_extensionPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Cocalimo_smcubes_extra_extensionSwitch<Adapter> modelSwitch =
		new Cocalimo_smcubes_extra_extensionSwitch<Adapter>() {
			@Override
			public Adapter caseSmcubesExtraModel(SmcubesExtraModel object) {
				return createSmcubesExtraModelAdapter();
			}
			@Override
			public Adapter caseCombinationModule(CombinationModule object) {
				return createCombinationModuleAdapter();
			}
			@Override
			public Adapter caseCubeModule(CubeModule object) {
				return createCubeModuleAdapter();
			}
			@Override
			public Adapter caseMappingDefinitionModule(MappingDefinitionModule object) {
				return createMappingDefinitionModuleAdapter();
			}
			@Override
			public Adapter caseCubeMappingModule(CubeMappingModule object) {
				return createCubeMappingModuleAdapter();
			}
			@Override
			public Adapter caseMemberMappingModule(MemberMappingModule object) {
				return createMemberMappingModuleAdapter();
			}
			@Override
			public Adapter caseVariableMappingModule(VariableMappingModule object) {
				return createVariableMappingModuleAdapter();
			}
			@Override
			public Adapter caseAxisModule(AxisModule object) {
				return createAxisModuleAdapter();
			}
			@Override
			public Adapter caseAxisOrdinateModule(AxisOrdinateModule object) {
				return createAxisOrdinateModuleAdapter();
			}
			@Override
			public Adapter caseCellPositionModule(CellPositionModule object) {
				return createCellPositionModuleAdapter();
			}
			@Override
			public Adapter caseOrdinateItemModule(OrdinateItemModule object) {
				return createOrdinateItemModuleAdapter();
			}
			@Override
			public Adapter caseReportCell(ReportCell object) {
				return createReportCellAdapter();
			}
			@Override
			public Adapter caseReportCellValue(ReportCellValue object) {
				return createReportCellValueAdapter();
			}
			@Override
			public Adapter caseReportTableModule(ReportTableModule object) {
				return createReportTableModuleAdapter();
			}
			@Override
			public Adapter caseTableCellModule(TableCellModule object) {
				return createTableCellModuleAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.SmcubesExtraModel <em>Smcubes Extra Model</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.SmcubesExtraModel
	 * @generated
	 */
	public Adapter createSmcubesExtraModelAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule <em>Combination Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule
	 * @generated
	 */
	public Adapter createCombinationModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule <em>Cube Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule
	 * @generated
	 */
	public Adapter createCubeModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MappingDefinitionModule <em>Mapping Definition Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MappingDefinitionModule
	 * @generated
	 */
	public Adapter createMappingDefinitionModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeMappingModule <em>Cube Mapping Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeMappingModule
	 * @generated
	 */
	public Adapter createCubeMappingModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MemberMappingModule <em>Member Mapping Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MemberMappingModule
	 * @generated
	 */
	public Adapter createMemberMappingModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.VariableMappingModule <em>Variable Mapping Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.VariableMappingModule
	 * @generated
	 */
	public Adapter createVariableMappingModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisModule <em>Axis Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisModule
	 * @generated
	 */
	public Adapter createAxisModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisOrdinateModule <em>Axis Ordinate Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisOrdinateModule
	 * @generated
	 */
	public Adapter createAxisOrdinateModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CellPositionModule <em>Cell Position Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CellPositionModule
	 * @generated
	 */
	public Adapter createCellPositionModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.OrdinateItemModule <em>Ordinate Item Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.OrdinateItemModule
	 * @generated
	 */
	public Adapter createOrdinateItemModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell <em>Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell
	 * @generated
	 */
	public Adapter createReportCellAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue <em>Report Cell Value</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue
	 * @generated
	 */
	public Adapter createReportCellValueAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportTableModule <em>Report Table Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportTableModule
	 * @generated
	 */
	public Adapter createReportTableModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.TableCellModule <em>Table Cell Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.TableCellModule
	 * @generated
	 */
	public Adapter createTableCellModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //Cocalimo_smcubes_extra_extensionAdapterFactory
