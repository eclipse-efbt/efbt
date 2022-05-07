/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.util;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionPackage
 * @generated
 */
public class Cocalimo_smcubes_extra_extensionSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Cocalimo_smcubes_extra_extensionPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cocalimo_smcubes_extra_extensionSwitch() {
		if (modelPackage == null) {
			modelPackage = Cocalimo_smcubes_extra_extensionPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case Cocalimo_smcubes_extra_extensionPackage.SMCUBES_EXTRA_MODEL: {
				SmcubesExtraModel smcubesExtraModel = (SmcubesExtraModel)theEObject;
				T result = caseSmcubesExtraModel(smcubesExtraModel);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.COMBINATION_MODULE: {
				CombinationModule combinationModule = (CombinationModule)theEObject;
				T result = caseCombinationModule(combinationModule);
				if (result == null) result = caseModule(combinationModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.CUBE_MODULE: {
				CubeModule cubeModule = (CubeModule)theEObject;
				T result = caseCubeModule(cubeModule);
				if (result == null) result = caseModule(cubeModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.MAPPING_DEFINITION_MODULE: {
				MappingDefinitionModule mappingDefinitionModule = (MappingDefinitionModule)theEObject;
				T result = caseMappingDefinitionModule(mappingDefinitionModule);
				if (result == null) result = caseModule(mappingDefinitionModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.CUBE_MAPPING_MODULE: {
				CubeMappingModule cubeMappingModule = (CubeMappingModule)theEObject;
				T result = caseCubeMappingModule(cubeMappingModule);
				if (result == null) result = caseModule(cubeMappingModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.MEMBER_MAPPING_MODULE: {
				MemberMappingModule memberMappingModule = (MemberMappingModule)theEObject;
				T result = caseMemberMappingModule(memberMappingModule);
				if (result == null) result = caseModule(memberMappingModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.VARIABLE_MAPPING_MODULE: {
				VariableMappingModule variableMappingModule = (VariableMappingModule)theEObject;
				T result = caseVariableMappingModule(variableMappingModule);
				if (result == null) result = caseModule(variableMappingModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.AXIS_MODULE: {
				AxisModule axisModule = (AxisModule)theEObject;
				T result = caseAxisModule(axisModule);
				if (result == null) result = caseModule(axisModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE: {
				AxisOrdinateModule axisOrdinateModule = (AxisOrdinateModule)theEObject;
				T result = caseAxisOrdinateModule(axisOrdinateModule);
				if (result == null) result = caseModule(axisOrdinateModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.CELL_POSITION_MODULE: {
				CellPositionModule cellPositionModule = (CellPositionModule)theEObject;
				T result = caseCellPositionModule(cellPositionModule);
				if (result == null) result = caseModule(cellPositionModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.ORDINATE_ITEM_MODULE: {
				OrdinateItemModule ordinateItemModule = (OrdinateItemModule)theEObject;
				T result = caseOrdinateItemModule(ordinateItemModule);
				if (result == null) result = caseModule(ordinateItemModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.REPORT_CELL: {
				ReportCell reportCell = (ReportCell)theEObject;
				T result = caseReportCell(reportCell);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.REPORT_CELL_VALUE: {
				ReportCellValue reportCellValue = (ReportCellValue)theEObject;
				T result = caseReportCellValue(reportCellValue);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.REPORT_TABLE_MODULE: {
				ReportTableModule reportTableModule = (ReportTableModule)theEObject;
				T result = caseReportTableModule(reportTableModule);
				if (result == null) result = caseModule(reportTableModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Cocalimo_smcubes_extra_extensionPackage.TABLE_CELL_MODULE: {
				TableCellModule tableCellModule = (TableCellModule)theEObject;
				T result = caseTableCellModule(tableCellModule);
				if (result == null) result = caseModule(tableCellModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Smcubes Extra Model</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Smcubes Extra Model</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSmcubesExtraModel(SmcubesExtraModel object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Combination Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Combination Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCombinationModule(CombinationModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cube Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cube Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCubeModule(CubeModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Mapping Definition Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Mapping Definition Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMappingDefinitionModule(MappingDefinitionModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cube Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cube Mapping Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCubeMappingModule(CubeMappingModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Member Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Member Mapping Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMemberMappingModule(MemberMappingModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Variable Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Variable Mapping Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVariableMappingModule(VariableMappingModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Axis Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Axis Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAxisModule(AxisModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Axis Ordinate Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Axis Ordinate Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAxisOrdinateModule(AxisOrdinateModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cell Position Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cell Position Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCellPositionModule(CellPositionModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Ordinate Item Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Ordinate Item Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOrdinateItemModule(OrdinateItemModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Cell</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Cell</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportCell(ReportCell object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Cell Value</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Cell Value</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportCellValue(ReportCellValue object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Table Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Table Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportTableModule(ReportTableModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Table Cell Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Table Cell Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTableCellModule(TableCellModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //Cocalimo_smcubes_extra_extensionSwitch
