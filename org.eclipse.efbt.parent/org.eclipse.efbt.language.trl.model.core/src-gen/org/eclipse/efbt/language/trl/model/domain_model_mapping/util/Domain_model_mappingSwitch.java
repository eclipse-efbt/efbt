/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping.util;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.*;

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
 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage
 * @generated
 */
public class Domain_model_mappingSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Domain_model_mappingPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Domain_model_mappingSwitch()
	{
		if (modelPackage == null)
		{
			modelPackage = Domain_model_mappingPackage.eINSTANCE;
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
	protected boolean isSwitchFor(EPackage ePackage)
	{
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
	protected T doSwitch(int classifierID, EObject theEObject)
	{
		switch (classifierID)
		{
			case Domain_model_mappingPackage.LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULE:
			{
				LeafModuleToVersionedSQLViewsModule leafModuleToVersionedSQLViewsModule = (LeafModuleToVersionedSQLViewsModule)theEObject;
				T result = caseLeafModuleToVersionedSQLViewsModule(leafModuleToVersionedSQLViewsModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULE:
			{
				LeafModuleToVersionedCubeSchemaModule leafModuleToVersionedCubeSchemaModule = (LeafModuleToVersionedCubeSchemaModule)theEObject;
				T result = caseLeafModuleToVersionedCubeSchemaModule(leafModuleToVersionedCubeSchemaModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE:
			{
				LeafModuleToReportCellViewModule leafModuleToReportCellViewModule = (LeafModuleToReportCellViewModule)theEObject;
				T result = caseLeafModuleToReportCellViewModule(leafModuleToReportCellViewModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS:
			{
				ScenarioToCodeConditions scenarioToCodeConditions = (ScenarioToCodeConditions)theEObject;
				T result = caseScenarioToCodeConditions(scenarioToCodeConditions);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.CODE_CONDITION:
			{
				CodeCondition codeCondition = (CodeCondition)theEObject;
				T result = caseCodeCondition(codeCondition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION:
			{
				ColumnSettingCondition columnSettingCondition = (ColumnSettingCondition)theEObject;
				T result = caseColumnSettingCondition(columnSettingCondition);
				if (result == null) result = caseCodeCondition(columnSettingCondition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.DATA_SET_FILTER_CONDITION:
			{
				DataSetFilterCondition dataSetFilterCondition = (DataSetFilterCondition)theEObject;
				T result = caseDataSetFilterCondition(dataSetFilterCondition);
				if (result == null) result = caseCodeCondition(dataSetFilterCondition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION:
			{
				UnionChoiceCondition unionChoiceCondition = (UnionChoiceCondition)theEObject;
				T result = caseUnionChoiceCondition(unionChoiceCondition);
				if (result == null) result = caseCodeCondition(unionChoiceCondition);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE:
			{
				DomainMappingModule domainMappingModule = (DomainMappingModule)theEObject;
				T result = caseDomainMappingModule(domainMappingModule);
				if (result == null) result = caseModule(domainMappingModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Leaf Module To Versioned SQL Views Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Leaf Module To Versioned SQL Views Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseLeafModuleToVersionedSQLViewsModule(LeafModuleToVersionedSQLViewsModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Leaf Module To Versioned Cube Schema Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Leaf Module To Versioned Cube Schema Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseLeafModuleToVersionedCubeSchemaModule(LeafModuleToVersionedCubeSchemaModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Leaf Module To Report Cell View Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Leaf Module To Report Cell View Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseLeafModuleToReportCellViewModule(LeafModuleToReportCellViewModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Scenario To Code Conditions</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Scenario To Code Conditions</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseScenarioToCodeConditions(ScenarioToCodeConditions object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Code Condition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Code Condition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCodeCondition(CodeCondition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Column Setting Condition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Column Setting Condition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseColumnSettingCondition(ColumnSettingCondition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Data Set Filter Condition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Data Set Filter Condition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDataSetFilterCondition(DataSetFilterCondition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Union Choice Condition</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Union Choice Condition</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseUnionChoiceCondition(UnionChoiceCondition object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Domain Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Domain Mapping Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDomainMappingModule(DomainMappingModule object)
	{
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
	public T caseModule(org.eclipse.efbt.common.model.module_management.Module object)
	{
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
	public T defaultCase(EObject object)
	{
		return null;
	}

} //Domain_model_mappingSwitch
