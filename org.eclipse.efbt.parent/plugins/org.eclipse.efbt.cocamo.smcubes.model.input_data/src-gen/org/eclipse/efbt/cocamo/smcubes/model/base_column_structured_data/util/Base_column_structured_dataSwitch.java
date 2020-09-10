/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.util;

import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.*;

import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Cell;
import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.ColumnStructuredData;
import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.RowData;

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
 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage
 * @generated
 */
public class Base_column_structured_dataSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Base_column_structured_dataPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Base_column_structured_dataSwitch()
	{
		if (modelPackage == null)
		{
			modelPackage = Base_column_structured_dataPackage.eINSTANCE;
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
			case Base_column_structured_dataPackage.BASE_CELL:
			{
				BaseCell baseCell = (BaseCell)theEObject;
				T result = caseBaseCell(baseCell);
				if (result == null) result = caseCell(baseCell);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Base_column_structured_dataPackage.BASE_CELL_WITH_ENUMERATED_VALUE:
			{
				BaseCellWithEnumeratedValue baseCellWithEnumeratedValue = (BaseCellWithEnumeratedValue)theEObject;
				T result = caseBaseCellWithEnumeratedValue(baseCellWithEnumeratedValue);
				if (result == null) result = caseBaseCell(baseCellWithEnumeratedValue);
				if (result == null) result = caseCell(baseCellWithEnumeratedValue);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Base_column_structured_dataPackage.BASE_CELL_WITH_VALUE:
			{
				BaseCellWithValue baseCellWithValue = (BaseCellWithValue)theEObject;
				T result = caseBaseCellWithValue(baseCellWithValue);
				if (result == null) result = caseBaseCell(baseCellWithValue);
				if (result == null) result = caseCell(baseCellWithValue);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA:
			{
				BaseColumnStructuredData baseColumnStructuredData = (BaseColumnStructuredData)theEObject;
				T result = caseBaseColumnStructuredData(baseColumnStructuredData);
				if (result == null) result = caseColumnStructuredData(baseColumnStructuredData);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Base_column_structured_dataPackage.BASE_ROW_DATA:
			{
				BaseRowData baseRowData = (BaseRowData)theEObject;
				T result = caseBaseRowData(baseRowData);
				if (result == null) result = caseRowData(baseRowData);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Base_column_structured_dataPackage.INPUT_DATA:
			{
				InputData inputData = (InputData)theEObject;
				T result = caseInputData(inputData);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Base_column_structured_dataPackage.INPUT_DATA_MODULE:
			{
				InputDataModule inputDataModule = (InputDataModule)theEObject;
				T result = caseInputDataModule(inputDataModule);
				if (result == null) result = caseModule(inputDataModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Cell</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Cell</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseCell(BaseCell object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Cell With Enumerated Value</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Cell With Enumerated Value</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseCellWithEnumeratedValue(BaseCellWithEnumeratedValue object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Cell With Value</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Cell With Value</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseCellWithValue(BaseCellWithValue object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Column Structured Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Column Structured Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseColumnStructuredData(BaseColumnStructuredData object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Row Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Row Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseRowData(BaseRowData object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Input Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Input Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseInputData(InputData object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Input Data Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Input Data Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseInputDataModule(InputDataModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cell</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cell</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCell(Cell object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Column Structured Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Column Structured Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseColumnStructuredData(ColumnStructuredData object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Row Data</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Row Data</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRowData(RowData object)
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
	public T caseModule(org.eclipse.efbt.cocamo.core.model.module_management.Module object)
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

} //Base_column_structured_dataSwitch
