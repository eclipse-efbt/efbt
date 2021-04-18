/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.*;

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
public class Base_column_structured_dataFactoryImpl extends EFactoryImpl implements Base_column_structured_dataFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Base_column_structured_dataFactory init()
	{
		try {
			Base_column_structured_dataFactory theBase_column_structured_dataFactory = (Base_column_structured_dataFactory)EPackage.Registry.INSTANCE.getEFactory(Base_column_structured_dataPackage.eNS_URI);
			if (theBase_column_structured_dataFactory != null) {
				return theBase_column_structured_dataFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Base_column_structured_dataFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Base_column_structured_dataFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID()) {
			case Base_column_structured_dataPackage.BASE_CELL_WITH_ENUMERATED_VALUE: return createBaseCellWithEnumeratedValue();
			case Base_column_structured_dataPackage.BASE_CELL_WITH_VALUE: return createBaseCellWithValue();
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA: return createBaseColumnStructuredData();
			case Base_column_structured_dataPackage.BASE_ROW_DATA: return createBaseRowData();
			case Base_column_structured_dataPackage.INPUT_DATA: return createInputData();
			case Base_column_structured_dataPackage.INPUT_DATA_MODULE: return createInputDataModule();
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
	public BaseCellWithEnumeratedValue createBaseCellWithEnumeratedValue()
	{
		BaseCellWithEnumeratedValueImpl baseCellWithEnumeratedValue = new BaseCellWithEnumeratedValueImpl();
		return baseCellWithEnumeratedValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseCellWithValue createBaseCellWithValue()
	{
		BaseCellWithValueImpl baseCellWithValue = new BaseCellWithValueImpl();
		return baseCellWithValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseColumnStructuredData createBaseColumnStructuredData()
	{
		BaseColumnStructuredDataImpl baseColumnStructuredData = new BaseColumnStructuredDataImpl();
		return baseColumnStructuredData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseRowData createBaseRowData()
	{
		BaseRowDataImpl baseRowData = new BaseRowDataImpl();
		return baseRowData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public InputData createInputData()
	{
		InputDataImpl inputData = new InputDataImpl();
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public InputDataModule createInputDataModule()
	{
		InputDataModuleImpl inputDataModule = new InputDataModuleImpl();
		return inputDataModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Base_column_structured_dataPackage getBase_column_structured_dataPackage()
	{
		return (Base_column_structured_dataPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Base_column_structured_dataPackage getPackage()
	{
		return Base_column_structured_dataPackage.eINSTANCE;
	}

} //Base_column_structured_dataFactoryImpl
