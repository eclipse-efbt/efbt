/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.util;

import org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Column_structured_dataPackage
 * @generated
 */
public class Column_structured_dataAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Column_structured_dataPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Column_structured_dataAdapterFactory()
	{
		if (modelPackage == null)
		{
			modelPackage = Column_structured_dataPackage.eINSTANCE;
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
	public boolean isFactoryForType(Object object)
	{
		if (object == modelPackage)
		{
			return true;
		}
		if (object instanceof EObject)
		{
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
	protected Column_structured_dataSwitch<Adapter> modelSwitch =
		new Column_structured_dataSwitch<Adapter>()
		{
			@Override
			public Adapter caseCell(Cell object)
			{
				return createCellAdapter();
			}
			@Override
			public Adapter caseColumnStructuredData(ColumnStructuredData object)
			{
				return createColumnStructuredDataAdapter();
			}
			@Override
			public Adapter caseColumnStructuredDataModule(ColumnStructuredDataModule object)
			{
				return createColumnStructuredDataModuleAdapter();
			}
			@Override
			public Adapter caseRowData(RowData object)
			{
				return createRowDataAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocamo.core.model.module_management.Module object)
			{
				return createModuleAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object)
			{
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
	public Adapter createAdapter(Notifier target)
	{
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Cell <em>Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Cell
	 * @generated
	 */
	public Adapter createCellAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.ColumnStructuredData <em>Column Structured Data</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.ColumnStructuredData
	 * @generated
	 */
	public Adapter createColumnStructuredDataAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.ColumnStructuredDataModule <em>Column Structured Data Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.ColumnStructuredDataModule
	 * @generated
	 */
	public Adapter createColumnStructuredDataModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.RowData <em>Row Data</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.RowData
	 * @generated
	 */
	public Adapter createRowDataAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter()
	{
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
	public Adapter createEObjectAdapter()
	{
		return null;
	}

} //Column_structured_dataAdapterFactory
