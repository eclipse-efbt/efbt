/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.*;

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
public class Column_structured_dataFactoryImpl extends EFactoryImpl implements Column_structured_dataFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Column_structured_dataFactory init()
	{
		try {
			Column_structured_dataFactory theColumn_structured_dataFactory = (Column_structured_dataFactory)EPackage.Registry.INSTANCE.getEFactory(Column_structured_dataPackage.eNS_URI);
			if (theColumn_structured_dataFactory != null) {
				return theColumn_structured_dataFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Column_structured_dataFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Column_structured_dataFactoryImpl()
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
			case Column_structured_dataPackage.CELL: return createCell();
			case Column_structured_dataPackage.COLUMN_STRUCTURED_DATA: return createColumnStructuredData();
			case Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_MODULE: return createColumnStructuredDataModule();
			case Column_structured_dataPackage.ROW_DATA: return createRowData();
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
	public Cell createCell()
	{
		CellImpl cell = new CellImpl();
		return cell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ColumnStructuredData createColumnStructuredData()
	{
		ColumnStructuredDataImpl columnStructuredData = new ColumnStructuredDataImpl();
		return columnStructuredData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ColumnStructuredDataModule createColumnStructuredDataModule()
	{
		ColumnStructuredDataModuleImpl columnStructuredDataModule = new ColumnStructuredDataModuleImpl();
		return columnStructuredDataModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RowData createRowData()
	{
		RowDataImpl rowData = new RowDataImpl();
		return rowData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Column_structured_dataPackage getColumn_structured_dataPackage()
	{
		return (Column_structured_dataPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Column_structured_dataPackage getPackage()
	{
		return Column_structured_dataPackage.eINSTANCE;
	}

} //Column_structured_dataFactoryImpl
