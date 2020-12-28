/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.reports.util;

import org.eclipse.efbt.cocamo.smcubes.model.reports.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.ReportsPackage
 * @generated
 */
public class ReportsAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static ReportsPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportsAdapterFactory()
	{
		if (modelPackage == null)
		{
			modelPackage = ReportsPackage.eINSTANCE;
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
	protected ReportsSwitch<Adapter> modelSwitch =
		new ReportsSwitch<Adapter>()
		{
			@Override
			public Adapter caseAxisModule(AxisModule object)
			{
				return createAxisModuleAdapter();
			}
			@Override
			public Adapter caseAxisOrdinateModule(AxisOrdinateModule object)
			{
				return createAxisOrdinateModuleAdapter();
			}
			@Override
			public Adapter caseCellPositionModule(CellPositionModule object)
			{
				return createCellPositionModuleAdapter();
			}
			@Override
			public Adapter caseOrdinateItemModule(OrdinateItemModule object)
			{
				return createOrdinateItemModuleAdapter();
			}
			@Override
			public Adapter casePopulatedReport(PopulatedReport object)
			{
				return createPopulatedReportAdapter();
			}
			@Override
			public Adapter caseReportCell(ReportCell object)
			{
				return createReportCellAdapter();
			}
			@Override
			public Adapter caseReportCellValue(ReportCellValue object)
			{
				return createReportCellValueAdapter();
			}
			@Override
			public Adapter caseReportTableModule(ReportTableModule object)
			{
				return createReportTableModuleAdapter();
			}
			@Override
			public Adapter caseTableCellModule(TableCellModule object)
			{
				return createTableCellModuleAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object)
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.AxisModule <em>Axis Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.AxisModule
	 * @generated
	 */
	public Adapter createAxisModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.AxisOrdinateModule <em>Axis Ordinate Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.AxisOrdinateModule
	 * @generated
	 */
	public Adapter createAxisOrdinateModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.CellPositionModule <em>Cell Position Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.CellPositionModule
	 * @generated
	 */
	public Adapter createCellPositionModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.OrdinateItemModule <em>Ordinate Item Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.OrdinateItemModule
	 * @generated
	 */
	public Adapter createOrdinateItemModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.PopulatedReport <em>Populated Report</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.PopulatedReport
	 * @generated
	 */
	public Adapter createPopulatedReportAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.ReportCell <em>Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.ReportCell
	 * @generated
	 */
	public Adapter createReportCellAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.ReportCellValue <em>Report Cell Value</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.ReportCellValue
	 * @generated
	 */
	public Adapter createReportCellValueAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.ReportTableModule <em>Report Table Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.ReportTableModule
	 * @generated
	 */
	public Adapter createReportTableModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocamo.smcubes.model.reports.TableCellModule <em>Table Cell Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.reports.TableCellModule
	 * @generated
	 */
	public Adapter createTableCellModuleAdapter()
	{
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

} //ReportsAdapterFactory
