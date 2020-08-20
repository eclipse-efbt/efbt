/**
 */
package org.eclipse.efbt.language.trl.model.trl_report_cell_views.util;

import org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.*;

import org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage
 * @generated
 */
public class Trl_report_cell_viewsAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Trl_report_cell_viewsPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Trl_report_cell_viewsAdapterFactory()
	{
		if (modelPackage == null)
		{
			modelPackage = Trl_report_cell_viewsPackage.eINSTANCE;
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
	protected Trl_report_cell_viewsSwitch<Adapter> modelSwitch =
		new Trl_report_cell_viewsSwitch<Adapter>()
		{
			@Override
			public Adapter caseReportCellView(ReportCellView object)
			{
				return createReportCellViewAdapter();
			}
			@Override
			public Adapter caseReportCellViewModule(ReportCellViewModule object)
			{
				return createReportCellViewModuleAdapter();
			}
			@Override
			public Adapter caseDataSetTransformation(DataSetTransformation object)
			{
				return createDataSetTransformationAdapter();
			}
			@Override
			public Adapter caseSQLView(SQLView object)
			{
				return createSQLViewAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.common.model.module_management.Module object)
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView <em>Report Cell View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView
	 * @generated
	 */
	public Adapter createReportCellViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule <em>Report Cell View Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule
	 * @generated
	 */
	public Adapter createReportCellViewModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation <em>Data Set Transformation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation
	 * @generated
	 */
	public Adapter createDataSetTransformationAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView <em>SQL View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView
	 * @generated
	 */
	public Adapter createSQLViewAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.common.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.common.model.module_management.Module
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

} //Trl_report_cell_viewsAdapterFactory
