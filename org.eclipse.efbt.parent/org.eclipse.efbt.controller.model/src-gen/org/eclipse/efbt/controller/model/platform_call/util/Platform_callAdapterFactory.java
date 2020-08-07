/**
 */
package org.eclipse.efbt.controller.model.platform_call.util;

import org.eclipse.efbt.controller.model.platform_call.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage
 * @generated
 */
public class Platform_callAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Platform_callPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Platform_callAdapterFactory()
	{
		if (modelPackage == null)
		{
			modelPackage = Platform_callPackage.eINSTANCE;
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
	protected Platform_callSwitch<Adapter> modelSwitch =
		new Platform_callSwitch<Adapter>()
		{
			@Override
			public Adapter caseCompareAttributeLineageModels(CompareAttributeLineageModels object)
			{
				return createCompareAttributeLineageModelsAdapter();
			}
			@Override
			public Adapter caseIsNotSubset(IsNotSubset object)
			{
				return createIsNotSubsetAdapter();
			}
			@Override
			public Adapter caseIsSubset(IsSubset object)
			{
				return createIsSubsetAdapter();
			}
			@Override
			public Adapter caseSubsetBoolean(SubsetBoolean object)
			{
				return createSubsetBooleanAdapter();
			}
			@Override
			public Adapter caseExecuteAttributeLineageModel(ExecuteAttributeLineageModel object)
			{
				return createExecuteAttributeLineageModelAdapter();
			}
			@Override
			public Adapter casePlatformCall(PlatformCall object)
			{
				return createPlatformCallAdapter();
			}
			@Override
			public Adapter casePlatformCallModule(PlatformCallModule object)
			{
				return createPlatformCallModuleAdapter();
			}
			@Override
			public Adapter caseGetAttributeLineageModel(GetAttributeLineageModel object)
			{
				return createGetAttributeLineageModelAdapter();
			}
			@Override
			public Adapter caseGetAttributeLineageForOneReportCell(GetAttributeLineageForOneReportCell object)
			{
				return createGetAttributeLineageForOneReportCellAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.aorta.module_management.Module object)
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels <em>Compare Attribute Lineage Models</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels
	 * @generated
	 */
	public Adapter createCompareAttributeLineageModelsAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.IsNotSubset <em>Is Not Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.IsNotSubset
	 * @generated
	 */
	public Adapter createIsNotSubsetAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.IsSubset <em>Is Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.IsSubset
	 * @generated
	 */
	public Adapter createIsSubsetAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.SubsetBoolean <em>Subset Boolean</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.SubsetBoolean
	 * @generated
	 */
	public Adapter createSubsetBooleanAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.ExecuteAttributeLineageModel <em>Execute Attribute Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.ExecuteAttributeLineageModel
	 * @generated
	 */
	public Adapter createExecuteAttributeLineageModelAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.PlatformCall <em>Platform Call</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.PlatformCall
	 * @generated
	 */
	public Adapter createPlatformCallAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.PlatformCallModule <em>Platform Call Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.PlatformCallModule
	 * @generated
	 */
	public Adapter createPlatformCallModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel <em>Get Attribute Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel
	 * @generated
	 */
	public Adapter createGetAttributeLineageModelAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell <em>Get Attribute Lineage For One Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell
	 * @generated
	 */
	public Adapter createGetAttributeLineageForOneReportCellAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.aorta.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.aorta.module_management.Module
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

} //Platform_callAdapterFactory
