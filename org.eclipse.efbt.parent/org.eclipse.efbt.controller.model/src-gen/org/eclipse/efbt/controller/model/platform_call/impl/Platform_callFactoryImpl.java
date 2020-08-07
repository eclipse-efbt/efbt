/**
 */
package org.eclipse.efbt.controller.model.platform_call.impl;

import org.eclipse.efbt.controller.model.platform_call.*;

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
public class Platform_callFactoryImpl extends EFactoryImpl implements Platform_callFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Platform_callFactory init()
	{
		try
		{
			Platform_callFactory thePlatform_callFactory = (Platform_callFactory)EPackage.Registry.INSTANCE.getEFactory(Platform_callPackage.eNS_URI);
			if (thePlatform_callFactory != null)
			{
				return thePlatform_callFactory;
			}
		}
		catch (Exception exception)
		{
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Platform_callFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Platform_callFactoryImpl()
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
		switch (eClass.getClassifierID())
		{
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS: return createCompareAttributeLineageModels();
			case Platform_callPackage.IS_NOT_SUBSET: return createIsNotSubset();
			case Platform_callPackage.IS_SUBSET: return createIsSubset();
			case Platform_callPackage.SUBSET_BOOLEAN: return createSubsetBoolean();
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL: return createExecuteAttributeLineageModel();
			case Platform_callPackage.PLATFORM_CALL: return createPlatformCall();
			case Platform_callPackage.PLATFORM_CALL_MODULE: return createPlatformCallModule();
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL: return createGetAttributeLineageModel();
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL: return createGetAttributeLineageForOneReportCell();
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
	public CompareAttributeLineageModels createCompareAttributeLineageModels()
	{
		CompareAttributeLineageModelsImpl compareAttributeLineageModels = new CompareAttributeLineageModelsImpl();
		return compareAttributeLineageModels;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public IsNotSubset createIsNotSubset()
	{
		IsNotSubsetImpl isNotSubset = new IsNotSubsetImpl();
		return isNotSubset;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public IsSubset createIsSubset()
	{
		IsSubsetImpl isSubset = new IsSubsetImpl();
		return isSubset;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SubsetBoolean createSubsetBoolean()
	{
		SubsetBooleanImpl subsetBoolean = new SubsetBooleanImpl();
		return subsetBoolean;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ExecuteAttributeLineageModel createExecuteAttributeLineageModel()
	{
		ExecuteAttributeLineageModelImpl executeAttributeLineageModel = new ExecuteAttributeLineageModelImpl();
		return executeAttributeLineageModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public PlatformCall createPlatformCall()
	{
		PlatformCallImpl platformCall = new PlatformCallImpl();
		return platformCall;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public PlatformCallModule createPlatformCallModule()
	{
		PlatformCallModuleImpl platformCallModule = new PlatformCallModuleImpl();
		return platformCallModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GetAttributeLineageModel createGetAttributeLineageModel()
	{
		GetAttributeLineageModelImpl getAttributeLineageModel = new GetAttributeLineageModelImpl();
		return getAttributeLineageModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GetAttributeLineageForOneReportCell createGetAttributeLineageForOneReportCell()
	{
		GetAttributeLineageForOneReportCellImpl getAttributeLineageForOneReportCell = new GetAttributeLineageForOneReportCellImpl();
		return getAttributeLineageForOneReportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Platform_callPackage getPlatform_callPackage()
	{
		return (Platform_callPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Platform_callPackage getPackage()
	{
		return Platform_callPackage.eINSTANCE;
	}

} //Platform_callFactoryImpl
