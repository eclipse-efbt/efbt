/**
 */
package org.eclipse.efbt.language.trl.model.transformation.impl;

import org.eclipse.efbt.language.trl.model.transformation.*;

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
public class TransformationFactoryImpl extends EFactoryImpl implements TransformationFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static TransformationFactory init()
	{
		try
		{
			TransformationFactory theTransformationFactory = (TransformationFactory)EPackage.Registry.INSTANCE.getEFactory(TransformationPackage.eNS_URI);
			if (theTransformationFactory != null)
			{
				return theTransformationFactory;
			}
		}
		catch (Exception exception)
		{
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TransformationFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TransformationFactoryImpl()
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
			case TransformationPackage.DATA_SET_TRANSFORMATION: return createDataSetTransformation();
			case TransformationPackage.RELEASE: return createRelease();
			case TransformationPackage.VERSIONED_COMPONENTS_SET: return createVersionedComponentsSet();
			case TransformationPackage.VERSIONED_CUBE_SCHEMA_MODULE: return createVersionedCubeSchemaModule();
			case TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC: return createVersionedFunctionalModuleLogic();
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
	public DataSetTransformation createDataSetTransformation()
	{
		DataSetTransformationImpl dataSetTransformation = new DataSetTransformationImpl();
		return dataSetTransformation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Release createRelease()
	{
		ReleaseImpl release = new ReleaseImpl();
		return release;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VersionedComponentsSet createVersionedComponentsSet()
	{
		VersionedComponentsSetImpl versionedComponentsSet = new VersionedComponentsSetImpl();
		return versionedComponentsSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VersionedCubeSchemaModule createVersionedCubeSchemaModule()
	{
		VersionedCubeSchemaModuleImpl versionedCubeSchemaModule = new VersionedCubeSchemaModuleImpl();
		return versionedCubeSchemaModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VersionedFunctionalModuleLogic createVersionedFunctionalModuleLogic()
	{
		VersionedFunctionalModuleLogicImpl versionedFunctionalModuleLogic = new VersionedFunctionalModuleLogicImpl();
		return versionedFunctionalModuleLogic;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TransformationPackage getTransformationPackage()
	{
		return (TransformationPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static TransformationPackage getPackage()
	{
		return TransformationPackage.eINSTANCE;
	}

} //TransformationFactoryImpl
