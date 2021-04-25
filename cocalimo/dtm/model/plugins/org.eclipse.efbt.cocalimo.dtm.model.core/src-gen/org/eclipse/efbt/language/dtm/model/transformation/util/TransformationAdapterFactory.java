/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.util;

import org.eclipse.efbt.language.dtm.model.transformation.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage
 * @generated
 */
public class TransformationAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static TransformationPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TransformationAdapterFactory()
	{
		if (modelPackage == null) {
			modelPackage = TransformationPackage.eINSTANCE;
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
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
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
	protected TransformationSwitch<Adapter> modelSwitch =
		new TransformationSwitch<Adapter>() {
			@Override
			public Adapter caseDataSetTransformation(DataSetTransformation object) {
				return createDataSetTransformationAdapter();
			}
			@Override
			public Adapter caseRelease(Release object) {
				return createReleaseAdapter();
			}
			@Override
			public Adapter caseVersionedComponentsSet(VersionedComponentsSet object) {
				return createVersionedComponentsSetAdapter();
			}
			@Override
			public Adapter caseVersionedCubeSchemaModule(VersionedCubeSchemaModule object) {
				return createVersionedCubeSchemaModuleAdapter();
			}
			@Override
			public Adapter caseVersionedFunctionalModuleLogic(VersionedFunctionalModuleLogic object) {
				return createVersionedFunctionalModuleLogicAdapter();
			}
			@Override
			public Adapter casedtmExecutableLogic(dtmExecutableLogic object) {
				return createdtmExecutableLogicAdapter();
			}
			@Override
			public Adapter casedtmProgram(dtmProgram object) {
				return createdtmProgramAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation <em>Data Set Transformation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation
	 * @generated
	 */
	public Adapter createDataSetTransformationAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.Release <em>Release</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.Release
	 * @generated
	 */
	public Adapter createReleaseAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet <em>Versioned Components Set</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet
	 * @generated
	 */
	public Adapter createVersionedComponentsSetAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule <em>Versioned Cube Schema Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule
	 * @generated
	 */
	public Adapter createVersionedCubeSchemaModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic <em>Versioned Functional Module Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic
	 * @generated
	 */
	public Adapter createVersionedFunctionalModuleLogicAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic <em>dtm Executable Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic
	 * @generated
	 */
	public Adapter createdtmExecutableLogicAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram <em>dtm Program</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.dtmProgram
	 * @generated
	 */
	public Adapter createdtmProgramAdapter()
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

} //TransformationAdapterFactory
