/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.*;

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
public class Incremental_cubesFactoryImpl extends EFactoryImpl implements Incremental_cubesFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Incremental_cubesFactory init()
	{
		try {
			Incremental_cubesFactory theIncremental_cubesFactory = (Incremental_cubesFactory)EPackage.Registry.INSTANCE.getEFactory(Incremental_cubesPackage.eNS_URI);
			if (theIncremental_cubesFactory != null) {
				return theIncremental_cubesFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Incremental_cubesFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Incremental_cubesFactoryImpl()
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
			case Incremental_cubesPackage.BASE_DELTA_CUBE: return createBaseDeltaCube();
			case Incremental_cubesPackage.BASE_VIEW_CUBE: return createBaseViewCube();
			case Incremental_cubesPackage.DELTA_ACCUMULATION_CUBE: return createDeltaAccumulationCube();
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
	public BaseDeltaCube createBaseDeltaCube()
	{
		BaseDeltaCubeImpl baseDeltaCube = new BaseDeltaCubeImpl();
		return baseDeltaCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseViewCube createBaseViewCube()
	{
		BaseViewCubeImpl baseViewCube = new BaseViewCubeImpl();
		return baseViewCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DeltaAccumulationCube createDeltaAccumulationCube()
	{
		DeltaAccumulationCubeImpl deltaAccumulationCube = new DeltaAccumulationCubeImpl();
		return deltaAccumulationCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Incremental_cubesPackage getIncremental_cubesPackage()
	{
		return (Incremental_cubesPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Incremental_cubesPackage getPackage()
	{
		return Incremental_cubesPackage.eINSTANCE;
	}

} //Incremental_cubesFactoryImpl
