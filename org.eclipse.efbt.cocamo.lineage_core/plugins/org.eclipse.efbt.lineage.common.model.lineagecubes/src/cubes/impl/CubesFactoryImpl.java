/**
 */
package cubes.impl;

import cubes.*;

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
public class CubesFactoryImpl extends EFactoryImpl implements CubesFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static CubesFactory init()
	{
		try
		{
			CubesFactory theCubesFactory = (CubesFactory)EPackage.Registry.INSTANCE.getEFactory(CubesPackage.eNS_URI);
			if (theCubesFactory != null)
			{
				return theCubesFactory;
			}
		}
		catch (Exception exception)
		{
			EcorePlugin.INSTANCE.log(exception);
		}
		return new CubesFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubesFactoryImpl()
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
			case CubesPackage.BASE_CUBE: return createBaseCube();
			case CubesPackage.DERIVED_CUBE: return createDerivedCube();
			case CubesPackage.FREE_BIRD_TOOLS_CUBE: return createFreeBirdToolsCube();
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE: return createFreeBirdToolsCubeModule();
			case CubesPackage.TARGET_CUBE: return createTargetCube();
			case CubesPackage.BASE_DELTA_CUBE: return createBaseDeltaCube();
			case CubesPackage.BASE_VIEW_CUBE: return createBaseViewCube();
			case CubesPackage.DELTA_ACCUMULATION_CUBE: return createDeltaAccumulationCube();
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
	public BaseCube createBaseCube()
	{
		BaseCubeImpl baseCube = new BaseCubeImpl();
		return baseCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DerivedCube createDerivedCube()
	{
		DerivedCubeImpl derivedCube = new DerivedCubeImpl();
		return derivedCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube createFreeBirdToolsCube()
	{
		FreeBirdToolsCubeImpl freeBirdToolsCube = new FreeBirdToolsCubeImpl();
		return freeBirdToolsCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCubeModule createFreeBirdToolsCubeModule()
	{
		FreeBirdToolsCubeModuleImpl freeBirdToolsCubeModule = new FreeBirdToolsCubeModuleImpl();
		return freeBirdToolsCubeModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TargetCube createTargetCube()
	{
		TargetCubeImpl targetCube = new TargetCubeImpl();
		return targetCube;
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
	public CubesPackage getCubesPackage()
	{
		return (CubesPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static CubesPackage getPackage()
	{
		return CubesPackage.eINSTANCE;
	}

} //CubesFactoryImpl
