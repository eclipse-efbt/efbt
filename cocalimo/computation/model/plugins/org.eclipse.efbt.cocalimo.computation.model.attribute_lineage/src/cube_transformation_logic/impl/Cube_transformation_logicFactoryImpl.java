/**
 */
package cube_transformation_logic.impl;

import cube_transformation_logic.*;

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
public class Cube_transformation_logicFactoryImpl extends EFactoryImpl implements Cube_transformation_logicFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Cube_transformation_logicFactory init() {
		try {
			Cube_transformation_logicFactory theCube_transformation_logicFactory = (Cube_transformation_logicFactory)EPackage.Registry.INSTANCE.getEFactory(Cube_transformation_logicPackage.eNS_URI);
			if (theCube_transformation_logicFactory != null) {
				return theCube_transformation_logicFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Cube_transformation_logicFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cube_transformation_logicFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Cube_transformation_logicPackage.CUBE_TRANSFORMATION_LOGIC: return createCubeTransformationLogic();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubeTransformationLogic createCubeTransformationLogic() {
		CubeTransformationLogicImpl cubeTransformationLogic = new CubeTransformationLogicImpl();
		return cubeTransformationLogic;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cube_transformation_logicPackage getCube_transformation_logicPackage() {
		return (Cube_transformation_logicPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Cube_transformation_logicPackage getPackage() {
		return Cube_transformation_logicPackage.eINSTANCE;
	}

} //Cube_transformation_logicFactoryImpl
