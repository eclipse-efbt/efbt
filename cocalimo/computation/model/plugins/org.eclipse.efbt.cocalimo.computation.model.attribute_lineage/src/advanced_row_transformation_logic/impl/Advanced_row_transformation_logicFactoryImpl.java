/**
 */
package advanced_row_transformation_logic.impl;

import advanced_row_transformation_logic.*;

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
public class Advanced_row_transformation_logicFactoryImpl extends EFactoryImpl implements Advanced_row_transformation_logicFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Advanced_row_transformation_logicFactory init() {
		try {
			Advanced_row_transformation_logicFactory theAdvanced_row_transformation_logicFactory = (Advanced_row_transformation_logicFactory)EPackage.Registry.INSTANCE.getEFactory(Advanced_row_transformation_logicPackage.eNS_URI);
			if (theAdvanced_row_transformation_logicFactory != null) {
				return theAdvanced_row_transformation_logicFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Advanced_row_transformation_logicFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Advanced_row_transformation_logicFactoryImpl() {
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
			case Advanced_row_transformation_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION: return createExplodeArrayOfStructsRowFunction();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ExplodeArrayOfStructsRowFunction createExplodeArrayOfStructsRowFunction() {
		ExplodeArrayOfStructsRowFunctionImpl explodeArrayOfStructsRowFunction = new ExplodeArrayOfStructsRowFunctionImpl();
		return explodeArrayOfStructsRowFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Advanced_row_transformation_logicPackage getAdvanced_row_transformation_logicPackage() {
		return (Advanced_row_transformation_logicPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Advanced_row_transformation_logicPackage getPackage() {
		return Advanced_row_transformation_logicPackage.eINSTANCE;
	}

} //Advanced_row_transformation_logicFactoryImpl
