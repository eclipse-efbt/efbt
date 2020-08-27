/**
 */
package testmodel.testsubmodel.impl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

import testmodel.testsubmodel.*;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestsubmodelFactoryImpl extends EFactoryImpl implements TestsubmodelFactory {
    /**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    public static TestsubmodelFactory init() {
		try {
			TestsubmodelFactory theTestsubmodelFactory = (TestsubmodelFactory)EPackage.Registry.INSTANCE.getEFactory(TestsubmodelPackage.eNS_URI);
			if (theTestsubmodelFactory != null) {
				return theTestsubmodelFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TestsubmodelFactoryImpl();
	}

    /**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    public TestsubmodelFactoryImpl() {
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
			case TestsubmodelPackage.TEST_INNER_MODEL: return createTestInnerModel();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

    /**
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    public TestInnerModel createTestInnerModel() {
		TestInnerModelImpl testInnerModel = new TestInnerModelImpl();
		return testInnerModel;
	}

    /**
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    public TestsubmodelPackage getTestsubmodelPackage() {
		return (TestsubmodelPackage)getEPackage();
	}

    /**
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
    @Deprecated
    public static TestsubmodelPackage getPackage() {
		return TestsubmodelPackage.eINSTANCE;
	}

} //TestsubmodelFactoryImpl
