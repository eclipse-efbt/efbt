/**
 */
package testModelWithXtextReference.impl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

import testModelWithXtextReference.*;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestModelWithXtextReferenceFactoryImpl extends EFactoryImpl implements TestModelWithXtextReferenceFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static TestModelWithXtextReferenceFactory init() {
		try {
			TestModelWithXtextReferenceFactory theTestModelWithXtextReferenceFactory = (TestModelWithXtextReferenceFactory)EPackage.Registry.INSTANCE.getEFactory(TestModelWithXtextReferencePackage.eNS_URI);
			if (theTestModelWithXtextReferenceFactory != null) {
				return theTestModelWithXtextReferenceFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TestModelWithXtextReferenceFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModelWithXtextReferenceFactoryImpl() {
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
			case TestModelWithXtextReferencePackage.TEST_MODEL_WITH_XTEXT_REFERENCE: return createTestModelWithXtextReference();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModelWithXtextReference createTestModelWithXtextReference() {
		TestModelWithXtextReferenceImpl testModelWithXtextReference = new TestModelWithXtextReferenceImpl();
		return testModelWithXtextReference;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModelWithXtextReferencePackage getTestModelWithXtextReferencePackage() {
		return (TestModelWithXtextReferencePackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static TestModelWithXtextReferencePackage getPackage() {
		return TestModelWithXtextReferencePackage.eINSTANCE;
	}

} //TestModelWithXtextReferenceFactoryImpl
