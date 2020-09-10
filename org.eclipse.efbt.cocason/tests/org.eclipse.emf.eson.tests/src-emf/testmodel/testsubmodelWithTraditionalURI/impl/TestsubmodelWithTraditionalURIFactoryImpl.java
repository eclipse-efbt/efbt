/**
 */
package testmodel.testsubmodelWithTraditionalURI.impl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

import testmodel.testsubmodelWithTraditionalURI.*;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestsubmodelWithTraditionalURIFactoryImpl extends EFactoryImpl implements TestsubmodelWithTraditionalURIFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static TestsubmodelWithTraditionalURIFactory init() {
		try {
			TestsubmodelWithTraditionalURIFactory theTestsubmodelWithTraditionalURIFactory = (TestsubmodelWithTraditionalURIFactory)EPackage.Registry.INSTANCE.getEFactory(TestsubmodelWithTraditionalURIPackage.eNS_URI);
			if (theTestsubmodelWithTraditionalURIFactory != null) {
				return theTestsubmodelWithTraditionalURIFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TestsubmodelWithTraditionalURIFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestsubmodelWithTraditionalURIFactoryImpl() {
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
			case TestsubmodelWithTraditionalURIPackage.TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI: return createTestInnerModelInPackageWithTraditionalURI();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestInnerModelInPackageWithTraditionalURI createTestInnerModelInPackageWithTraditionalURI() {
		TestInnerModelInPackageWithTraditionalURIImpl testInnerModelInPackageWithTraditionalURI = new TestInnerModelInPackageWithTraditionalURIImpl();
		return testInnerModelInPackageWithTraditionalURI;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestsubmodelWithTraditionalURIPackage getTestsubmodelWithTraditionalURIPackage() {
		return (TestsubmodelWithTraditionalURIPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static TestsubmodelWithTraditionalURIPackage getPackage() {
		return TestsubmodelWithTraditionalURIPackage.eINSTANCE;
	}

} //TestsubmodelWithTraditionalURIFactoryImpl
