/**
 */
package testmodel.testsubmodelWithTraditionalURI;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see testmodel.testsubmodelWithTraditionalURI.TestsubmodelWithTraditionalURIFactory
 * @model kind="package"
 * @generated
 */
public interface TestsubmodelWithTraditionalURIPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "testsubmodelWithTraditionalURI";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://testmodel/subWithTraditionalURI/1.0";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "testsubmodelWithTraditionalURI";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestsubmodelWithTraditionalURIPackage eINSTANCE = testmodel.testsubmodelWithTraditionalURI.impl.TestsubmodelWithTraditionalURIPackageImpl.init();

	/**
	 * The meta object id for the '{@link testmodel.testsubmodelWithTraditionalURI.impl.TestInnerModelInPackageWithTraditionalURIImpl <em>Test Inner Model In Package With Traditional URI</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testmodel.testsubmodelWithTraditionalURI.impl.TestInnerModelInPackageWithTraditionalURIImpl
	 * @see testmodel.testsubmodelWithTraditionalURI.impl.TestsubmodelWithTraditionalURIPackageImpl#getTestInnerModelInPackageWithTraditionalURI()
	 * @generated
	 */
	int TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI__NAME = 0;

	/**
	 * The number of structural features of the '<em>Test Inner Model In Package With Traditional URI</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI_FEATURE_COUNT = 1;


	/**
	 * Returns the meta object for class '{@link testmodel.testsubmodelWithTraditionalURI.TestInnerModelInPackageWithTraditionalURI <em>Test Inner Model In Package With Traditional URI</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Inner Model In Package With Traditional URI</em>'.
	 * @see testmodel.testsubmodelWithTraditionalURI.TestInnerModelInPackageWithTraditionalURI
	 * @generated
	 */
	EClass getTestInnerModelInPackageWithTraditionalURI();

	/**
	 * Returns the meta object for the attribute '{@link testmodel.testsubmodelWithTraditionalURI.TestInnerModelInPackageWithTraditionalURI#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testmodel.testsubmodelWithTraditionalURI.TestInnerModelInPackageWithTraditionalURI#getName()
	 * @see #getTestInnerModelInPackageWithTraditionalURI()
	 * @generated
	 */
	EAttribute getTestInnerModelInPackageWithTraditionalURI_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TestsubmodelWithTraditionalURIFactory getTestsubmodelWithTraditionalURIFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals {
		/**
		 * The meta object literal for the '{@link testmodel.testsubmodelWithTraditionalURI.impl.TestInnerModelInPackageWithTraditionalURIImpl <em>Test Inner Model In Package With Traditional URI</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testmodel.testsubmodelWithTraditionalURI.impl.TestInnerModelInPackageWithTraditionalURIImpl
		 * @see testmodel.testsubmodelWithTraditionalURI.impl.TestsubmodelWithTraditionalURIPackageImpl#getTestInnerModelInPackageWithTraditionalURI()
		 * @generated
		 */
		EClass TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI = eINSTANCE.getTestInnerModelInPackageWithTraditionalURI();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST_INNER_MODEL_IN_PACKAGE_WITH_TRADITIONAL_URI__NAME = eINSTANCE.getTestInnerModelInPackageWithTraditionalURI_Name();

	}

} //TestsubmodelWithTraditionalURIPackage
