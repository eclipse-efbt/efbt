/**
 */
package testModelWithXtextReference;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see testModelWithXtextReference.TestModelWithXtextReferenceFactory
 * @model kind="package"
 * @generated
 */
public interface TestModelWithXtextReferencePackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "testModelWithXtextReference";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://testModelWithXtextReference/1.0";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "testModelWithXtextReference";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestModelWithXtextReferencePackage eINSTANCE = testModelWithXtextReference.impl.TestModelWithXtextReferencePackageImpl.init();

	/**
	 * The meta object id for the '{@link testModelWithXtextReference.impl.TestModelWithXtextReferenceImpl <em>Test Model With Xtext Reference</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see testModelWithXtextReference.impl.TestModelWithXtextReferenceImpl
	 * @see testModelWithXtextReference.impl.TestModelWithXtextReferencePackageImpl#getTestModelWithXtextReference()
	 * @generated
	 */
	int TEST_MODEL_WITH_XTEXT_REFERENCE = 0;

	/**
	 * The feature id for the '<em><b>Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL_WITH_XTEXT_REFERENCE__MODEL = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL_WITH_XTEXT_REFERENCE__NAME = 1;

	/**
	 * The number of structural features of the '<em>Test Model With Xtext Reference</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL_WITH_XTEXT_REFERENCE_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Test Model With Xtext Reference</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODEL_WITH_XTEXT_REFERENCE_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link testModelWithXtextReference.TestModelWithXtextReference <em>Test Model With Xtext Reference</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Model With Xtext Reference</em>'.
	 * @see testModelWithXtextReference.TestModelWithXtextReference
	 * @generated
	 */
	EClass getTestModelWithXtextReference();

	/**
	 * Returns the meta object for the reference '{@link testModelWithXtextReference.TestModelWithXtextReference#getModel <em>Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Model</em>'.
	 * @see testModelWithXtextReference.TestModelWithXtextReference#getModel()
	 * @see #getTestModelWithXtextReference()
	 * @generated
	 */
	EReference getTestModelWithXtextReference_Model();

	/**
	 * Returns the meta object for the attribute '{@link testModelWithXtextReference.TestModelWithXtextReference#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see testModelWithXtextReference.TestModelWithXtextReference#getName()
	 * @see #getTestModelWithXtextReference()
	 * @generated
	 */
	EAttribute getTestModelWithXtextReference_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TestModelWithXtextReferenceFactory getTestModelWithXtextReferenceFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals {
		/**
		 * The meta object literal for the '{@link testModelWithXtextReference.impl.TestModelWithXtextReferenceImpl <em>Test Model With Xtext Reference</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see testModelWithXtextReference.impl.TestModelWithXtextReferenceImpl
		 * @see testModelWithXtextReference.impl.TestModelWithXtextReferencePackageImpl#getTestModelWithXtextReference()
		 * @generated
		 */
		EClass TEST_MODEL_WITH_XTEXT_REFERENCE = eINSTANCE.getTestModelWithXtextReference();

		/**
		 * The meta object literal for the '<em><b>Model</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODEL_WITH_XTEXT_REFERENCE__MODEL = eINSTANCE.getTestModelWithXtextReference_Model();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST_MODEL_WITH_XTEXT_REFERENCE__NAME = eINSTANCE.getTestModelWithXtextReference_Name();

	}

} //TestModelWithXtextReferencePackage
