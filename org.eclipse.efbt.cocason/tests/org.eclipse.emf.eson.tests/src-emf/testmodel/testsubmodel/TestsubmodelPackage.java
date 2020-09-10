/**
 */
package testmodel.testsubmodel;

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
 * @see testmodel.testsubmodel.TestsubmodelFactory
 * @model kind="package"
 * @generated
 */
public interface TestsubmodelPackage extends EPackage {
    /**
	 * The package name.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    String eNAME = "testsubmodel";

    /**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    String eNS_URI = "testmodel.testsubmodel";

    /**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    String eNS_PREFIX = "testsubmodel";

    /**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 */
    TestsubmodelPackage eINSTANCE = testmodel.testsubmodel.impl.TestsubmodelPackageImpl.init();

    /**
	 * The meta object id for the '{@link testmodel.testsubmodel.impl.TestInnerModelImpl <em>Test Inner Model</em>}' class.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @see testmodel.testsubmodel.impl.TestInnerModelImpl
	 * @see testmodel.testsubmodel.impl.TestsubmodelPackageImpl#getTestInnerModel()
	 * @generated
	 */
    int TEST_INNER_MODEL = 0;

    /**
	 * The number of structural features of the '<em>Test Inner Model</em>' class.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
    int TEST_INNER_MODEL_FEATURE_COUNT = 0;


    /**
	 * Returns the meta object for class '{@link testmodel.testsubmodel.TestInnerModel <em>Test Inner Model</em>}'.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Inner Model</em>'.
	 * @see testmodel.testsubmodel.TestInnerModel
	 * @generated
	 */
    EClass getTestInnerModel();

    /**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
    TestsubmodelFactory getTestsubmodelFactory();

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
		 * The meta object literal for the '{@link testmodel.testsubmodel.impl.TestInnerModelImpl <em>Test Inner Model</em>}' class.
		 * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
		 * @see testmodel.testsubmodel.impl.TestInnerModelImpl
		 * @see testmodel.testsubmodel.impl.TestsubmodelPackageImpl#getTestInnerModel()
		 * @generated
		 */
        EClass TEST_INNER_MODEL = eINSTANCE.getTestInnerModel();

    }

} //TestsubmodelPackage
