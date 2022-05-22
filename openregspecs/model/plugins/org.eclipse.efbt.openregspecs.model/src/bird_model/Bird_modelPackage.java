/**
 */
package bird_model;

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
 * @see bird_model.Bird_modelFactory
 * @model kind="package"
 * @generated
 */
public interface Bird_modelPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "bird_model";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/bird_model";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "bird_model";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Bird_modelPackage eINSTANCE = bird_model.impl.Bird_modelPackageImpl.init();

	/**
	 * The meta object id for the '{@link bird_model.impl.BIRDModelImpl <em>BIRD Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.BIRDModelImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getBIRDModel()
	 * @generated
	 */
	int BIRD_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Sm Cubes Core Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__SM_CUBES_CORE_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Entity Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__ENTITY_MODULES = 1;

	/**
	 * The feature id for the '<em><b>SQL Entity Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__SQL_ENTITY_MODULES = 2;

	/**
	 * The feature id for the '<em><b>Sub Processes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__SUB_PROCESSES = 3;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__REQUIREMENTS = 4;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__TESTS = 5;

	/**
	 * The number of structural features of the '<em>BIRD Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>BIRD Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link bird_model.BIRDModel <em>BIRD Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BIRD Model</em>'.
	 * @see bird_model.BIRDModel
	 * @generated
	 */
	EClass getBIRDModel();

	/**
	 * Returns the meta object for the containment reference '{@link bird_model.BIRDModel#getSmCubesCoreModel <em>Sm Cubes Core Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Sm Cubes Core Model</em>'.
	 * @see bird_model.BIRDModel#getSmCubesCoreModel()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_SmCubesCoreModel();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.BIRDModel#getEntityModules <em>Entity Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Entity Modules</em>'.
	 * @see bird_model.BIRDModel#getEntityModules()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_EntityModules();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.BIRDModel#getSQLEntityModules <em>SQL Entity Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>SQL Entity Modules</em>'.
	 * @see bird_model.BIRDModel#getSQLEntityModules()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_SQLEntityModules();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.BIRDModel#getSubProcesses <em>Sub Processes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sub Processes</em>'.
	 * @see bird_model.BIRDModel#getSubProcesses()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_SubProcesses();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.BIRDModel#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Requirements</em>'.
	 * @see bird_model.BIRDModel#getRequirements()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_Requirements();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.BIRDModel#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see bird_model.BIRDModel#getTests()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_Tests();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Bird_modelFactory getBird_modelFactory();

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
		 * The meta object literal for the '{@link bird_model.impl.BIRDModelImpl <em>BIRD Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.BIRDModelImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getBIRDModel()
		 * @generated
		 */
		EClass BIRD_MODEL = eINSTANCE.getBIRDModel();

		/**
		 * The meta object literal for the '<em><b>Sm Cubes Core Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__SM_CUBES_CORE_MODEL = eINSTANCE.getBIRDModel_SmCubesCoreModel();

		/**
		 * The meta object literal for the '<em><b>Entity Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__ENTITY_MODULES = eINSTANCE.getBIRDModel_EntityModules();

		/**
		 * The meta object literal for the '<em><b>SQL Entity Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__SQL_ENTITY_MODULES = eINSTANCE.getBIRDModel_SQLEntityModules();

		/**
		 * The meta object literal for the '<em><b>Sub Processes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__SUB_PROCESSES = eINSTANCE.getBIRDModel_SubProcesses();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__REQUIREMENTS = eINSTANCE.getBIRDModel_Requirements();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__TESTS = eINSTANCE.getBIRDModel_Tests();

	}

} //Bird_modelPackage
