/**
 */
package org.eclipse.efbt.openregspecs.model.open_reg_specs;

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
 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsFactory
 * @model kind="package"
 * @generated
 */
public interface Open_reg_specsPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "open_reg_specs";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/open_reg_specs";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "open_reg_specs";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Open_reg_specsPackage eINSTANCE = org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.Open_reg_specsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl <em>Open Reg Specs</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.Open_reg_specsPackageImpl#getOpenRegSpecs()
	 * @generated
	 */
	int OPEN_REG_SPECS = 0;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS__REQUIREMENTS = 0;

	/**
	 * The feature id for the '<em><b>Types and concepts</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS__TYPES_AND_CONCEPTS = 1;

	/**
	 * The feature id for the '<em><b>Data model</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS__DATA_MODEL = 2;

	/**
	 * The feature id for the '<em><b>Process workflow</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS__PROCESS_WORKFLOW = 3;

	/**
	 * The feature id for the '<em><b>Report generation</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS__REPORT_GENERATION = 4;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS__TESTS = 5;

	/**
	 * The number of structural features of the '<em>Open Reg Specs</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>Open Reg Specs</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPEN_REG_SPECS_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs <em>Open Reg Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Open Reg Specs</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs
	 * @generated
	 */
	EClass getOpenRegSpecs();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getRequirements()
	 * @see #getOpenRegSpecs()
	 * @generated
	 */
	EReference getOpenRegSpecs_Requirements();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getTypes_and_concepts <em>Types and concepts</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Types and concepts</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getTypes_and_concepts()
	 * @see #getOpenRegSpecs()
	 * @generated
	 */
	EReference getOpenRegSpecs_Types_and_concepts();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getData_model <em>Data model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Data model</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getData_model()
	 * @see #getOpenRegSpecs()
	 * @generated
	 */
	EReference getOpenRegSpecs_Data_model();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getProcess_workflow <em>Process workflow</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Process workflow</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getProcess_workflow()
	 * @see #getOpenRegSpecs()
	 * @generated
	 */
	EReference getOpenRegSpecs_Process_workflow();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getReport_generation <em>Report generation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report generation</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getReport_generation()
	 * @see #getOpenRegSpecs()
	 * @generated
	 */
	EReference getOpenRegSpecs_Report_generation();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getTests()
	 * @see #getOpenRegSpecs()
	 * @generated
	 */
	EReference getOpenRegSpecs_Tests();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Open_reg_specsFactory getOpen_reg_specsFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl <em>Open Reg Specs</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl
		 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.Open_reg_specsPackageImpl#getOpenRegSpecs()
		 * @generated
		 */
		EClass OPEN_REG_SPECS = eINSTANCE.getOpenRegSpecs();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference OPEN_REG_SPECS__REQUIREMENTS = eINSTANCE.getOpenRegSpecs_Requirements();

		/**
		 * The meta object literal for the '<em><b>Types and concepts</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference OPEN_REG_SPECS__TYPES_AND_CONCEPTS = eINSTANCE.getOpenRegSpecs_Types_and_concepts();

		/**
		 * The meta object literal for the '<em><b>Data model</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference OPEN_REG_SPECS__DATA_MODEL = eINSTANCE.getOpenRegSpecs_Data_model();

		/**
		 * The meta object literal for the '<em><b>Process workflow</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference OPEN_REG_SPECS__PROCESS_WORKFLOW = eINSTANCE.getOpenRegSpecs_Process_workflow();

		/**
		 * The meta object literal for the '<em><b>Report generation</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference OPEN_REG_SPECS__REPORT_GENERATION = eINSTANCE.getOpenRegSpecs_Report_generation();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference OPEN_REG_SPECS__TESTS = eINSTANCE.getOpenRegSpecs_Tests();

	}

} //Open_reg_specsPackage
