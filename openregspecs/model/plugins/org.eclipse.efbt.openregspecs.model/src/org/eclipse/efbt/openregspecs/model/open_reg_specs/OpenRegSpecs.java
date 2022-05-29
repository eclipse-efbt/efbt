/**
 */
package org.eclipse.efbt.openregspecs.model.open_reg_specs;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.SubProcess;

import org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel;

import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule;

import org.eclipse.efbt.openregspecs.model.testing.TestModule;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Open Reg Specs</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getTypes_and_concepts <em>Types and concepts</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getData_model <em>Data model</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getProcess_workflow <em>Process workflow</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getReport_generation <em>Report generation</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getTests <em>Tests</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#getOpenRegSpecs()
 * @model
 * @generated
 */
public interface OpenRegSpecs extends EObject {
	/**
	 * Returns the value of the '<em><b>Requirements</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Requirements</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#getOpenRegSpecs_Requirements()
	 * @model containment="true"
	 * @generated
	 */
	EList<RequirementsModule> getRequirements();

	/**
	 * Returns the value of the '<em><b>Types and concepts</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Types and concepts</em>' containment reference.
	 * @see #setTypes_and_concepts(SMCubesCoreModel)
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#getOpenRegSpecs_Types_and_concepts()
	 * @model containment="true"
	 * @generated
	 */
	SMCubesCoreModel getTypes_and_concepts();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs#getTypes_and_concepts <em>Types and concepts</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Types and concepts</em>' containment reference.
	 * @see #getTypes_and_concepts()
	 * @generated
	 */
	void setTypes_and_concepts(SMCubesCoreModel value);

	/**
	 * Returns the value of the '<em><b>Data model</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Data model</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#getOpenRegSpecs_Data_model()
	 * @model containment="true"
	 * @generated
	 */
	EList<EntityModule> getData_model();

	/**
	 * Returns the value of the '<em><b>Process workflow</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.bpmn_lite.SubProcess}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Process workflow</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#getOpenRegSpecs_Process_workflow()
	 * @model containment="true"
	 * @generated
	 */
	EList<SubProcess> getProcess_workflow();

	/**
	 * Returns the value of the '<em><b>Report generation</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Report generation</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#getOpenRegSpecs_Report_generation()
	 * @model containment="true"
	 * @generated
	 */
	EList<SQLEntityModule> getReport_generation();

	/**
	 * Returns the value of the '<em><b>Tests</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.TestModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Tests</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage#getOpenRegSpecs_Tests()
	 * @model containment="true"
	 * @generated
	 */
	EList<TestModule> getTests();

} // OpenRegSpecs
