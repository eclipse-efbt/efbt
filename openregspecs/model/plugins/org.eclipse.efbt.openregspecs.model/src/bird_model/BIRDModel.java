/**
 */
package bird_model;

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
 * A representation of the model object '<em><b>BIRD Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.BIRDModel#getSmCubesCoreModel <em>Sm Cubes Core Model</em>}</li>
 *   <li>{@link bird_model.BIRDModel#getEntityModules <em>Entity Modules</em>}</li>
 *   <li>{@link bird_model.BIRDModel#getSQLEntityModules <em>SQL Entity Modules</em>}</li>
 *   <li>{@link bird_model.BIRDModel#getSubProcesses <em>Sub Processes</em>}</li>
 *   <li>{@link bird_model.BIRDModel#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link bird_model.BIRDModel#getTests <em>Tests</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getBIRDModel()
 * @model
 * @generated
 */
public interface BIRDModel extends EObject {
	/**
	 * Returns the value of the '<em><b>Sm Cubes Core Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sm Cubes Core Model</em>' containment reference.
	 * @see #setSmCubesCoreModel(SMCubesCoreModel)
	 * @see bird_model.Bird_modelPackage#getBIRDModel_SmCubesCoreModel()
	 * @model containment="true"
	 * @generated
	 */
	SMCubesCoreModel getSmCubesCoreModel();

	/**
	 * Sets the value of the '{@link bird_model.BIRDModel#getSmCubesCoreModel <em>Sm Cubes Core Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sm Cubes Core Model</em>' containment reference.
	 * @see #getSmCubesCoreModel()
	 * @generated
	 */
	void setSmCubesCoreModel(SMCubesCoreModel value);

	/**
	 * Returns the value of the '<em><b>Entity Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity Modules</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getBIRDModel_EntityModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<EntityModule> getEntityModules();

	/**
	 * Returns the value of the '<em><b>SQL Entity Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>SQL Entity Modules</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getBIRDModel_SQLEntityModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<SQLEntityModule> getSQLEntityModules();

	/**
	 * Returns the value of the '<em><b>Sub Processes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.bpmn_lite.SubProcess}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sub Processes</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getBIRDModel_SubProcesses()
	 * @model containment="true"
	 * @generated
	 */
	EList<SubProcess> getSubProcesses();

	/**
	 * Returns the value of the '<em><b>Requirements</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Requirements</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getBIRDModel_Requirements()
	 * @model containment="true"
	 * @generated
	 */
	EList<RequirementsModule> getRequirements();

	/**
	 * Returns the value of the '<em><b>Tests</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.TestModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Tests</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getBIRDModel_Tests()
	 * @model containment="true"
	 * @generated
	 */
	EList<TestModule> getTests();

} // BIRDModel
