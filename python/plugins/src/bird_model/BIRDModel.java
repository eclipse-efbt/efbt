/**
 */
package bird_model;

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
 *   <li>{@link bird_model.BIRDModel#getEntityModule <em>Entity Module</em>}</li>
 *   <li>{@link bird_model.BIRDModel#getSmcubesCoreModel <em>Smcubes Core Model</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getBIRDModel()
 * @model
 * @generated
 */
public interface BIRDModel extends EObject {
	/**
	 * Returns the value of the '<em><b>Entity Module</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.EntityModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity Module</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getBIRDModel_EntityModule()
	 * @model containment="true"
	 * @generated
	 */
	EList<EntityModule> getEntityModule();

	/**
	 * Returns the value of the '<em><b>Smcubes Core Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Smcubes Core Model</em>' containment reference.
	 * @see #setSmcubesCoreModel(SMCubesCoreModel)
	 * @see bird_model.Bird_modelPackage#getBIRDModel_SmcubesCoreModel()
	 * @model containment="true"
	 * @generated
	 */
	SMCubesCoreModel getSmcubesCoreModel();

	/**
	 * Sets the value of the '{@link bird_model.BIRDModel#getSmcubesCoreModel <em>Smcubes Core Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Smcubes Core Model</em>' containment reference.
	 * @see #getSmcubesCoreModel()
	 * @generated
	 */
	void setSmcubesCoreModel(SMCubesCoreModel value);

} // BIRDModel
