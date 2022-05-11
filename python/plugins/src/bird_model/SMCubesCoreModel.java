/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SM Cubes Core Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.SMCubesCoreModel#getName <em>Name</em>}</li>
 *   <li>{@link bird_model.SMCubesCoreModel#getVariableModules <em>Variable Modules</em>}</li>
 *   <li>{@link bird_model.SMCubesCoreModel#getSubDomainModules <em>Sub Domain Modules</em>}</li>
 *   <li>{@link bird_model.SMCubesCoreModel#getDomainModules <em>Domain Modules</em>}</li>
 *   <li>{@link bird_model.SMCubesCoreModel#getMemberModules <em>Member Modules</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getSMCubesCoreModel()
 * @model
 * @generated
 */
public interface SMCubesCoreModel extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see bird_model.Bird_modelPackage#getSMCubesCoreModel_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link bird_model.SMCubesCoreModel#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Variable Modules</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.VariableModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable Modules</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getSMCubesCoreModel_VariableModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<VariableModule> getVariableModules();

	/**
	 * Returns the value of the '<em><b>Sub Domain Modules</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.SubDomainModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sub Domain Modules</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getSMCubesCoreModel_SubDomainModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<SubDomainModule> getSubDomainModules();

	/**
	 * Returns the value of the '<em><b>Domain Modules</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.DomainModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Domain Modules</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getSMCubesCoreModel_DomainModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<DomainModule> getDomainModules();

	/**
	 * Returns the value of the '<em><b>Member Modules</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.MemberModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member Modules</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getSMCubesCoreModel_MemberModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<MemberModule> getMemberModules();

} // SMCubesCoreModel
