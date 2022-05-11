/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Member Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module storing a list of Members
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.MemberModule#getMembers <em>Members</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getMemberModule()
 * @model extendedMetaData="name='MemberModule' kind='elementOnly'"
 * @generated
 */
public interface MemberModule extends bird_model.Module {
	/**
	 * Returns the value of the '<em><b>Members</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.MEMBER}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of Members contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Members</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getMemberModule_Members()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='members'"
	 * @generated
	 */
	EList<MEMBER> getMembers();

} // MemberModule
