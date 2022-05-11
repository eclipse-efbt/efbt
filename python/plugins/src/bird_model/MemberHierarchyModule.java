/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Member Hierarchy Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module storing a list of MemberHierarchies and MemberHierarchiesNodes
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.MemberHierarchyModule#getMemberHierarchies <em>Member Hierarchies</em>}</li>
 *   <li>{@link bird_model.MemberHierarchyModule#getMemberHierarchiesNodes <em>Member Hierarchies Nodes</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getMemberHierarchyModule()
 * @model extendedMetaData="name='MemberHierarchyModule' kind='elementOnly'"
 * @generated
 */
public interface MemberHierarchyModule extends bird_model.Module {
	/**
	 * Returns the value of the '<em><b>Member Hierarchies</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.MEMBER_HIERARCHY}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of MemberHierarchies contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Member Hierarchies</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getMemberHierarchyModule_MemberHierarchies()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='memberHierarchies'"
	 * @generated
	 */
	EList<MEMBER_HIERARCHY> getMemberHierarchies();

	/**
	 * Returns the value of the '<em><b>Member Hierarchies Nodes</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.MEMBER_HIERARCHY_NODE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of MemberHierarchiesNodes contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Member Hierarchies Nodes</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getMemberHierarchyModule_MemberHierarchiesNodes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='memberHierarchiesNodes'"
	 * @generated
	 */
	EList<MEMBER_HIERARCHY_NODE> getMemberHierarchiesNodes();

} // MemberHierarchyModule
