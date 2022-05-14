/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension;

import org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY;
import org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY_NODE;

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
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule#getMemberHierarchies <em>Member Hierarchies</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule#getMemberHierarchiesNodes <em>Member Hierarchies Nodes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getMemberHierarchyModule()
 * @model extendedMetaData="name='MemberHierarchyModule' kind='elementOnly'"
 * @generated
 */
public interface MemberHierarchyModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Member Hierarchies</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of MemberHierarchies contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Member Hierarchies</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getMemberHierarchyModule_MemberHierarchies()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='memberHierarchies'"
	 * @generated
	 */
	EList<MEMBER_HIERARCHY> getMemberHierarchies();

	/**
	 * Returns the value of the '<em><b>Member Hierarchies Nodes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY_NODE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of MemberHierarchiesNodes contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Member Hierarchies Nodes</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getMemberHierarchyModule_MemberHierarchiesNodes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='memberHierarchiesNodes'"
	 * @generated
	 */
	EList<MEMBER_HIERARCHY_NODE> getMemberHierarchiesNodes();

} // MemberHierarchyModule
