/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition;

import org.eclipse.efbt.cocamo.smcubes.model.core.MEMBER;

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
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.MemberModule#getMembers <em>Members</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getMemberModule()
 * @model extendedMetaData="name='MemberModule' kind='elementOnly'"
 * @generated
 */
public interface MemberModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Members</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocamo.smcubes.model.core.MEMBER}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of Members contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Members</em>' containment reference list.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage#getMemberModule_Members()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='members'"
	 * @generated
	 */
	EList<MEMBER> getMembers();

} // MemberModule
