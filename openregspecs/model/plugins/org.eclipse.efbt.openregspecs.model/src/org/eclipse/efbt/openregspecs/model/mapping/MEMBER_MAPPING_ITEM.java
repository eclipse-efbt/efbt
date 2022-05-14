/**
 */
package org.eclipse.efbt.openregspecs.model.mapping;

import org.eclipse.efbt.openregspecs.model.core.MEMBER;
import org.eclipse.efbt.openregspecs.model.core.VARIABLE;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>MEMBER MAPPING ITEM</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING_ITEM#getIsSource <em>Is Source</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING_ITEM#getMember <em>Member</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING_ITEM#getVariable <em>Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.mapping.MappingPackage#getMEMBER_MAPPING_ITEM()
 * @model
 * @generated
 */
public interface MEMBER_MAPPING_ITEM extends EObject {
	/**
	 * Returns the value of the '<em><b>Is Source</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is Source</em>' attribute.
	 * @see #setIsSource(String)
	 * @see org.eclipse.efbt.openregspecs.model.mapping.MappingPackage#getMEMBER_MAPPING_ITEM_IsSource()
	 * @model
	 * @generated
	 */
	String getIsSource();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING_ITEM#getIsSource <em>Is Source</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is Source</em>' attribute.
	 * @see #getIsSource()
	 * @generated
	 */
	void setIsSource(String value);

	/**
	 * Returns the value of the '<em><b>Member</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member</em>' reference.
	 * @see #setMember(MEMBER)
	 * @see org.eclipse.efbt.openregspecs.model.mapping.MappingPackage#getMEMBER_MAPPING_ITEM_Member()
	 * @model
	 * @generated
	 */
	MEMBER getMember();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING_ITEM#getMember <em>Member</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member</em>' reference.
	 * @see #getMember()
	 * @generated
	 */
	void setMember(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable</em>' reference.
	 * @see #setVariable(VARIABLE)
	 * @see org.eclipse.efbt.openregspecs.model.mapping.MappingPackage#getMEMBER_MAPPING_ITEM_Variable()
	 * @model
	 * @generated
	 */
	VARIABLE getVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING_ITEM#getVariable <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable</em>' reference.
	 * @see #getVariable()
	 * @generated
	 */
	void setVariable(VARIABLE value);

} // MEMBER_MAPPING_ITEM
