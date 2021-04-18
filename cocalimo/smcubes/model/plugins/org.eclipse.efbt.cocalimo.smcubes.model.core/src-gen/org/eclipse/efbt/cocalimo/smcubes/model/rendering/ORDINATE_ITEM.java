/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.rendering;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>ORDINATE ITEM</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getAxis_ordinate_id <em>Axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getIs_starting_member_included <em>Is starting member included</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_hierarchy_id <em>Member hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getStarting_member_id <em>Starting member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getVariable_id <em>Variable id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getORDINATE_ITEM()
 * @model extendedMetaData="name='ORDINATE_ITEM' kind='empty'"
 * @generated
 */
public interface ORDINATE_ITEM extends EObject
{
	/**
	 * Returns the value of the '<em><b>Axis ordinate id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Axis ordinate id</em>' reference.
	 * @see #setAxis_ordinate_id(AXIS_ORDINATE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getORDINATE_ITEM_Axis_ordinate_id()
	 * @model extendedMetaData="kind='attribute' name='axis_ordinate_id'"
	 * @generated
	 */
	AXIS_ORDINATE getAxis_ordinate_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getAxis_ordinate_id <em>Axis ordinate id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Axis ordinate id</em>' reference.
	 * @see #getAxis_ordinate_id()
	 * @generated
	 */
	void setAxis_ordinate_id(AXIS_ORDINATE value);

	/**
	 * Returns the value of the '<em><b>Is starting member included</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is starting member included</em>' attribute.
	 * @see #setIs_starting_member_included(Boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getORDINATE_ITEM_Is_starting_member_included()
	 * @model extendedMetaData="kind='attribute' name='is_starting_member_included'"
	 * @generated
	 */
	Boolean getIs_starting_member_included();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getIs_starting_member_included <em>Is starting member included</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is starting member included</em>' attribute.
	 * @see #getIs_starting_member_included()
	 * @generated
	 */
	void setIs_starting_member_included(Boolean value);

	/**
	 * Returns the value of the '<em><b>Member hierarchy id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member hierarchy id</em>' reference.
	 * @see #setMember_hierarchy_id(MEMBER_HIERARCHY)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getORDINATE_ITEM_Member_hierarchy_id()
	 * @model extendedMetaData="kind='attribute' name='member_hierarchy_id'"
	 * @generated
	 */
	MEMBER_HIERARCHY getMember_hierarchy_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_hierarchy_id <em>Member hierarchy id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member hierarchy id</em>' reference.
	 * @see #getMember_hierarchy_id()
	 * @generated
	 */
	void setMember_hierarchy_id(MEMBER_HIERARCHY value);

	/**
	 * Returns the value of the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member id</em>' reference.
	 * @see #setMember_id(MEMBER)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getORDINATE_ITEM_Member_id()
	 * @model extendedMetaData="kind='attribute' name='member_id'"
	 * @generated
	 */
	MEMBER getMember_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_id <em>Member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member id</em>' reference.
	 * @see #getMember_id()
	 * @generated
	 */
	void setMember_id(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Starting member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Starting member id</em>' reference.
	 * @see #setStarting_member_id(MEMBER)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getORDINATE_ITEM_Starting_member_id()
	 * @model extendedMetaData="kind='attribute' name='starting_member_id'"
	 * @generated
	 */
	MEMBER getStarting_member_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getStarting_member_id <em>Starting member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Starting member id</em>' reference.
	 * @see #getStarting_member_id()
	 * @generated
	 */
	void setStarting_member_id(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable id</em>' reference.
	 * @see #setVariable_id(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getORDINATE_ITEM_Variable_id()
	 * @model extendedMetaData="kind='attribute' name='variable_id'"
	 * @generated
	 */
	VARIABLE getVariable_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getVariable_id <em>Variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable id</em>' reference.
	 * @see #getVariable_id()
	 * @generated
	 */
	void setVariable_id(VARIABLE value);

} // ORDINATE_ITEM
