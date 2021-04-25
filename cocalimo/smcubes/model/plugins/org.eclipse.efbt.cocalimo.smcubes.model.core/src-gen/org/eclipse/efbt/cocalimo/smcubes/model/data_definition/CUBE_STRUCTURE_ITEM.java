/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE STRUCTURE ITEM</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getAttribute_associated_variable <em>Attribute associated variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_structure_id <em>Cube structure id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDimension_type <em>Dimension type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_flow <em>Is flow</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_mandatory <em>Is mandatory</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getRole <em>Role</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_id <em>Variable id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_set_id <em>Variable set id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIsIdentifier <em>Is Identifier</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_variable_code <em>Cube variable code</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM()
 * @model extendedMetaData="name='CUBE_STRUCTURE_ITEM' kind='empty'"
 * @generated
 */
public interface CUBE_STRUCTURE_ITEM extends EObject
{
	/**
	 * Returns the value of the '<em><b>Attribute associated variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attribute associated variable</em>' reference.
	 * @see #setAttribute_associated_variable(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Attribute_associated_variable()
	 * @model extendedMetaData="kind='attribute' name='attribute_associated_variable'"
	 * @generated
	 */
	VARIABLE getAttribute_associated_variable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getAttribute_associated_variable <em>Attribute associated variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attribute associated variable</em>' reference.
	 * @see #getAttribute_associated_variable()
	 * @generated
	 */
	void setAttribute_associated_variable(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Cube structure id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube structure id</em>' reference.
	 * @see #setCube_structure_id(CUBE_STRUCTURE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Cube_structure_id()
	 * @model extendedMetaData="kind='attribute' name='cube_structure_id'"
	 * @generated
	 */
	CUBE_STRUCTURE getCube_structure_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_structure_id <em>Cube structure id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube structure id</em>' reference.
	 * @see #getCube_structure_id()
	 * @generated
	 */
	void setCube_structure_id(CUBE_STRUCTURE value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Description()
	 * @model extendedMetaData="kind='attribute' name='description'"
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Dimension type</b></em>' attribute.
	 * The literals are from the enumeration {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Dimension type</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
	 * @see #setDimension_type(TYP_DMNSN)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Dimension_type()
	 * @model extendedMetaData="kind='attribute' name='dimension_type'"
	 * @generated
	 */
	TYP_DMNSN getDimension_type();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getDimension_type <em>Dimension type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Dimension type</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN
	 * @see #getDimension_type()
	 * @generated
	 */
	void setDimension_type(TYP_DMNSN value);

	/**
	 * Returns the value of the '<em><b>Is flow</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is flow</em>' attribute.
	 * @see #setIs_flow(boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Is_flow()
	 * @model extendedMetaData="kind='attribute' name='is_flow'"
	 * @generated
	 */
	boolean isIs_flow();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_flow <em>Is flow</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is flow</em>' attribute.
	 * @see #isIs_flow()
	 * @generated
	 */
	void setIs_flow(boolean value);

	/**
	 * Returns the value of the '<em><b>Is mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is mandatory</em>' attribute.
	 * @see #setIs_mandatory(boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Is_mandatory()
	 * @model extendedMetaData="kind='attribute' name='is_mandatory'"
	 * @generated
	 */
	boolean isIs_mandatory();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIs_mandatory <em>Is mandatory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is mandatory</em>' attribute.
	 * @see #isIs_mandatory()
	 * @generated
	 */
	void setIs_mandatory(boolean value);

	/**
	 * Returns the value of the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member id</em>' reference.
	 * @see #setMember_id(MEMBER)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Member_id()
	 * @model extendedMetaData="kind='attribute' name='member_id'"
	 * @generated
	 */
	MEMBER getMember_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getMember_id <em>Member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member id</em>' reference.
	 * @see #getMember_id()
	 * @generated
	 */
	void setMember_id(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Order</em>' attribute.
	 * @see #setOrder(int)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Order()
	 * @model extendedMetaData="kind='attribute' name='order'"
	 * @generated
	 */
	int getOrder();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getOrder <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Order</em>' attribute.
	 * @see #getOrder()
	 * @generated
	 */
	void setOrder(int value);

	/**
	 * Returns the value of the '<em><b>Role</b></em>' attribute.
	 * The literals are from the enumeration {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Role</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
	 * @see #setRole(TYP_RL)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Role()
	 * @model extendedMetaData="kind='attribute' name='role'"
	 * @generated
	 */
	TYP_RL getRole();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getRole <em>Role</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Role</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL
	 * @see #getRole()
	 * @generated
	 */
	void setRole(TYP_RL value);

	/**
	 * Returns the value of the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomain id</em>' reference.
	 * @see #setSubdomain_id(SUBDOMAIN)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Subdomain_id()
	 * @model extendedMetaData="kind='attribute' name='subdomain_id'"
	 * @generated
	 */
	SUBDOMAIN getSubdomain_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getSubdomain_id <em>Subdomain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Subdomain id</em>' reference.
	 * @see #getSubdomain_id()
	 * @generated
	 */
	void setSubdomain_id(SUBDOMAIN value);

	/**
	 * Returns the value of the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable id</em>' reference.
	 * @see #setVariable_id(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Variable_id()
	 * @model extendedMetaData="kind='attribute' name='variable_id'"
	 * @generated
	 */
	VARIABLE getVariable_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_id <em>Variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable id</em>' reference.
	 * @see #getVariable_id()
	 * @generated
	 */
	void setVariable_id(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable set id</em>' reference.
	 * @see #setVariable_set_id(VARIABLE_SET)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Variable_set_id()
	 * @model extendedMetaData="kind='attribute' name='variable_set_id'"
	 * @generated
	 */
	VARIABLE_SET getVariable_set_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getVariable_set_id <em>Variable set id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable set id</em>' reference.
	 * @see #getVariable_set_id()
	 * @generated
	 */
	void setVariable_set_id(VARIABLE_SET value);

	/**
	 * Returns the value of the '<em><b>Is Identifier</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is Identifier</em>' attribute.
	 * @see #setIsIdentifier(boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_IsIdentifier()
	 * @model
	 * @generated
	 */
	boolean isIsIdentifier();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#isIsIdentifier <em>Is Identifier</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is Identifier</em>' attribute.
	 * @see #isIsIdentifier()
	 * @generated
	 */
	void setIsIdentifier(boolean value);

	/**
	 * Returns the value of the '<em><b>Cube variable code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube variable code</em>' attribute.
	 * @see #setCube_variable_code(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_STRUCTURE_ITEM_Cube_variable_code()
	 * @model
	 * @generated
	 */
	String getCube_variable_code();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM#getCube_variable_code <em>Cube variable code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube variable code</em>' attribute.
	 * @see #getCube_variable_code()
	 * @generated
	 */
	void setCube_variable_code(String value);

} // CUBE_STRUCTURE_ITEM
