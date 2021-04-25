/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import java.util.Date;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE GROUP ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_group_id <em>Cube group id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_id <em>Cube id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_GROUP_ENUMERATION()
 * @model extendedMetaData="name='CUBE_GROUP_ENUMERATION' kind='empty'"
 * @generated
 */
public interface CUBE_GROUP_ENUMERATION extends EObject
{
	/**
	 * Returns the value of the '<em><b>Cube group id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube group id</em>' reference.
	 * @see #setCube_group_id(CUBE_GROUP)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_GROUP_ENUMERATION_Cube_group_id()
	 * @model extendedMetaData="kind='attribute' name='cube_group_id'"
	 * @generated
	 */
	CUBE_GROUP getCube_group_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_group_id <em>Cube group id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube group id</em>' reference.
	 * @see #getCube_group_id()
	 * @generated
	 */
	void setCube_group_id(CUBE_GROUP value);

	/**
	 * Returns the value of the '<em><b>Cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube id</em>' reference.
	 * @see #setCube_id(CUBE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_GROUP_ENUMERATION_Cube_id()
	 * @model extendedMetaData="kind='attribute' name='cube_id'"
	 * @generated
	 */
	CUBE getCube_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getCube_id <em>Cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube id</em>' reference.
	 * @see #getCube_id()
	 * @generated
	 */
	void setCube_id(CUBE value);

	/**
	 * Returns the value of the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Order</em>' attribute.
	 * @see #setOrder(int)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_GROUP_ENUMERATION_Order()
	 * @model extendedMetaData="kind='attribute' name='order'"
	 * @generated
	 */
	int getOrder();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getOrder <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Order</em>' attribute.
	 * @see #getOrder()
	 * @generated
	 */
	void setOrder(int value);

	/**
	 * Returns the value of the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid from</em>' attribute.
	 * @see #setValid_from(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_GROUP_ENUMERATION_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_from <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid from</em>' attribute.
	 * @see #getValid_from()
	 * @generated
	 */
	void setValid_from(Date value);

	/**
	 * Returns the value of the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid to</em>' attribute.
	 * @see #setValid_to(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_GROUP_ENUMERATION_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP_ENUMERATION#getValid_to <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid to</em>' attribute.
	 * @see #getValid_to()
	 * @generated
	 */
	void setValid_to(Date value);

} // CUBE_GROUP_ENUMERATION
