/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import java.util.Date;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE HIERARCHY NODE</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_group_id <em>Cube group id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_hierarchy_id <em>Cube hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getLevel <em>Level</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNODE_CODE <em>NODE CODE</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNode_name <em>Node name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getParent_node_code <em>Parent node code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE()
 * @model extendedMetaData="name='CUBE_HIERARCHY_NODE' kind='empty'"
 * @generated
 */
public interface CUBE_HIERARCHY_NODE extends EObject {
	/**
	 * Returns the value of the '<em><b>Cube group id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube group id</em>' reference.
	 * @see #setCube_group_id(CUBE_GROUP)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Cube_group_id()
	 * @model extendedMetaData="kind='attribute' name='cube_group_id'"
	 * @generated
	 */
	CUBE_GROUP getCube_group_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_group_id <em>Cube group id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube group id</em>' reference.
	 * @see #getCube_group_id()
	 * @generated
	 */
	void setCube_group_id(CUBE_GROUP value);

	/**
	 * Returns the value of the '<em><b>Cube hierarchy id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube hierarchy id</em>' reference.
	 * @see #setCube_hierarchy_id(CUBE_HIERARCHY)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Cube_hierarchy_id()
	 * @model extendedMetaData="kind='attribute' name='cube_hierarchy_id'"
	 * @generated
	 */
	CUBE_HIERARCHY getCube_hierarchy_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getCube_hierarchy_id <em>Cube hierarchy id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube hierarchy id</em>' reference.
	 * @see #getCube_hierarchy_id()
	 * @generated
	 */
	void setCube_hierarchy_id(CUBE_HIERARCHY value);

	/**
	 * Returns the value of the '<em><b>Level</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Level</em>' attribute.
	 * @see #setLevel(int)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Level()
	 * @model extendedMetaData="kind='attribute' name='level'"
	 * @generated
	 */
	int getLevel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getLevel <em>Level</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Level</em>' attribute.
	 * @see #getLevel()
	 * @generated
	 */
	void setLevel(int value);

	/**
	 * Returns the value of the '<em><b>NODE CODE</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>NODE CODE</em>' attribute.
	 * @see #setNODE_CODE(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_NODE_CODE()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='NODE_CODE'"
	 * @generated
	 */
	String getNODE_CODE();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNODE_CODE <em>NODE CODE</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>NODE CODE</em>' attribute.
	 * @see #getNODE_CODE()
	 * @generated
	 */
	void setNODE_CODE(String value);

	/**
	 * Returns the value of the '<em><b>Node name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Node name</em>' attribute.
	 * @see #setNode_name(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Node_name()
	 * @model extendedMetaData="kind='attribute' name='node_name'"
	 * @generated
	 */
	String getNode_name();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getNode_name <em>Node name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Node name</em>' attribute.
	 * @see #getNode_name()
	 * @generated
	 */
	void setNode_name(String value);

	/**
	 * Returns the value of the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Order</em>' attribute.
	 * @see #setOrder(int)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Order()
	 * @model extendedMetaData="kind='attribute' name='order'"
	 * @generated
	 */
	int getOrder();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getOrder <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Order</em>' attribute.
	 * @see #getOrder()
	 * @generated
	 */
	void setOrder(int value);

	/**
	 * Returns the value of the '<em><b>Parent node code</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parent node code</em>' reference.
	 * @see #setParent_node_code(CUBE_HIERARCHY_NODE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Parent_node_code()
	 * @model extendedMetaData="kind='attribute' name='parent_node_code'"
	 * @generated
	 */
	CUBE_HIERARCHY_NODE getParent_node_code();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getParent_node_code <em>Parent node code</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parent node code</em>' reference.
	 * @see #getParent_node_code()
	 * @generated
	 */
	void setParent_node_code(CUBE_HIERARCHY_NODE value);

	/**
	 * Returns the value of the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid from</em>' attribute.
	 * @see #setValid_from(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_from <em>Valid from</em>}' attribute.
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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_HIERARCHY_NODE_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_HIERARCHY_NODE#getValid_to <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid to</em>' attribute.
	 * @see #getValid_to()
	 * @generated
	 */
	void setValid_to(Date value);

} // CUBE_HIERARCHY_NODE
