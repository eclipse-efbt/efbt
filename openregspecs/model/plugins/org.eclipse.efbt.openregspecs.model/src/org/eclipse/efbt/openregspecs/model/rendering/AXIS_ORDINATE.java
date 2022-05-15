/**
 */
package org.eclipse.efbt.openregspecs.model.rendering;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>AXIS ORDINATE</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getAxis_id <em>Axis id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getAxis_ordinate_id <em>Axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getIs_abstract_header <em>Is abstract header</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getLevel <em>Level</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getParent_axis_ordinate_id <em>Parent axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getPath <em>Path</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE()
 * @model extendedMetaData="name='AXIS_ORDINATE' kind='empty'"
 * @generated
 */
public interface AXIS_ORDINATE extends EObject {
	/**
	 * Returns the value of the '<em><b>Axis id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Axis id</em>' reference.
	 * @see #setAxis_id(AXIS)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Axis_id()
	 * @model extendedMetaData="kind='attribute' name='axis_id'"
	 * @generated
	 */
	AXIS getAxis_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getAxis_id <em>Axis id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Axis id</em>' reference.
	 * @see #getAxis_id()
	 * @generated
	 */
	void setAxis_id(AXIS value);

	/**
	 * Returns the value of the '<em><b>Axis ordinate id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Axis ordinate id</em>' attribute.
	 * @see #setAxis_ordinate_id(String)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Axis_ordinate_id()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='axis_ordinate_id'"
	 * @generated
	 */
	String getAxis_ordinate_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getAxis_ordinate_id <em>Axis ordinate id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Axis ordinate id</em>' attribute.
	 * @see #getAxis_ordinate_id()
	 * @generated
	 */
	void setAxis_ordinate_id(String value);

	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Description()
	 * @model extendedMetaData="kind='attribute' name='description'"
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Is abstract header</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is abstract header</em>' attribute.
	 * @see #setIs_abstract_header(Boolean)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Is_abstract_header()
	 * @model extendedMetaData="kind='attribute' name='is_abstract_header'"
	 * @generated
	 */
	Boolean getIs_abstract_header();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getIs_abstract_header <em>Is abstract header</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is abstract header</em>' attribute.
	 * @see #getIs_abstract_header()
	 * @generated
	 */
	void setIs_abstract_header(Boolean value);

	/**
	 * Returns the value of the '<em><b>Level</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Level</em>' attribute.
	 * @see #setLevel(int)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Level()
	 * @model extendedMetaData="kind='attribute' name='level'"
	 * @generated
	 */
	int getLevel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getLevel <em>Level</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Level</em>' attribute.
	 * @see #getLevel()
	 * @generated
	 */
	void setLevel(int value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Order</em>' attribute.
	 * @see #setOrder(int)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Order()
	 * @model extendedMetaData="kind='attribute' name='order'"
	 * @generated
	 */
	int getOrder();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getOrder <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Order</em>' attribute.
	 * @see #getOrder()
	 * @generated
	 */
	void setOrder(int value);

	/**
	 * Returns the value of the '<em><b>Parent axis ordinate id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parent axis ordinate id</em>' reference.
	 * @see #setParent_axis_ordinate_id(AXIS_ORDINATE)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Parent_axis_ordinate_id()
	 * @model extendedMetaData="kind='attribute' name='parent_axis_ordinate_id'"
	 * @generated
	 */
	AXIS_ORDINATE getParent_axis_ordinate_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getParent_axis_ordinate_id <em>Parent axis ordinate id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parent axis ordinate id</em>' reference.
	 * @see #getParent_axis_ordinate_id()
	 * @generated
	 */
	void setParent_axis_ordinate_id(AXIS_ORDINATE value);

	/**
	 * Returns the value of the '<em><b>Path</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Path</em>' attribute.
	 * @see #setPath(String)
	 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage#getAXIS_ORDINATE_Path()
	 * @model extendedMetaData="kind='attribute' name='path'"
	 * @generated
	 */
	String getPath();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE#getPath <em>Path</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Path</em>' attribute.
	 * @see #getPath()
	 * @generated
	 */
	void setPath(String value);

} // AXIS_ORDINATE
