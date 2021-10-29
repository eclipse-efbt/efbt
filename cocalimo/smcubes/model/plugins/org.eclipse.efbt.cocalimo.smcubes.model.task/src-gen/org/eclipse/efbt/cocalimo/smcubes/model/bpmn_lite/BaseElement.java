/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Element</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.BaseElement#getId <em>Id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.BaseElement#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.BaseElement#isInvisible <em>Invisible</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getBaseElement()
 * @model abstract="true"
 * @generated
 */
public interface BaseElement extends EObject {
	/**
	 * Returns the value of the '<em><b>Id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Id</em>' attribute.
	 * @see #setId(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getBaseElement_Id()
	 * @model id="true" required="true" ordered="false"
	 * @generated
	 */
	String getId();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.BaseElement#getId <em>Id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Id</em>' attribute.
	 * @see #getId()
	 * @generated
	 */
	void setId(String value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getBaseElement_Description()
	 * @model
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.BaseElement#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Invisible</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Invisible</em>' attribute.
	 * @see #setInvisible(boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getBaseElement_Invisible()
	 * @model
	 * @generated
	 */
	boolean isInvisible();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.BaseElement#isInvisible <em>Invisible</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Invisible</em>' attribute.
	 * @see #isInvisible()
	 * @generated
	 */
	void setInvisible(boolean value);

} // BaseElement
