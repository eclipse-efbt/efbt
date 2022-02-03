/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EStructuralFeature;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute From Entity Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Resolved parameter, which relates to a variable within a resolved CubeColumn, where the CubeColumn holds a StructTypeVaraible 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getAttributeInEntity <em>Attribute In Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getEntityFeature <em>Entity Feature</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getAttributeFromEntityParameter()
 * @model
 * @generated
 */
public interface AttributeFromEntityParameter extends EParameter {
	/**
	 * Returns the value of the '<em><b>Attribute In Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Variable that exists inside  a CubeColumn that contains a StructTypeVariable.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Attribute In Entity</em>' reference.
	 * @see #setAttributeInEntity(EAttribute)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getAttributeFromEntityParameter_AttributeInEntity()
	 * @model
	 * @generated
	 */
	EAttribute getAttributeInEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getAttributeInEntity <em>Attribute In Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attribute In Entity</em>' reference.
	 * @see #getAttributeInEntity()
	 * @generated
	 */
	void setAttributeInEntity(EAttribute value);

	/**
	 * Returns the value of the '<em><b>Entity Feature</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn that contains a StructTypeVariable.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Entity Feature</em>' reference.
	 * @see #setEntityFeature(EStructuralFeature)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getAttributeFromEntityParameter_EntityFeature()
	 * @model
	 * @generated
	 */
	EStructuralFeature getEntityFeature();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getEntityFeature <em>Entity Feature</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity Feature</em>' reference.
	 * @see #getEntityFeature()
	 * @generated
	 */
	void setEntityFeature(EStructuralFeature value);

} // AttributeFromEntityParameter
