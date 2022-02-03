/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Get Attribute From Entity Derived Feature</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This is a ColumnFunction which represents the retrieval of a the value of a single Variable that exists inside  a CubeColumn that contains a StructTypeVariable.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getAttributeInEntity <em>Attribute In Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getEntityFeature <em>Entity Feature</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGetAttributeFromEntityDerivedFeature()
 * @model
 * @generated
 */
public interface GetAttributeFromEntityDerivedFeature extends DerivedAttribute {
	/**
	 * Returns the value of the '<em><b>Attribute In Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Variable that exists inside  a CubeColumn that contains a StructTypeVariable.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Attribute In Entity</em>' reference.
	 * @see #setAttributeInEntity(EAttribute)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGetAttributeFromEntityDerivedFeature_AttributeInEntity()
	 * @model
	 * @generated
	 */
	EAttribute getAttributeInEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getAttributeInEntity <em>Attribute In Entity</em>}' reference.
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
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getGetAttributeFromEntityDerivedFeature_EntityFeature()
	 * @model
	 * @generated
	 */
	EStructuralFeature getEntityFeature();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getEntityFeature <em>Entity Feature</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity Feature</em>' reference.
	 * @see #getEntityFeature()
	 * @generated
	 */
	void setEntityFeature(EStructuralFeature value);

} // GetAttributeFromEntityDerivedFeature
