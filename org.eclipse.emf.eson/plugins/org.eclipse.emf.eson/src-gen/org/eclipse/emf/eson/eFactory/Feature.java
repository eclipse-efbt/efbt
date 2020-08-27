/**
 */
package org.eclipse.emf.eson.eFactory;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Feature</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.Feature#getEFeature <em>EFeature</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.Feature#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getFeature()
 * @model
 * @generated
 */
public interface Feature extends EObject
{
  /**
   * Returns the value of the '<em><b>EFeature</b></em>' reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>EFeature</em>' reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>EFeature</em>' reference.
   * @see #setEFeature(EStructuralFeature)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getFeature_EFeature()
   * @model
   * @generated
   */
  EStructuralFeature getEFeature();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.Feature#getEFeature <em>EFeature</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>EFeature</em>' reference.
   * @see #getEFeature()
   * @generated
   */
  void setEFeature(EStructuralFeature value);

  /**
   * Returns the value of the '<em><b>Value</b></em>' containment reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Value</em>' containment reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Value</em>' containment reference.
   * @see #setValue(Value)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getFeature_Value()
   * @model containment="true"
   * @generated
   */
  Value getValue();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.Feature#getValue <em>Value</em>}' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Value</em>' containment reference.
   * @see #getValue()
   * @generated
   */
  void setValue(Value value);

} // Feature
