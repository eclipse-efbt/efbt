/**
 */
package org.eclipse.emf.eson.eFactory;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Custom Name Mapping</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.CustomNameMapping#getEClass <em>EClass</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.CustomNameMapping#getNameFeature <em>Name Feature</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getCustomNameMapping()
 * @model
 * @generated
 */
public interface CustomNameMapping extends Annotation
{
  /**
   * Returns the value of the '<em><b>EClass</b></em>' reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>EClass</em>' reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>EClass</em>' reference.
   * @see #setEClass(EClass)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getCustomNameMapping_EClass()
   * @model
   * @generated
   */
  EClass getEClass();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.CustomNameMapping#getEClass <em>EClass</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>EClass</em>' reference.
   * @see #getEClass()
   * @generated
   */
  void setEClass(EClass value);

  /**
   * Returns the value of the '<em><b>Name Feature</b></em>' reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Name Feature</em>' reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Name Feature</em>' reference.
   * @see #setNameFeature(EAttribute)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getCustomNameMapping_NameFeature()
   * @model
   * @generated
   */
  EAttribute getNameFeature();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.CustomNameMapping#getNameFeature <em>Name Feature</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Name Feature</em>' reference.
   * @see #getNameFeature()
   * @generated
   */
  void setNameFeature(EAttribute value);

} // CustomNameMapping
