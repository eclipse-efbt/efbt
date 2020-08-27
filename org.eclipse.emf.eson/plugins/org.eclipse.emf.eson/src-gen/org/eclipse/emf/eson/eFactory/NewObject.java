/**
 */
package org.eclipse.emf.eson.eFactory;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>New Object</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.NewObject#getEClass <em>EClass</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.NewObject#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.NewObject#getFeatures <em>Features</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getNewObject()
 * @model
 * @generated
 */
public interface NewObject extends Value
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
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getNewObject_EClass()
   * @model
   * @generated
   */
  EClass getEClass();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.NewObject#getEClass <em>EClass</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>EClass</em>' reference.
   * @see #getEClass()
   * @generated
   */
  void setEClass(EClass value);

  /**
   * Returns the value of the '<em><b>Name</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Name</em>' attribute isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Name</em>' attribute.
   * @see #setName(String)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getNewObject_Name()
   * @model
   * @generated
   */
  String getName();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.NewObject#getName <em>Name</em>}' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Name</em>' attribute.
   * @see #getName()
   * @generated
   */
  void setName(String value);

  /**
   * Returns the value of the '<em><b>Features</b></em>' containment reference list.
   * The list contents are of type {@link org.eclipse.emf.eson.eFactory.Feature}.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Features</em>' containment reference list isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Features</em>' containment reference list.
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getNewObject_Features()
   * @model containment="true"
   * @generated
   */
  EList<Feature> getFeatures();

} // NewObject
