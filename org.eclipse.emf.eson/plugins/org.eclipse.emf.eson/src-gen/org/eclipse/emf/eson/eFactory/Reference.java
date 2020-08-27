/**
 */
package org.eclipse.emf.eson.eFactory;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Reference</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.Reference#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getReference()
 * @model
 * @generated
 */
public interface Reference extends Value
{
  /**
   * Returns the value of the '<em><b>Value</b></em>' reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Value</em>' reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Value</em>' reference.
   * @see #setValue(EObject)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getReference_Value()
   * @model
   * @generated
   */
  EObject getValue();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.Reference#getValue <em>Value</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Value</em>' reference.
   * @see #getValue()
   * @generated
   */
  void setValue(EObject value);

} // Reference
