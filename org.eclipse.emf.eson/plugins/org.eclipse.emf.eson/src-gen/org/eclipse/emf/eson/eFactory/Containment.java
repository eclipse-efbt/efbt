/**
 */
package org.eclipse.emf.eson.eFactory;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Containment</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.Containment#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getContainment()
 * @model
 * @generated
 */
public interface Containment extends Value
{
  /**
   * Returns the value of the '<em><b>Value</b></em>' containment reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Value</em>' containment reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Value</em>' containment reference.
   * @see #setValue(NewObject)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getContainment_Value()
   * @model containment="true"
   * @generated
   */
  NewObject getValue();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.Containment#getValue <em>Value</em>}' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Value</em>' containment reference.
   * @see #getValue()
   * @generated
   */
  void setValue(NewObject value);

} // Containment
