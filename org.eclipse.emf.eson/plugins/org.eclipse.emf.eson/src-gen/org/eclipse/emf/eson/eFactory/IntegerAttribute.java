/**
 */
package org.eclipse.emf.eson.eFactory;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Integer Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.IntegerAttribute#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getIntegerAttribute()
 * @model
 * @generated
 */
public interface IntegerAttribute extends Attribute
{
  /**
   * Returns the value of the '<em><b>Value</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Value</em>' attribute isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Value</em>' attribute.
   * @see #setValue(long)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getIntegerAttribute_Value()
   * @model
   * @generated
   */
  long getValue();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.IntegerAttribute#getValue <em>Value</em>}' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Value</em>' attribute.
   * @see #getValue()
   * @generated
   */
  void setValue(long value);

} // IntegerAttribute
