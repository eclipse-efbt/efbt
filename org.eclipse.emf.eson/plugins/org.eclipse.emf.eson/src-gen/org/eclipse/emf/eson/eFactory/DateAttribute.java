/**
 */
package org.eclipse.emf.eson.eFactory;

import java.util.Date;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Date Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.DateAttribute#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getDateAttribute()
 * @model
 * @generated
 */
public interface DateAttribute extends Attribute
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
   * @see #setValue(Date)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getDateAttribute_Value()
   * @model
   * @generated
   */
  Date getValue();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.DateAttribute#getValue <em>Value</em>}' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Value</em>' attribute.
   * @see #getValue()
   * @generated
   */
  void setValue(Date value);

} // DateAttribute
