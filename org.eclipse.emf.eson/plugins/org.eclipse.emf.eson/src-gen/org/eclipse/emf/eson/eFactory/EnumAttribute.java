/**
 */
package org.eclipse.emf.eson.eFactory;

import org.eclipse.emf.ecore.EEnumLiteral;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Enum Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.EnumAttribute#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getEnumAttribute()
 * @model
 * @generated
 */
public interface EnumAttribute extends Attribute
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
   * @see #setValue(EEnumLiteral)
   * @see org.eclipse.emf.eson.eFactory.EFactoryPackage#getEnumAttribute_Value()
   * @model
   * @generated
   */
  EEnumLiteral getValue();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.eFactory.EnumAttribute#getValue <em>Value</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Value</em>' reference.
   * @see #getValue()
   * @generated
   */
  void setValue(EEnumLiteral value);

} // EnumAttribute
