/**
 */
package org.eclipse.emf.eson.eFactory.impl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.eson.eFactory.CustomNameMapping;
import org.eclipse.emf.eson.eFactory.EFactoryPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Custom Name Mapping</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.impl.CustomNameMappingImpl#getEClass <em>EClass</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.impl.CustomNameMappingImpl#getNameFeature <em>Name Feature</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CustomNameMappingImpl extends AnnotationImpl implements CustomNameMapping
{
  /**
   * The cached value of the '{@link #getEClass() <em>EClass</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getEClass()
   * @generated
   * @ordered
   */
  protected EClass eClass;

  /**
   * The cached value of the '{@link #getNameFeature() <em>Name Feature</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getNameFeature()
   * @generated
   * @ordered
   */
  protected EAttribute nameFeature;

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  protected CustomNameMappingImpl()
  {
    super();
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  protected EClass eStaticClass()
  {
    return EFactoryPackage.Literals.CUSTOM_NAME_MAPPING;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EClass getEClass()
  {
    if (eClass != null && eClass.eIsProxy())
    {
      InternalEObject oldEClass = (InternalEObject)eClass;
      eClass = (EClass)eResolveProxy(oldEClass);
      if (eClass != oldEClass)
      {
        if (eNotificationRequired())
          eNotify(new ENotificationImpl(this, Notification.RESOLVE, EFactoryPackage.CUSTOM_NAME_MAPPING__ECLASS, oldEClass, eClass));
      }
    }
    return eClass;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EClass basicGetEClass()
  {
    return eClass;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public void setEClass(EClass newEClass)
  {
    EClass oldEClass = eClass;
    eClass = newEClass;
    if (eNotificationRequired())
      eNotify(new ENotificationImpl(this, Notification.SET, EFactoryPackage.CUSTOM_NAME_MAPPING__ECLASS, oldEClass, eClass));
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EAttribute getNameFeature()
  {
    if (nameFeature != null && nameFeature.eIsProxy())
    {
      InternalEObject oldNameFeature = (InternalEObject)nameFeature;
      nameFeature = (EAttribute)eResolveProxy(oldNameFeature);
      if (nameFeature != oldNameFeature)
      {
        if (eNotificationRequired())
          eNotify(new ENotificationImpl(this, Notification.RESOLVE, EFactoryPackage.CUSTOM_NAME_MAPPING__NAME_FEATURE, oldNameFeature, nameFeature));
      }
    }
    return nameFeature;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EAttribute basicGetNameFeature()
  {
    return nameFeature;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public void setNameFeature(EAttribute newNameFeature)
  {
    EAttribute oldNameFeature = nameFeature;
    nameFeature = newNameFeature;
    if (eNotificationRequired())
      eNotify(new ENotificationImpl(this, Notification.SET, EFactoryPackage.CUSTOM_NAME_MAPPING__NAME_FEATURE, oldNameFeature, nameFeature));
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public Object eGet(int featureID, boolean resolve, boolean coreType)
  {
    switch (featureID)
    {
      case EFactoryPackage.CUSTOM_NAME_MAPPING__ECLASS:
        if (resolve) return getEClass();
        return basicGetEClass();
      case EFactoryPackage.CUSTOM_NAME_MAPPING__NAME_FEATURE:
        if (resolve) return getNameFeature();
        return basicGetNameFeature();
    }
    return super.eGet(featureID, resolve, coreType);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public void eSet(int featureID, Object newValue)
  {
    switch (featureID)
    {
      case EFactoryPackage.CUSTOM_NAME_MAPPING__ECLASS:
        setEClass((EClass)newValue);
        return;
      case EFactoryPackage.CUSTOM_NAME_MAPPING__NAME_FEATURE:
        setNameFeature((EAttribute)newValue);
        return;
    }
    super.eSet(featureID, newValue);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public void eUnset(int featureID)
  {
    switch (featureID)
    {
      case EFactoryPackage.CUSTOM_NAME_MAPPING__ECLASS:
        setEClass((EClass)null);
        return;
      case EFactoryPackage.CUSTOM_NAME_MAPPING__NAME_FEATURE:
        setNameFeature((EAttribute)null);
        return;
    }
    super.eUnset(featureID);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public boolean eIsSet(int featureID)
  {
    switch (featureID)
    {
      case EFactoryPackage.CUSTOM_NAME_MAPPING__ECLASS:
        return eClass != null;
      case EFactoryPackage.CUSTOM_NAME_MAPPING__NAME_FEATURE:
        return nameFeature != null;
    }
    return super.eIsSet(featureID);
  }

} //CustomNameMappingImpl
