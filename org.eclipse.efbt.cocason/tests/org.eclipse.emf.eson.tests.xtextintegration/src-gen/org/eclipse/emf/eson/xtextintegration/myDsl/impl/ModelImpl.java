/**
 */
package org.eclipse.emf.eson.xtextintegration.myDsl.impl;

import java.util.Collection;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

import org.eclipse.emf.eson.xtextintegration.myDsl.Greeting;
import org.eclipse.emf.eson.xtextintegration.myDsl.Model;
import org.eclipse.emf.eson.xtextintegration.myDsl.MyDslPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.impl.ModelImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.impl.ModelImpl#getRefModel <em>Ref Model</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.impl.ModelImpl#getRefModels <em>Ref Models</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.impl.ModelImpl#getGreetings <em>Greetings</em>}</li>
 * </ul>
 * </p>
 *
 * @generated
 */
public class ModelImpl extends MinimalEObjectImpl.Container implements Model
{
  /**
   * The default value of the '{@link #getName() <em>Name</em>}' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getName()
   * @generated
   * @ordered
   */
  protected static final String NAME_EDEFAULT = null;

  /**
   * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getName()
   * @generated
   * @ordered
   */
  protected String name = NAME_EDEFAULT;

  /**
   * The cached value of the '{@link #getRefModel() <em>Ref Model</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getRefModel()
   * @generated
   * @ordered
   */
  protected Model refModel;

  /**
   * The cached value of the '{@link #getRefModels() <em>Ref Models</em>}' reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getRefModels()
   * @generated
   * @ordered
   */
  protected EList<Model> refModels;

  /**
   * The cached value of the '{@link #getGreetings() <em>Greetings</em>}' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getGreetings()
   * @generated
   * @ordered
   */
  protected EList<Greeting> greetings;

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  protected ModelImpl()
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
    return MyDslPackage.Literals.MODEL;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public String getName()
  {
    return name;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public void setName(String newName)
  {
    String oldName = name;
    name = newName;
    if (eNotificationRequired())
      eNotify(new ENotificationImpl(this, Notification.SET, MyDslPackage.MODEL__NAME, oldName, name));
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Model getRefModel()
  {
    if (refModel != null && refModel.eIsProxy())
    {
      InternalEObject oldRefModel = (InternalEObject)refModel;
      refModel = (Model)eResolveProxy(oldRefModel);
      if (refModel != oldRefModel)
      {
        if (eNotificationRequired())
          eNotify(new ENotificationImpl(this, Notification.RESOLVE, MyDslPackage.MODEL__REF_MODEL, oldRefModel, refModel));
      }
    }
    return refModel;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Model basicGetRefModel()
  {
    return refModel;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public void setRefModel(Model newRefModel)
  {
    Model oldRefModel = refModel;
    refModel = newRefModel;
    if (eNotificationRequired())
      eNotify(new ENotificationImpl(this, Notification.SET, MyDslPackage.MODEL__REF_MODEL, oldRefModel, refModel));
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EList<Model> getRefModels()
  {
    if (refModels == null)
    {
      refModels = new EObjectResolvingEList<Model>(Model.class, this, MyDslPackage.MODEL__REF_MODELS);
    }
    return refModels;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EList<Greeting> getGreetings()
  {
    if (greetings == null)
    {
      greetings = new EObjectContainmentEList<Greeting>(Greeting.class, this, MyDslPackage.MODEL__GREETINGS);
    }
    return greetings;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
  {
    switch (featureID)
    {
      case MyDslPackage.MODEL__GREETINGS:
        return ((InternalEList<?>)getGreetings()).basicRemove(otherEnd, msgs);
    }
    return super.eInverseRemove(otherEnd, featureID, msgs);
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
      case MyDslPackage.MODEL__NAME:
        return getName();
      case MyDslPackage.MODEL__REF_MODEL:
        if (resolve) return getRefModel();
        return basicGetRefModel();
      case MyDslPackage.MODEL__REF_MODELS:
        return getRefModels();
      case MyDslPackage.MODEL__GREETINGS:
        return getGreetings();
    }
    return super.eGet(featureID, resolve, coreType);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @SuppressWarnings("unchecked")
  @Override
  public void eSet(int featureID, Object newValue)
  {
    switch (featureID)
    {
      case MyDslPackage.MODEL__NAME:
        setName((String)newValue);
        return;
      case MyDslPackage.MODEL__REF_MODEL:
        setRefModel((Model)newValue);
        return;
      case MyDslPackage.MODEL__REF_MODELS:
        getRefModels().clear();
        getRefModels().addAll((Collection<? extends Model>)newValue);
        return;
      case MyDslPackage.MODEL__GREETINGS:
        getGreetings().clear();
        getGreetings().addAll((Collection<? extends Greeting>)newValue);
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
      case MyDslPackage.MODEL__NAME:
        setName(NAME_EDEFAULT);
        return;
      case MyDslPackage.MODEL__REF_MODEL:
        setRefModel((Model)null);
        return;
      case MyDslPackage.MODEL__REF_MODELS:
        getRefModels().clear();
        return;
      case MyDslPackage.MODEL__GREETINGS:
        getGreetings().clear();
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
      case MyDslPackage.MODEL__NAME:
        return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
      case MyDslPackage.MODEL__REF_MODEL:
        return refModel != null;
      case MyDslPackage.MODEL__REF_MODELS:
        return refModels != null && !refModels.isEmpty();
      case MyDslPackage.MODEL__GREETINGS:
        return greetings != null && !greetings.isEmpty();
    }
    return super.eIsSet(featureID);
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public String toString()
  {
    if (eIsProxy()) return super.toString();

    StringBuffer result = new StringBuffer(super.toString());
    result.append(" (name: ");
    result.append(name);
    result.append(')');
    return result.toString();
  }

} //ModelImpl
