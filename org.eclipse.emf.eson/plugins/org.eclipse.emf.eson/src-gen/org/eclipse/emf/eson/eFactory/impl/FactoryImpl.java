/**
 */
package org.eclipse.emf.eson.eFactory.impl;

import java.util.Collection;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

import org.eclipse.emf.eson.eFactory.Annotation;
import org.eclipse.emf.eson.eFactory.EFactoryPackage;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.NamespaceImport;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.PackageImport;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Factory</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.eFactory.impl.FactoryImpl#getImports <em>Imports</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.impl.FactoryImpl#getEPackages <em>EPackages</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.impl.FactoryImpl#getAnnotations <em>Annotations</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.eFactory.impl.FactoryImpl#getRoot <em>Root</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FactoryImpl extends MinimalEObjectImpl.Container implements Factory
{
  /**
   * The cached value of the '{@link #getImports() <em>Imports</em>}' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getImports()
   * @generated
   * @ordered
   */
  protected EList<NamespaceImport> imports;

  /**
   * The cached value of the '{@link #getEPackages() <em>EPackages</em>}' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getEPackages()
   * @generated
   * @ordered
   */
  protected EList<PackageImport> ePackages;

  /**
   * The cached value of the '{@link #getAnnotations() <em>Annotations</em>}' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getAnnotations()
   * @generated
   * @ordered
   */
  protected EList<Annotation> annotations;

  /**
   * The cached value of the '{@link #getRoot() <em>Root</em>}' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see #getRoot()
   * @generated
   * @ordered
   */
  protected NewObject root;

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  protected FactoryImpl()
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
    return EFactoryPackage.Literals.FACTORY;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EList<NamespaceImport> getImports()
  {
    if (imports == null)
    {
      imports = new EObjectContainmentEList<NamespaceImport>(NamespaceImport.class, this, EFactoryPackage.FACTORY__IMPORTS);
    }
    return imports;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EList<PackageImport> getEPackages()
  {
    if (ePackages == null)
    {
      ePackages = new EObjectContainmentEList<PackageImport>(PackageImport.class, this, EFactoryPackage.FACTORY__EPACKAGES);
    }
    return ePackages;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EList<Annotation> getAnnotations()
  {
    if (annotations == null)
    {
      annotations = new EObjectContainmentEList<Annotation>(Annotation.class, this, EFactoryPackage.FACTORY__ANNOTATIONS);
    }
    return annotations;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public NewObject getRoot()
  {
    return root;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public NotificationChain basicSetRoot(NewObject newRoot, NotificationChain msgs)
  {
    NewObject oldRoot = root;
    root = newRoot;
    if (eNotificationRequired())
    {
      ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, EFactoryPackage.FACTORY__ROOT, oldRoot, newRoot);
      if (msgs == null) msgs = notification; else msgs.add(notification);
    }
    return msgs;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public void setRoot(NewObject newRoot)
  {
    if (newRoot != root)
    {
      NotificationChain msgs = null;
      if (root != null)
        msgs = ((InternalEObject)root).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - EFactoryPackage.FACTORY__ROOT, null, msgs);
      if (newRoot != null)
        msgs = ((InternalEObject)newRoot).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - EFactoryPackage.FACTORY__ROOT, null, msgs);
      msgs = basicSetRoot(newRoot, msgs);
      if (msgs != null) msgs.dispatch();
    }
    else if (eNotificationRequired())
      eNotify(new ENotificationImpl(this, Notification.SET, EFactoryPackage.FACTORY__ROOT, newRoot, newRoot));
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
      case EFactoryPackage.FACTORY__IMPORTS:
        return ((InternalEList<?>)getImports()).basicRemove(otherEnd, msgs);
      case EFactoryPackage.FACTORY__EPACKAGES:
        return ((InternalEList<?>)getEPackages()).basicRemove(otherEnd, msgs);
      case EFactoryPackage.FACTORY__ANNOTATIONS:
        return ((InternalEList<?>)getAnnotations()).basicRemove(otherEnd, msgs);
      case EFactoryPackage.FACTORY__ROOT:
        return basicSetRoot(null, msgs);
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
      case EFactoryPackage.FACTORY__IMPORTS:
        return getImports();
      case EFactoryPackage.FACTORY__EPACKAGES:
        return getEPackages();
      case EFactoryPackage.FACTORY__ANNOTATIONS:
        return getAnnotations();
      case EFactoryPackage.FACTORY__ROOT:
        return getRoot();
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
      case EFactoryPackage.FACTORY__IMPORTS:
        getImports().clear();
        getImports().addAll((Collection<? extends NamespaceImport>)newValue);
        return;
      case EFactoryPackage.FACTORY__EPACKAGES:
        getEPackages().clear();
        getEPackages().addAll((Collection<? extends PackageImport>)newValue);
        return;
      case EFactoryPackage.FACTORY__ANNOTATIONS:
        getAnnotations().clear();
        getAnnotations().addAll((Collection<? extends Annotation>)newValue);
        return;
      case EFactoryPackage.FACTORY__ROOT:
        setRoot((NewObject)newValue);
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
      case EFactoryPackage.FACTORY__IMPORTS:
        getImports().clear();
        return;
      case EFactoryPackage.FACTORY__EPACKAGES:
        getEPackages().clear();
        return;
      case EFactoryPackage.FACTORY__ANNOTATIONS:
        getAnnotations().clear();
        return;
      case EFactoryPackage.FACTORY__ROOT:
        setRoot((NewObject)null);
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
      case EFactoryPackage.FACTORY__IMPORTS:
        return imports != null && !imports.isEmpty();
      case EFactoryPackage.FACTORY__EPACKAGES:
        return ePackages != null && !ePackages.isEmpty();
      case EFactoryPackage.FACTORY__ANNOTATIONS:
        return annotations != null && !annotations.isEmpty();
      case EFactoryPackage.FACTORY__ROOT:
        return root != null;
    }
    return super.eIsSet(featureID);
  }

} //FactoryImpl
