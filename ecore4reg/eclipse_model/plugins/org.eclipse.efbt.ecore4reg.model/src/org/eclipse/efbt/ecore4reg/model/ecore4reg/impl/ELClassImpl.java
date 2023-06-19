/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStructuralFeature;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Class</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassImpl#isEAbstract <em>EAbstract</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassImpl#getESuperTypes <em>ESuper Types</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassImpl#getEStructuralFeatures <em>EStructural Features</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassImpl#getEOperations <em>EOperations</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ELClassImpl extends ELClassifierImpl implements ELClass {
	/**
	 * The default value of the '{@link #isEAbstract() <em>EAbstract</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isEAbstract()
	 * @generated
	 * @ordered
	 */
	protected static final boolean EABSTRACT_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isEAbstract() <em>EAbstract</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isEAbstract()
	 * @generated
	 * @ordered
	 */
	protected boolean eAbstract = EABSTRACT_EDEFAULT;

	/**
	 * The cached value of the '{@link #getESuperTypes() <em>ESuper Types</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getESuperTypes()
	 * @generated
	 * @ordered
	 */
	protected EList<ELClass> eSuperTypes;

	/**
	 * The cached value of the '{@link #getEStructuralFeatures() <em>EStructural Features</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEStructuralFeatures()
	 * @generated
	 * @ordered
	 */
	protected EList<ELStructuralFeature> eStructuralFeatures;

	/**
	 * The cached value of the '{@link #getEOperations() <em>EOperations</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEOperations()
	 * @generated
	 * @ordered
	 */
	protected EList<ELOperation> eOperations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELClassImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getELClass();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isEAbstract() {
		return eAbstract;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEAbstract(boolean newEAbstract) {
		boolean oldEAbstract = eAbstract;
		eAbstract = newEAbstract;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.EL_CLASS__EABSTRACT, oldEAbstract, eAbstract));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ELClass> getESuperTypes() {
		if (eSuperTypes == null) {
			eSuperTypes = new EObjectResolvingEList.Unsettable<ELClass>(ELClass.class, this, Ecore4regPackage.EL_CLASS__ESUPER_TYPES);
		}
		return eSuperTypes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void unsetESuperTypes() {
		if (eSuperTypes != null) ((InternalEList.Unsettable<?>)eSuperTypes).unset();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isSetESuperTypes() {
		return eSuperTypes != null && ((InternalEList.Unsettable<?>)eSuperTypes).isSet();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ELStructuralFeature> getEStructuralFeatures() {
		if (eStructuralFeatures == null) {
			eStructuralFeatures = new EObjectContainmentEList<ELStructuralFeature>(ELStructuralFeature.class, this, Ecore4regPackage.EL_CLASS__ESTRUCTURAL_FEATURES);
		}
		return eStructuralFeatures;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ELOperation> getEOperations() {
		if (eOperations == null) {
			eOperations = new EObjectContainmentEList<ELOperation>(ELOperation.class, this, Ecore4regPackage.EL_CLASS__EOPERATIONS);
		}
		return eOperations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASS__ESTRUCTURAL_FEATURES:
				return ((InternalEList<?>)getEStructuralFeatures()).basicRemove(otherEnd, msgs);
			case Ecore4regPackage.EL_CLASS__EOPERATIONS:
				return ((InternalEList<?>)getEOperations()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASS__EABSTRACT:
				return isEAbstract();
			case Ecore4regPackage.EL_CLASS__ESUPER_TYPES:
				return getESuperTypes();
			case Ecore4regPackage.EL_CLASS__ESTRUCTURAL_FEATURES:
				return getEStructuralFeatures();
			case Ecore4regPackage.EL_CLASS__EOPERATIONS:
				return getEOperations();
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
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASS__EABSTRACT:
				setEAbstract((Boolean)newValue);
				return;
			case Ecore4regPackage.EL_CLASS__ESUPER_TYPES:
				getESuperTypes().clear();
				getESuperTypes().addAll((Collection<? extends ELClass>)newValue);
				return;
			case Ecore4regPackage.EL_CLASS__ESTRUCTURAL_FEATURES:
				getEStructuralFeatures().clear();
				getEStructuralFeatures().addAll((Collection<? extends ELStructuralFeature>)newValue);
				return;
			case Ecore4regPackage.EL_CLASS__EOPERATIONS:
				getEOperations().clear();
				getEOperations().addAll((Collection<? extends ELOperation>)newValue);
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
	public void eUnset(int featureID) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASS__EABSTRACT:
				setEAbstract(EABSTRACT_EDEFAULT);
				return;
			case Ecore4regPackage.EL_CLASS__ESUPER_TYPES:
				unsetESuperTypes();
				return;
			case Ecore4regPackage.EL_CLASS__ESTRUCTURAL_FEATURES:
				getEStructuralFeatures().clear();
				return;
			case Ecore4regPackage.EL_CLASS__EOPERATIONS:
				getEOperations().clear();
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
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASS__EABSTRACT:
				return eAbstract != EABSTRACT_EDEFAULT;
			case Ecore4regPackage.EL_CLASS__ESUPER_TYPES:
				return isSetESuperTypes();
			case Ecore4regPackage.EL_CLASS__ESTRUCTURAL_FEATURES:
				return eStructuralFeatures != null && !eStructuralFeatures.isEmpty();
			case Ecore4regPackage.EL_CLASS__EOPERATIONS:
				return eOperations != null && !eOperations.isEmpty();
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (eAbstract: ");
		result.append(eAbstract);
		result.append(')');
		return result.toString();
	}

} //ELClassImpl
