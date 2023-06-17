/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL For View Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewModuleImpl#getVTLForViews <em>VTL For Views</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLForViewModuleImpl extends ModuleImpl implements VTLForViewModule {
	/**
	 * The cached value of the '{@link #getVTLForViews() <em>VTL For Views</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLForViews()
	 * @generated
	 * @ordered
	 */
	protected EList<VTLForView> vTLForViews;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLForViewModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLForViewModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLForView> getVTLForViews() {
		if (vTLForViews == null) {
			vTLForViews = new EObjectContainmentEList<VTLForView>(VTLForView.class, this, Ecore4regPackage.VTL_FOR_VIEW_MODULE__VTL_FOR_VIEWS);
		}
		return vTLForViews;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_VIEW_MODULE__VTL_FOR_VIEWS:
				return ((InternalEList<?>)getVTLForViews()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.VTL_FOR_VIEW_MODULE__VTL_FOR_VIEWS:
				return getVTLForViews();
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
			case Ecore4regPackage.VTL_FOR_VIEW_MODULE__VTL_FOR_VIEWS:
				getVTLForViews().clear();
				getVTLForViews().addAll((Collection<? extends VTLForView>)newValue);
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
			case Ecore4regPackage.VTL_FOR_VIEW_MODULE__VTL_FOR_VIEWS:
				getVTLForViews().clear();
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
			case Ecore4regPackage.VTL_FOR_VIEW_MODULE__VTL_FOR_VIEWS:
				return vTLForViews != null && !vTLForViews.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VTLForViewModuleImpl
