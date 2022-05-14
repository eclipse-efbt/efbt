/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.AxisOrdinateModule;
import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.openregspecs_smcubes_extra_extensionPackage;

import org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Axis Ordinate Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_extra_extension.impl.AxisOrdinateModuleImpl#getAxis_ordinates <em>Axis ordinates</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AxisOrdinateModuleImpl extends ModuleImpl implements AxisOrdinateModule {
	/**
	 * The cached value of the '{@link #getAxis_ordinates() <em>Axis ordinates</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_ordinates()
	 * @generated
	 * @ordered
	 */
	protected EList<AXIS_ORDINATE> axis_ordinates;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AxisOrdinateModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return openregspecs_smcubes_extra_extensionPackage.Literals.AXIS_ORDINATE_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<AXIS_ORDINATE> getAxis_ordinates() {
		if (axis_ordinates == null) {
			axis_ordinates = new EObjectContainmentEList<AXIS_ORDINATE>(AXIS_ORDINATE.class, this, openregspecs_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE__AXIS_ORDINATES);
		}
		return axis_ordinates;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case openregspecs_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE__AXIS_ORDINATES:
				return ((InternalEList<?>)getAxis_ordinates()).basicRemove(otherEnd, msgs);
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
			case openregspecs_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE__AXIS_ORDINATES:
				return getAxis_ordinates();
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
			case openregspecs_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE__AXIS_ORDINATES:
				getAxis_ordinates().clear();
				getAxis_ordinates().addAll((Collection<? extends AXIS_ORDINATE>)newValue);
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
			case openregspecs_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE__AXIS_ORDINATES:
				getAxis_ordinates().clear();
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
			case openregspecs_smcubes_extra_extensionPackage.AXIS_ORDINATE_MODULE__AXIS_ORDINATES:
				return axis_ordinates != null && !axis_ordinates.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //AxisOrdinateModuleImpl
