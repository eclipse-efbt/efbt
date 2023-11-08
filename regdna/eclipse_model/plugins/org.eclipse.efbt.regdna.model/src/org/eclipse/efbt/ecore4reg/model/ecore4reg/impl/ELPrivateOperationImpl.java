/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.ELParameter;
import org.eclipse.efbt.regdna.model.regdna.ELPrivateOperation;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Private Operation</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELPrivateOperationImpl#getEParameters <em>EParameters</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ELPrivateOperationImpl extends ELOperationImpl implements ELPrivateOperation {
	/**
	 * The cached value of the '{@link #getEParameters() <em>EParameters</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEParameters()
	 * @generated
	 * @ordered
	 */
	protected EList<ELParameter> eParameters;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELPrivateOperationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getELPrivateOperation();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ELParameter> getEParameters() {
		if (eParameters == null) {
			eParameters = new EObjectContainmentEList<ELParameter>(ELParameter.class, this, regdnaPackage.EL_PRIVATE_OPERATION__EPARAMETERS);
		}
		return eParameters;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.EL_PRIVATE_OPERATION__EPARAMETERS:
				return ((InternalEList<?>)getEParameters()).basicRemove(otherEnd, msgs);
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
			case regdnaPackage.EL_PRIVATE_OPERATION__EPARAMETERS:
				return getEParameters();
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
			case regdnaPackage.EL_PRIVATE_OPERATION__EPARAMETERS:
				getEParameters().clear();
				getEParameters().addAll((Collection<? extends ELParameter>)newValue);
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
			case regdnaPackage.EL_PRIVATE_OPERATION__EPARAMETERS:
				getEParameters().clear();
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
			case regdnaPackage.EL_PRIVATE_OPERATION__EPARAMETERS:
				return eParameters != null && !eParameters.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ELPrivateOperationImpl
