/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPrivateOperation;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPublicOperation;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Public Operation</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELPublicOperationImpl#getCalledPrivateOperations <em>Called Private Operations</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ELPublicOperationImpl extends ELOperationImpl implements ELPublicOperation {
	/**
	 * The cached value of the '{@link #getCalledPrivateOperations() <em>Called Private Operations</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCalledPrivateOperations()
	 * @generated
	 * @ordered
	 */
	protected EList<ELPrivateOperation> calledPrivateOperations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELPublicOperationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getELPublicOperation();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ELPrivateOperation> getCalledPrivateOperations() {
		if (calledPrivateOperations == null) {
			calledPrivateOperations = new EObjectResolvingEList<ELPrivateOperation>(ELPrivateOperation.class, this, Ecore4regPackage.EL_PUBLIC_OPERATION__CALLED_PRIVATE_OPERATIONS);
		}
		return calledPrivateOperations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.EL_PUBLIC_OPERATION__CALLED_PRIVATE_OPERATIONS:
				return getCalledPrivateOperations();
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
			case Ecore4regPackage.EL_PUBLIC_OPERATION__CALLED_PRIVATE_OPERATIONS:
				getCalledPrivateOperations().clear();
				getCalledPrivateOperations().addAll((Collection<? extends ELPrivateOperation>)newValue);
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
			case Ecore4regPackage.EL_PUBLIC_OPERATION__CALLED_PRIVATE_OPERATIONS:
				getCalledPrivateOperations().clear();
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
			case Ecore4regPackage.EL_PUBLIC_OPERATION__CALLED_PRIVATE_OPERATIONS:
				return calledPrivateOperations != null && !calledPrivateOperations.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ELPublicOperationImpl
