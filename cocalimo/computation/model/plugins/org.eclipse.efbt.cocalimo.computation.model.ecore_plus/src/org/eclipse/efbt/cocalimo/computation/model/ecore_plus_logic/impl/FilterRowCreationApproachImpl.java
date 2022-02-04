/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Filter Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterRowCreationApproachImpl#getFilterFunction <em>Filter Function</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FilterRowCreationApproachImpl extends RowCreationApproachImpl implements FilterRowCreationApproach {
	/**
	 * The cached value of the '{@link #getFilterFunction() <em>Filter Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFilterFunction()
	 * @generated
	 * @ordered
	 */
	protected BooleanOperation filterFunction;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FilterRowCreationApproachImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.FILTER_ROW_CREATION_APPROACH;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BooleanOperation getFilterFunction() {
		return filterFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetFilterFunction(BooleanOperation newFilterFunction, NotificationChain msgs) {
		BooleanOperation oldFilterFunction = filterFunction;
		filterFunction = newFilterFunction;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION, oldFilterFunction, newFilterFunction);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFilterFunction(BooleanOperation newFilterFunction) {
		if (newFilterFunction != filterFunction) {
			NotificationChain msgs = null;
			if (filterFunction != null)
				msgs = ((InternalEObject)filterFunction).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION, null, msgs);
			if (newFilterFunction != null)
				msgs = ((InternalEObject)newFilterFunction).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION, null, msgs);
			msgs = basicSetFilterFunction(newFilterFunction, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION, newFilterFunction, newFilterFunction));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				return basicSetFilterFunction(null, msgs);
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
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				return getFilterFunction();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				setFilterFunction((BooleanOperation)newValue);
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
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				setFilterFunction((BooleanOperation)null);
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
			case Ecore_plus_logicPackage.FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION:
				return filterFunction != null;
		}
		return super.eIsSet(featureID);
	}

} //FilterRowCreationApproachImpl
