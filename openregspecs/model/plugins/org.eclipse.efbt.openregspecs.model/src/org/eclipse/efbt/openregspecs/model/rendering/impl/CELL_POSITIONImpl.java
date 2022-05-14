/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.impl;

import org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE;
import org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION;
import org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage;
import org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CELL POSITION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.CELL_POSITIONImpl#getAxis_ordinate_id <em>Axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.CELL_POSITIONImpl#getCell_id <em>Cell id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CELL_POSITIONImpl extends MinimalEObjectImpl.Container implements CELL_POSITION {
	/**
	 * The cached value of the '{@link #getAxis_ordinate_id() <em>Axis ordinate id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_ordinate_id()
	 * @generated
	 * @ordered
	 */
	protected AXIS_ORDINATE axis_ordinate_id;

	/**
	 * The cached value of the '{@link #getCell_id() <em>Cell id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCell_id()
	 * @generated
	 * @ordered
	 */
	protected TABLE_CELL cell_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CELL_POSITIONImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return RenderingPackage.Literals.CELL_POSITION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AXIS_ORDINATE getAxis_ordinate_id() {
		if (axis_ordinate_id != null && axis_ordinate_id.eIsProxy()) {
			InternalEObject oldAxis_ordinate_id = (InternalEObject)axis_ordinate_id;
			axis_ordinate_id = (AXIS_ORDINATE)eResolveProxy(oldAxis_ordinate_id);
			if (axis_ordinate_id != oldAxis_ordinate_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.CELL_POSITION__AXIS_ORDINATE_ID, oldAxis_ordinate_id, axis_ordinate_id));
			}
		}
		return axis_ordinate_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AXIS_ORDINATE basicGetAxis_ordinate_id() {
		return axis_ordinate_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAxis_ordinate_id(AXIS_ORDINATE newAxis_ordinate_id) {
		AXIS_ORDINATE oldAxis_ordinate_id = axis_ordinate_id;
		axis_ordinate_id = newAxis_ordinate_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.CELL_POSITION__AXIS_ORDINATE_ID, oldAxis_ordinate_id, axis_ordinate_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE_CELL getCell_id() {
		if (cell_id != null && cell_id.eIsProxy()) {
			InternalEObject oldCell_id = (InternalEObject)cell_id;
			cell_id = (TABLE_CELL)eResolveProxy(oldCell_id);
			if (cell_id != oldCell_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.CELL_POSITION__CELL_ID, oldCell_id, cell_id));
			}
		}
		return cell_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE_CELL basicGetCell_id() {
		return cell_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCell_id(TABLE_CELL newCell_id) {
		TABLE_CELL oldCell_id = cell_id;
		cell_id = newCell_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.CELL_POSITION__CELL_ID, oldCell_id, cell_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case RenderingPackage.CELL_POSITION__AXIS_ORDINATE_ID:
				if (resolve) return getAxis_ordinate_id();
				return basicGetAxis_ordinate_id();
			case RenderingPackage.CELL_POSITION__CELL_ID:
				if (resolve) return getCell_id();
				return basicGetCell_id();
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
			case RenderingPackage.CELL_POSITION__AXIS_ORDINATE_ID:
				setAxis_ordinate_id((AXIS_ORDINATE)newValue);
				return;
			case RenderingPackage.CELL_POSITION__CELL_ID:
				setCell_id((TABLE_CELL)newValue);
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
			case RenderingPackage.CELL_POSITION__AXIS_ORDINATE_ID:
				setAxis_ordinate_id((AXIS_ORDINATE)null);
				return;
			case RenderingPackage.CELL_POSITION__CELL_ID:
				setCell_id((TABLE_CELL)null);
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
			case RenderingPackage.CELL_POSITION__AXIS_ORDINATE_ID:
				return axis_ordinate_id != null;
			case RenderingPackage.CELL_POSITION__CELL_ID:
				return cell_id != null;
		}
		return super.eIsSet(featureID);
	}

} //CELL_POSITIONImpl
