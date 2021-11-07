/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Row Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.RowDataImpl#getRowID <em>Row ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.RowDataImpl#getCells <em>Cells</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RowDataImpl extends MinimalEObjectImpl.Container implements RowData {
	/**
	 * The default value of the '{@link #getRowID() <em>Row ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowID()
	 * @generated
	 * @ordered
	 */
	protected static final String ROW_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getRowID() <em>Row ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowID()
	 * @generated
	 * @ordered
	 */
	protected String rowID = ROW_ID_EDEFAULT;

	/**
	 * The cached value of the '{@link #getCells() <em>Cells</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCells()
	 * @generated
	 * @ordered
	 */
	protected EList<Cell> cells;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RowDataImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestingPackage.Literals.ROW_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getRowID() {
		return rowID;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRowID(String newRowID) {
		String oldRowID = rowID;
		rowID = newRowID;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.ROW_DATA__ROW_ID, oldRowID, rowID));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Cell> getCells() {
		if (cells == null) {
			cells = new EObjectContainmentEList<Cell>(Cell.class, this, TestingPackage.ROW_DATA__CELLS);
		}
		return cells;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestingPackage.ROW_DATA__CELLS:
				return ((InternalEList<?>)getCells()).basicRemove(otherEnd, msgs);
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
			case TestingPackage.ROW_DATA__ROW_ID:
				return getRowID();
			case TestingPackage.ROW_DATA__CELLS:
				return getCells();
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
			case TestingPackage.ROW_DATA__ROW_ID:
				setRowID((String)newValue);
				return;
			case TestingPackage.ROW_DATA__CELLS:
				getCells().clear();
				getCells().addAll((Collection<? extends Cell>)newValue);
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
			case TestingPackage.ROW_DATA__ROW_ID:
				setRowID(ROW_ID_EDEFAULT);
				return;
			case TestingPackage.ROW_DATA__CELLS:
				getCells().clear();
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
			case TestingPackage.ROW_DATA__ROW_ID:
				return ROW_ID_EDEFAULT == null ? rowID != null : !ROW_ID_EDEFAULT.equals(rowID);
			case TestingPackage.ROW_DATA__CELLS:
				return cells != null && !cells.isEmpty();
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
		result.append(" (rowID: ");
		result.append(rowID);
		result.append(')');
		return result.toString();
	}

} //RowDataImpl
