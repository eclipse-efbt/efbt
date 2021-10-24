/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData;
import org.eclipse.efbt.cocalimo.smcubes.model.input_data.Input_dataPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.input_data.RowData;

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
 * An implementation of the model object '<em><b>Cube Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CubeDataImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CubeDataImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CubeDataImpl#getRows <em>Rows</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CubeDataImpl extends MinimalEObjectImpl.Container implements CubeData {
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
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected CUBE cube;

	/**
	 * The cached value of the '{@link #getRows() <em>Rows</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRows()
	 * @generated
	 * @ordered
	 */
	protected EList<RowData> rows;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CubeDataImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Input_dataPackage.Literals.CUBE_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Input_dataPackage.CUBE_DATA__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getCube() {
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (CUBE)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Input_dataPackage.CUBE_DATA__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetCube() {
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube(CUBE newCube) {
		CUBE oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Input_dataPackage.CUBE_DATA__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<RowData> getRows() {
		if (rows == null) {
			rows = new EObjectContainmentEList<RowData>(RowData.class, this, Input_dataPackage.CUBE_DATA__ROWS);
		}
		return rows;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Input_dataPackage.CUBE_DATA__ROWS:
				return ((InternalEList<?>)getRows()).basicRemove(otherEnd, msgs);
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
			case Input_dataPackage.CUBE_DATA__NAME:
				return getName();
			case Input_dataPackage.CUBE_DATA__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case Input_dataPackage.CUBE_DATA__ROWS:
				return getRows();
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
			case Input_dataPackage.CUBE_DATA__NAME:
				setName((String)newValue);
				return;
			case Input_dataPackage.CUBE_DATA__CUBE:
				setCube((CUBE)newValue);
				return;
			case Input_dataPackage.CUBE_DATA__ROWS:
				getRows().clear();
				getRows().addAll((Collection<? extends RowData>)newValue);
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
			case Input_dataPackage.CUBE_DATA__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Input_dataPackage.CUBE_DATA__CUBE:
				setCube((CUBE)null);
				return;
			case Input_dataPackage.CUBE_DATA__ROWS:
				getRows().clear();
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
			case Input_dataPackage.CUBE_DATA__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Input_dataPackage.CUBE_DATA__CUBE:
				return cube != null;
			case Input_dataPackage.CUBE_DATA__ROWS:
				return rows != null && !rows.isEmpty();
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
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //CubeDataImpl
