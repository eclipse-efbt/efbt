/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData;
import org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData;
import org.eclipse.efbt.cocalimo.smcubes.model.input_data.Input_dataPackage;

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
 * An implementation of the model object '<em><b>Input Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.InputDataImpl#getSourceCubeData <em>Source Cube Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.InputDataImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class InputDataImpl extends MinimalEObjectImpl.Container implements InputData {
	/**
	 * The cached value of the '{@link #getSourceCubeData() <em>Source Cube Data</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceCubeData()
	 * @generated
	 * @ordered
	 */
	protected EList<CubeData> sourceCubeData;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected InputDataImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Input_dataPackage.Literals.INPUT_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<CubeData> getSourceCubeData() {
		if (sourceCubeData == null) {
			sourceCubeData = new EObjectContainmentEList<CubeData>(CubeData.class, this, Input_dataPackage.INPUT_DATA__SOURCE_CUBE_DATA);
		}
		return sourceCubeData;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Input_dataPackage.INPUT_DATA__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Input_dataPackage.INPUT_DATA__SOURCE_CUBE_DATA:
				return ((InternalEList<?>)getSourceCubeData()).basicRemove(otherEnd, msgs);
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
			case Input_dataPackage.INPUT_DATA__SOURCE_CUBE_DATA:
				return getSourceCubeData();
			case Input_dataPackage.INPUT_DATA__NAME:
				return getName();
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
			case Input_dataPackage.INPUT_DATA__SOURCE_CUBE_DATA:
				getSourceCubeData().clear();
				getSourceCubeData().addAll((Collection<? extends CubeData>)newValue);
				return;
			case Input_dataPackage.INPUT_DATA__NAME:
				setName((String)newValue);
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
			case Input_dataPackage.INPUT_DATA__SOURCE_CUBE_DATA:
				getSourceCubeData().clear();
				return;
			case Input_dataPackage.INPUT_DATA__NAME:
				setName(NAME_EDEFAULT);
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
			case Input_dataPackage.INPUT_DATA__SOURCE_CUBE_DATA:
				return sourceCubeData != null && !sourceCubeData.isEmpty();
			case Input_dataPackage.INPUT_DATA__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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

} //InputDataImpl
