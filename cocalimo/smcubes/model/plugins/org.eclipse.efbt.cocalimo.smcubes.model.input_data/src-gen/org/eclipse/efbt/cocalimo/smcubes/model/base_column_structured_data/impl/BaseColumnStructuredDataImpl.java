/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base Column Structured Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseColumnStructuredDataImpl#getRows <em>Rows</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseColumnStructuredDataImpl#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseColumnStructuredDataImpl extends ColumnStructuredDataImpl implements BaseColumnStructuredData
{
	/**
	 * The cached value of the '{@link #getRows() <em>Rows</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRows()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseRowData> rows;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseColumnStructuredDataImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Base_column_structured_dataPackage.Literals.BASE_COLUMN_STRUCTURED_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BaseRowData> getRows()
	{
		if (rows == null) {
			rows = new EObjectContainmentEList<BaseRowData>(BaseRowData.class, this, Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__ROWS);
		}
		return rows;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getCube()
	{
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (CUBE)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetCube()
	{
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube(CUBE newCube)
	{
		CUBE oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__ROWS:
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
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__ROWS:
				return getRows();
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__ROWS:
				getRows().clear();
				getRows().addAll((Collection<? extends BaseRowData>)newValue);
				return;
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__CUBE:
				setCube((CUBE)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__ROWS:
				getRows().clear();
				return;
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__CUBE:
				setCube((CUBE)null);
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__ROWS:
				return rows != null && !rows.isEmpty();
			case Base_column_structured_dataPackage.BASE_COLUMN_STRUCTURED_DATA__CUBE:
				return cube != null;
		}
		return super.eIsSet(featureID);
	}

} //BaseColumnStructuredDataImpl
