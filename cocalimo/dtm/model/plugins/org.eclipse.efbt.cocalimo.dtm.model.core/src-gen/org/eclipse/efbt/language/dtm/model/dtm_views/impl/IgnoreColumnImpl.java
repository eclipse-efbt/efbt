/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Ignore Column</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.IgnoreColumnImpl#getColumnToIgnore <em>Column To Ignore</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.IgnoreColumnImpl#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class IgnoreColumnImpl extends MinimalEObjectImpl.Container implements IgnoreColumn {
	/**
	 * The cached value of the '{@link #getColumnToIgnore() <em>Column To Ignore</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnToIgnore()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE columnToIgnore;

	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube cube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected IgnoreColumnImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_viewsPackage.Literals.IGNORE_COLUMN;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getColumnToIgnore() {
		if (columnToIgnore != null && columnToIgnore.eIsProxy()) {
			InternalEObject oldColumnToIgnore = (InternalEObject)columnToIgnore;
			columnToIgnore = (VARIABLE)eResolveProxy(oldColumnToIgnore);
			if (columnToIgnore != oldColumnToIgnore) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_viewsPackage.IGNORE_COLUMN__COLUMN_TO_IGNORE, oldColumnToIgnore, columnToIgnore));
			}
		}
		return columnToIgnore;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetColumnToIgnore() {
		return columnToIgnore;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setColumnToIgnore(VARIABLE newColumnToIgnore) {
		VARIABLE oldColumnToIgnore = columnToIgnore;
		columnToIgnore = newColumnToIgnore;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_viewsPackage.IGNORE_COLUMN__COLUMN_TO_IGNORE, oldColumnToIgnore, columnToIgnore));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getCube() {
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (FreeBirdToolsCube)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_viewsPackage.IGNORE_COLUMN__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetCube() {
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube(FreeBirdToolsCube newCube) {
		FreeBirdToolsCube oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_viewsPackage.IGNORE_COLUMN__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case dtm_viewsPackage.IGNORE_COLUMN__COLUMN_TO_IGNORE:
				if (resolve) return getColumnToIgnore();
				return basicGetColumnToIgnore();
			case dtm_viewsPackage.IGNORE_COLUMN__CUBE:
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
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case dtm_viewsPackage.IGNORE_COLUMN__COLUMN_TO_IGNORE:
				setColumnToIgnore((VARIABLE)newValue);
				return;
			case dtm_viewsPackage.IGNORE_COLUMN__CUBE:
				setCube((FreeBirdToolsCube)newValue);
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
			case dtm_viewsPackage.IGNORE_COLUMN__COLUMN_TO_IGNORE:
				setColumnToIgnore((VARIABLE)null);
				return;
			case dtm_viewsPackage.IGNORE_COLUMN__CUBE:
				setCube((FreeBirdToolsCube)null);
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
			case dtm_viewsPackage.IGNORE_COLUMN__COLUMN_TO_IGNORE:
				return columnToIgnore != null;
			case dtm_viewsPackage.IGNORE_COLUMN__CUBE:
				return cube != null;
		}
		return super.eIsSet(featureID);
	}

} //IgnoreColumnImpl
