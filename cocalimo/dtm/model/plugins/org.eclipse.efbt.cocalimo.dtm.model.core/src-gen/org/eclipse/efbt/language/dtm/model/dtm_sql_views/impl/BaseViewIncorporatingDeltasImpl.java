/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.BaseViewIncorporatingDeltas;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base View Incorporating Deltas</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.BaseViewIncorporatingDeltasImpl#getUltimateSourceCube <em>Ultimate Source Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseViewIncorporatingDeltasImpl extends SQLViewImpl implements BaseViewIncorporatingDeltas
{
	/**
	 * The cached value of the '{@link #getUltimateSourceCube() <em>Ultimate Source Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUltimateSourceCube()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube ultimateSourceCube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseViewIncorporatingDeltasImpl()
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
		return dtm_sql_viewsPackage.Literals.BASE_VIEW_INCORPORATING_DELTAS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getUltimateSourceCube()
	{
		if (ultimateSourceCube != null && ultimateSourceCube.eIsProxy()) {
			InternalEObject oldUltimateSourceCube = (InternalEObject)ultimateSourceCube;
			ultimateSourceCube = (FreeBirdToolsCube)eResolveProxy(oldUltimateSourceCube);
			if (ultimateSourceCube != oldUltimateSourceCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE, oldUltimateSourceCube, ultimateSourceCube));
			}
		}
		return ultimateSourceCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetUltimateSourceCube()
	{
		return ultimateSourceCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUltimateSourceCube(FreeBirdToolsCube newUltimateSourceCube)
	{
		FreeBirdToolsCube oldUltimateSourceCube = ultimateSourceCube;
		ultimateSourceCube = newUltimateSourceCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE, oldUltimateSourceCube, ultimateSourceCube));
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
			case dtm_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE:
				if (resolve) return getUltimateSourceCube();
				return basicGetUltimateSourceCube();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case dtm_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE:
				setUltimateSourceCube((FreeBirdToolsCube)newValue);
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
			case dtm_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE:
				setUltimateSourceCube((FreeBirdToolsCube)null);
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
			case dtm_sql_viewsPackage.BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE:
				return ultimateSourceCube != null;
		}
		return super.eIsSet(featureID);
	}

} //BaseViewIncorporatingDeltasImpl
