/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.ExplodeStructView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Explode Struct View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.ExplodeStructViewImpl#getColumnToUnpack <em>Column To Unpack</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ExplodeStructViewImpl extends SQLViewImpl implements ExplodeStructView
{
	/**
	 * The cached value of the '{@link #getColumnToUnpack() <em>Column To Unpack</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnToUnpack()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable columnToUnpack;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ExplodeStructViewImpl()
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
		return dtm_sql_viewsPackage.Literals.EXPLODE_STRUCT_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getColumnToUnpack()
	{
		if (columnToUnpack != null && columnToUnpack.eIsProxy()) {
			InternalEObject oldColumnToUnpack = (InternalEObject)columnToUnpack;
			columnToUnpack = (StructTypedVariable)eResolveProxy(oldColumnToUnpack);
			if (columnToUnpack != oldColumnToUnpack) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK, oldColumnToUnpack, columnToUnpack));
			}
		}
		return columnToUnpack;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetColumnToUnpack()
	{
		return columnToUnpack;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setColumnToUnpack(StructTypedVariable newColumnToUnpack)
	{
		StructTypedVariable oldColumnToUnpack = columnToUnpack;
		columnToUnpack = newColumnToUnpack;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK, oldColumnToUnpack, columnToUnpack));
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
			case dtm_sql_viewsPackage.EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK:
				if (resolve) return getColumnToUnpack();
				return basicGetColumnToUnpack();
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
			case dtm_sql_viewsPackage.EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK:
				setColumnToUnpack((StructTypedVariable)newValue);
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
			case dtm_sql_viewsPackage.EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK:
				setColumnToUnpack((StructTypedVariable)null);
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
			case dtm_sql_viewsPackage.EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK:
				return columnToUnpack != null;
		}
		return super.eIsSet(featureID);
	}

} //ExplodeStructViewImpl
