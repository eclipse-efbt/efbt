/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Filter By Struct Class Column View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.FilterByStructClassColumnViewImpl#getRenameAsStructColumn <em>Rename As Struct Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.FilterByStructClassColumnViewImpl#getStructClass <em>Struct Class</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.FilterByStructClassColumnViewImpl#getStructColumn <em>Struct Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FilterByStructClassColumnViewImpl extends SQLViewImpl implements FilterByStructClassColumnView
{
	/**
	 * The cached value of the '{@link #getRenameAsStructColumn() <em>Rename As Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRenameAsStructColumn()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable renameAsStructColumn;

	/**
	 * The cached value of the '{@link #getStructClass() <em>Struct Class</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStructClass()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube structClass;

	/**
	 * The cached value of the '{@link #getStructColumn() <em>Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStructColumn()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable structColumn;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FilterByStructClassColumnViewImpl()
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
		return dtm_sql_viewsPackage.Literals.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getRenameAsStructColumn()
	{
		if (renameAsStructColumn != null && renameAsStructColumn.eIsProxy()) {
			InternalEObject oldRenameAsStructColumn = (InternalEObject)renameAsStructColumn;
			renameAsStructColumn = (StructTypedVariable)eResolveProxy(oldRenameAsStructColumn);
			if (renameAsStructColumn != oldRenameAsStructColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN, oldRenameAsStructColumn, renameAsStructColumn));
			}
		}
		return renameAsStructColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetRenameAsStructColumn()
	{
		return renameAsStructColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRenameAsStructColumn(StructTypedVariable newRenameAsStructColumn)
	{
		StructTypedVariable oldRenameAsStructColumn = renameAsStructColumn;
		renameAsStructColumn = newRenameAsStructColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN, oldRenameAsStructColumn, renameAsStructColumn));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getStructClass()
	{
		if (structClass != null && structClass.eIsProxy()) {
			InternalEObject oldStructClass = (InternalEObject)structClass;
			structClass = (FreeBirdToolsCube)eResolveProxy(oldStructClass);
			if (structClass != oldStructClass) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS, oldStructClass, structClass));
			}
		}
		return structClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetStructClass()
	{
		return structClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setStructClass(FreeBirdToolsCube newStructClass)
	{
		FreeBirdToolsCube oldStructClass = structClass;
		structClass = newStructClass;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS, oldStructClass, structClass));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getStructColumn()
	{
		if (structColumn != null && structColumn.eIsProxy()) {
			InternalEObject oldStructColumn = (InternalEObject)structColumn;
			structColumn = (StructTypedVariable)eResolveProxy(oldStructColumn);
			if (structColumn != oldStructColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN, oldStructColumn, structColumn));
			}
		}
		return structColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetStructColumn()
	{
		return structColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setStructColumn(StructTypedVariable newStructColumn)
	{
		StructTypedVariable oldStructColumn = structColumn;
		structColumn = newStructColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN, oldStructColumn, structColumn));
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
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN:
				if (resolve) return getRenameAsStructColumn();
				return basicGetRenameAsStructColumn();
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS:
				if (resolve) return getStructClass();
				return basicGetStructClass();
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN:
				if (resolve) return getStructColumn();
				return basicGetStructColumn();
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
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN:
				setRenameAsStructColumn((StructTypedVariable)newValue);
				return;
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS:
				setStructClass((FreeBirdToolsCube)newValue);
				return;
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN:
				setStructColumn((StructTypedVariable)newValue);
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
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN:
				setRenameAsStructColumn((StructTypedVariable)null);
				return;
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS:
				setStructClass((FreeBirdToolsCube)null);
				return;
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN:
				setStructColumn((StructTypedVariable)null);
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
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN:
				return renameAsStructColumn != null;
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS:
				return structClass != null;
			case dtm_sql_viewsPackage.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN:
				return structColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //FilterByStructClassColumnViewImpl
