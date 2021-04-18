/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.ExplodeArrayOfStructsView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Explode Array Of Structs View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.ExplodeArrayOfStructsViewImpl#getArraySourceColumn <em>Array Source Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.ExplodeArrayOfStructsViewImpl#getIndexColumn <em>Index Column</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.ExplodeArrayOfStructsViewImpl#getTargetStructColumn <em>Target Struct Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ExplodeArrayOfStructsViewImpl extends SQLViewImpl implements ExplodeArrayOfStructsView
{
	/**
	 * The cached value of the '{@link #getArraySourceColumn() <em>Array Source Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getArraySourceColumn()
	 * @generated
	 * @ordered
	 */
	protected ArrayTypedVariable arraySourceColumn;

	/**
	 * The cached value of the '{@link #getIndexColumn() <em>Index Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIndexColumn()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE indexColumn;

	/**
	 * The cached value of the '{@link #getTargetStructColumn() <em>Target Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTargetStructColumn()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable targetStructColumn;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ExplodeArrayOfStructsViewImpl()
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
		return dtm_sql_viewsPackage.Literals.EXPLODE_ARRAY_OF_STRUCTS_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ArrayTypedVariable getArraySourceColumn()
	{
		if (arraySourceColumn != null && arraySourceColumn.eIsProxy()) {
			InternalEObject oldArraySourceColumn = (InternalEObject)arraySourceColumn;
			arraySourceColumn = (ArrayTypedVariable)eResolveProxy(oldArraySourceColumn);
			if (arraySourceColumn != oldArraySourceColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN, oldArraySourceColumn, arraySourceColumn));
			}
		}
		return arraySourceColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ArrayTypedVariable basicGetArraySourceColumn()
	{
		return arraySourceColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setArraySourceColumn(ArrayTypedVariable newArraySourceColumn)
	{
		ArrayTypedVariable oldArraySourceColumn = arraySourceColumn;
		arraySourceColumn = newArraySourceColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN, oldArraySourceColumn, arraySourceColumn));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getIndexColumn()
	{
		if (indexColumn != null && indexColumn.eIsProxy()) {
			InternalEObject oldIndexColumn = (InternalEObject)indexColumn;
			indexColumn = (VARIABLE)eResolveProxy(oldIndexColumn);
			if (indexColumn != oldIndexColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN, oldIndexColumn, indexColumn));
			}
		}
		return indexColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetIndexColumn()
	{
		return indexColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIndexColumn(VARIABLE newIndexColumn)
	{
		VARIABLE oldIndexColumn = indexColumn;
		indexColumn = newIndexColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN, oldIndexColumn, indexColumn));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getTargetStructColumn()
	{
		if (targetStructColumn != null && targetStructColumn.eIsProxy()) {
			InternalEObject oldTargetStructColumn = (InternalEObject)targetStructColumn;
			targetStructColumn = (StructTypedVariable)eResolveProxy(oldTargetStructColumn);
			if (targetStructColumn != oldTargetStructColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN, oldTargetStructColumn, targetStructColumn));
			}
		}
		return targetStructColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetTargetStructColumn()
	{
		return targetStructColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTargetStructColumn(StructTypedVariable newTargetStructColumn)
	{
		StructTypedVariable oldTargetStructColumn = targetStructColumn;
		targetStructColumn = newTargetStructColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN, oldTargetStructColumn, targetStructColumn));
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
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN:
				if (resolve) return getArraySourceColumn();
				return basicGetArraySourceColumn();
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN:
				if (resolve) return getIndexColumn();
				return basicGetIndexColumn();
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN:
				if (resolve) return getTargetStructColumn();
				return basicGetTargetStructColumn();
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
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN:
				setArraySourceColumn((ArrayTypedVariable)newValue);
				return;
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN:
				setIndexColumn((VARIABLE)newValue);
				return;
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN:
				setTargetStructColumn((StructTypedVariable)newValue);
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
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN:
				setArraySourceColumn((ArrayTypedVariable)null);
				return;
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN:
				setIndexColumn((VARIABLE)null);
				return;
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN:
				setTargetStructColumn((StructTypedVariable)null);
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
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN:
				return arraySourceColumn != null;
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN:
				return indexColumn != null;
			case dtm_sql_viewsPackage.EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN:
				return targetStructColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //ExplodeArrayOfStructsViewImpl
