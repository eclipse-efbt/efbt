/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.CastColumnView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Cast Column View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.CastColumnViewImpl#getSourceStructVariable <em>Source Struct Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.CastColumnViewImpl#getTargetStructVariable <em>Target Struct Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CastColumnViewImpl extends SQLViewImpl implements CastColumnView
{
	/**
	 * The cached value of the '{@link #getSourceStructVariable() <em>Source Struct Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceStructVariable()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable sourceStructVariable;

	/**
	 * The cached value of the '{@link #getTargetStructVariable() <em>Target Struct Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTargetStructVariable()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariable targetStructVariable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CastColumnViewImpl()
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
		return dtm_sql_viewsPackage.Literals.CAST_COLUMN_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getSourceStructVariable()
	{
		if (sourceStructVariable != null && sourceStructVariable.eIsProxy()) {
			InternalEObject oldSourceStructVariable = (InternalEObject)sourceStructVariable;
			sourceStructVariable = (StructTypedVariable)eResolveProxy(oldSourceStructVariable);
			if (sourceStructVariable != oldSourceStructVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE, oldSourceStructVariable, sourceStructVariable));
			}
		}
		return sourceStructVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetSourceStructVariable()
	{
		return sourceStructVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSourceStructVariable(StructTypedVariable newSourceStructVariable)
	{
		StructTypedVariable oldSourceStructVariable = sourceStructVariable;
		sourceStructVariable = newSourceStructVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE, oldSourceStructVariable, sourceStructVariable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable getTargetStructVariable()
	{
		if (targetStructVariable != null && targetStructVariable.eIsProxy()) {
			InternalEObject oldTargetStructVariable = (InternalEObject)targetStructVariable;
			targetStructVariable = (StructTypedVariable)eResolveProxy(oldTargetStructVariable);
			if (targetStructVariable != oldTargetStructVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE, oldTargetStructVariable, targetStructVariable));
			}
		}
		return targetStructVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StructTypedVariable basicGetTargetStructVariable()
	{
		return targetStructVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTargetStructVariable(StructTypedVariable newTargetStructVariable)
	{
		StructTypedVariable oldTargetStructVariable = targetStructVariable;
		targetStructVariable = newTargetStructVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE, oldTargetStructVariable, targetStructVariable));
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
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE:
				if (resolve) return getSourceStructVariable();
				return basicGetSourceStructVariable();
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE:
				if (resolve) return getTargetStructVariable();
				return basicGetTargetStructVariable();
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
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE:
				setSourceStructVariable((StructTypedVariable)newValue);
				return;
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE:
				setTargetStructVariable((StructTypedVariable)newValue);
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
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE:
				setSourceStructVariable((StructTypedVariable)null);
				return;
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE:
				setTargetStructVariable((StructTypedVariable)null);
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
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE:
				return sourceStructVariable != null;
			case dtm_sql_viewsPackage.CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE:
				return targetStructVariable != null;
		}
		return super.eIsSet(featureID);
	}

} //CastColumnViewImpl
