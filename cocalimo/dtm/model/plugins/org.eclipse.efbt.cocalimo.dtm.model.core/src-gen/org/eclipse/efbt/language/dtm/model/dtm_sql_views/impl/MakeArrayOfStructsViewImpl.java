/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.MakeArrayOfStructsView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Make Array Of Structs View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.MakeArrayOfStructsViewImpl#getGroupingVariable <em>Grouping Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.MakeArrayOfStructsViewImpl#getSourceStructVariable <em>Source Struct Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.MakeArrayOfStructsViewImpl#getTargetArrayVariable <em>Target Array Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MakeArrayOfStructsViewImpl extends SQLViewImpl implements MakeArrayOfStructsView
{
	/**
	 * The cached value of the '{@link #getGroupingVariable() <em>Grouping Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupingVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE groupingVariable;

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
	 * The cached value of the '{@link #getTargetArrayVariable() <em>Target Array Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTargetArrayVariable()
	 * @generated
	 * @ordered
	 */
	protected ArrayTypedVariable targetArrayVariable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MakeArrayOfStructsViewImpl()
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
		return dtm_sql_viewsPackage.Literals.MAKE_ARRAY_OF_STRUCTS_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getGroupingVariable()
	{
		if (groupingVariable != null && groupingVariable.eIsProxy()) {
			InternalEObject oldGroupingVariable = (InternalEObject)groupingVariable;
			groupingVariable = (VARIABLE)eResolveProxy(oldGroupingVariable);
			if (groupingVariable != oldGroupingVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE, oldGroupingVariable, groupingVariable));
			}
		}
		return groupingVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetGroupingVariable()
	{
		return groupingVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setGroupingVariable(VARIABLE newGroupingVariable)
	{
		VARIABLE oldGroupingVariable = groupingVariable;
		groupingVariable = newGroupingVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE, oldGroupingVariable, groupingVariable));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE, oldSourceStructVariable, sourceStructVariable));
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
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE, oldSourceStructVariable, sourceStructVariable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ArrayTypedVariable getTargetArrayVariable()
	{
		if (targetArrayVariable != null && targetArrayVariable.eIsProxy()) {
			InternalEObject oldTargetArrayVariable = (InternalEObject)targetArrayVariable;
			targetArrayVariable = (ArrayTypedVariable)eResolveProxy(oldTargetArrayVariable);
			if (targetArrayVariable != oldTargetArrayVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE, oldTargetArrayVariable, targetArrayVariable));
			}
		}
		return targetArrayVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ArrayTypedVariable basicGetTargetArrayVariable()
	{
		return targetArrayVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTargetArrayVariable(ArrayTypedVariable newTargetArrayVariable)
	{
		ArrayTypedVariable oldTargetArrayVariable = targetArrayVariable;
		targetArrayVariable = newTargetArrayVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE, oldTargetArrayVariable, targetArrayVariable));
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
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE:
				if (resolve) return getGroupingVariable();
				return basicGetGroupingVariable();
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE:
				if (resolve) return getSourceStructVariable();
				return basicGetSourceStructVariable();
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE:
				if (resolve) return getTargetArrayVariable();
				return basicGetTargetArrayVariable();
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
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE:
				setGroupingVariable((VARIABLE)newValue);
				return;
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE:
				setSourceStructVariable((StructTypedVariable)newValue);
				return;
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE:
				setTargetArrayVariable((ArrayTypedVariable)newValue);
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
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE:
				setGroupingVariable((VARIABLE)null);
				return;
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE:
				setSourceStructVariable((StructTypedVariable)null);
				return;
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE:
				setTargetArrayVariable((ArrayTypedVariable)null);
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
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE:
				return groupingVariable != null;
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE:
				return sourceStructVariable != null;
			case dtm_sql_viewsPackage.MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE:
				return targetArrayVariable != null;
		}
		return super.eIsSet(featureID);
	}

} //MakeArrayOfStructsViewImpl
