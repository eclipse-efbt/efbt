/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import java.util.Collection;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.SQLView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.VersionedSQLViewsModule;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedFunctionalModuleLogicImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Versioned SQL Views Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.VersionedSQLViewsModuleImpl#getSqlViews <em>Sql Views</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VersionedSQLViewsModuleImpl extends VersionedFunctionalModuleLogicImpl implements VersionedSQLViewsModule
{
	/**
	 * The cached value of the '{@link #getSqlViews() <em>Sql Views</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSqlViews()
	 * @generated
	 * @ordered
	 */
	protected EList<SQLView> sqlViews;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VersionedSQLViewsModuleImpl()
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
		return dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SQLView> getSqlViews()
	{
		if (sqlViews == null) {
			sqlViews = new EObjectContainmentEList<SQLView>(SQLView.class, this, dtm_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS);
		}
		return sqlViews;
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
			case dtm_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS:
				return ((InternalEList<?>)getSqlViews()).basicRemove(otherEnd, msgs);
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
			case dtm_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS:
				return getSqlViews();
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
			case dtm_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS:
				getSqlViews().clear();
				getSqlViews().addAll((Collection<? extends SQLView>)newValue);
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
			case dtm_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS:
				getSqlViews().clear();
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
			case dtm_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS:
				return sqlViews != null && !sqlViews.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VersionedSQLViewsModuleImpl
