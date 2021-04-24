/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Group By Clause</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.GroupByClauseImpl#getGroupByColumns <em>Group By Columns</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GroupByClauseImpl extends MinimalEObjectImpl.Container implements GroupByClause {
	/**
	 * The cached value of the '{@link #getGroupByColumns() <em>Group By Columns</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupByColumns()
	 * @generated
	 * @ordered
	 */
	protected EList<VARIABLE> groupByColumns;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GroupByClauseImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_viewsPackage.Literals.GROUP_BY_CLAUSE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<VARIABLE> getGroupByColumns() {
		if (groupByColumns == null) {
			groupByColumns = new EObjectResolvingEList<VARIABLE>(VARIABLE.class, this, dtm_viewsPackage.GROUP_BY_CLAUSE__GROUP_BY_COLUMNS);
		}
		return groupByColumns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case dtm_viewsPackage.GROUP_BY_CLAUSE__GROUP_BY_COLUMNS:
				return getGroupByColumns();
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
			case dtm_viewsPackage.GROUP_BY_CLAUSE__GROUP_BY_COLUMNS:
				getGroupByColumns().clear();
				getGroupByColumns().addAll((Collection<? extends VARIABLE>)newValue);
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
			case dtm_viewsPackage.GROUP_BY_CLAUSE__GROUP_BY_COLUMNS:
				getGroupByColumns().clear();
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
			case dtm_viewsPackage.GROUP_BY_CLAUSE__GROUP_BY_COLUMNS:
				return groupByColumns != null && !groupByColumns.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //GroupByClauseImpl
