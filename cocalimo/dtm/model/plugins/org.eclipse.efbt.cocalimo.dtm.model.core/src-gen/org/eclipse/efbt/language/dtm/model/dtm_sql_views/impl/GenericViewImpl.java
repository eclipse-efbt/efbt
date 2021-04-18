/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.GenericView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.GroupByClause;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.SelectClause;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.WhereClause;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Generic View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.GenericViewImpl#getSelectClause <em>Select Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.GenericViewImpl#getWhereClause <em>Where Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.GenericViewImpl#getGroupByClause <em>Group By Clause</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GenericViewImpl extends SQLViewImpl implements GenericView
{
	/**
	 * The cached value of the '{@link #getSelectClause() <em>Select Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSelectClause()
	 * @generated
	 * @ordered
	 */
	protected SelectClause selectClause;

	/**
	 * The cached value of the '{@link #getWhereClause() <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWhereClause()
	 * @generated
	 * @ordered
	 */
	protected WhereClause whereClause;

	/**
	 * The cached value of the '{@link #getGroupByClause() <em>Group By Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupByClause()
	 * @generated
	 * @ordered
	 */
	protected GroupByClause groupByClause;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GenericViewImpl()
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
		return dtm_sql_viewsPackage.Literals.GENERIC_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectClause getSelectClause()
	{
		return selectClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSelectClause(SelectClause newSelectClause, NotificationChain msgs)
	{
		SelectClause oldSelectClause = selectClause;
		selectClause = newSelectClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE, oldSelectClause, newSelectClause);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSelectClause(SelectClause newSelectClause)
	{
		if (newSelectClause != selectClause) {
			NotificationChain msgs = null;
			if (selectClause != null)
				msgs = ((InternalEObject)selectClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE, null, msgs);
			if (newSelectClause != null)
				msgs = ((InternalEObject)newSelectClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE, null, msgs);
			msgs = basicSetSelectClause(newSelectClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE, newSelectClause, newSelectClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public WhereClause getWhereClause()
	{
		return whereClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetWhereClause(WhereClause newWhereClause, NotificationChain msgs)
	{
		WhereClause oldWhereClause = whereClause;
		whereClause = newWhereClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE, oldWhereClause, newWhereClause);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setWhereClause(WhereClause newWhereClause)
	{
		if (newWhereClause != whereClause) {
			NotificationChain msgs = null;
			if (whereClause != null)
				msgs = ((InternalEObject)whereClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE, null, msgs);
			if (newWhereClause != null)
				msgs = ((InternalEObject)newWhereClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE, null, msgs);
			msgs = basicSetWhereClause(newWhereClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE, newWhereClause, newWhereClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GroupByClause getGroupByClause()
	{
		return groupByClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetGroupByClause(GroupByClause newGroupByClause, NotificationChain msgs)
	{
		GroupByClause oldGroupByClause = groupByClause;
		groupByClause = newGroupByClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE, oldGroupByClause, newGroupByClause);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setGroupByClause(GroupByClause newGroupByClause)
	{
		if (newGroupByClause != groupByClause) {
			NotificationChain msgs = null;
			if (groupByClause != null)
				msgs = ((InternalEObject)groupByClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE, null, msgs);
			if (newGroupByClause != null)
				msgs = ((InternalEObject)newGroupByClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE, null, msgs);
			msgs = basicSetGroupByClause(newGroupByClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE, newGroupByClause, newGroupByClause));
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
			case dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE:
				return basicSetSelectClause(null, msgs);
			case dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE:
				return basicSetWhereClause(null, msgs);
			case dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE:
				return basicSetGroupByClause(null, msgs);
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
			case dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE:
				return getSelectClause();
			case dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE:
				return getWhereClause();
			case dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE:
				return getGroupByClause();
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
			case dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE:
				setSelectClause((SelectClause)newValue);
				return;
			case dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE:
				setWhereClause((WhereClause)newValue);
				return;
			case dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE:
				setGroupByClause((GroupByClause)newValue);
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
			case dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE:
				setSelectClause((SelectClause)null);
				return;
			case dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE:
				setWhereClause((WhereClause)null);
				return;
			case dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE:
				setGroupByClause((GroupByClause)null);
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
			case dtm_sql_viewsPackage.GENERIC_VIEW__SELECT_CLAUSE:
				return selectClause != null;
			case dtm_sql_viewsPackage.GENERIC_VIEW__WHERE_CLAUSE:
				return whereClause != null;
			case dtm_sql_viewsPackage.GENERIC_VIEW__GROUP_BY_CLAUSE:
				return groupByClause != null;
		}
		return super.eIsSet(featureID);
	}

} //GenericViewImpl
