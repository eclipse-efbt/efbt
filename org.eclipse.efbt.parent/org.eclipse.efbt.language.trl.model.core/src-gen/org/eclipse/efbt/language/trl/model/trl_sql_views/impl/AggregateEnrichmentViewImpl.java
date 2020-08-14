/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views.impl;

import java.util.Collection;

import org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause;
import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Aggregate Enrichment View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.AggregateEnrichmentViewImpl#getFunctions <em>Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.AggregateEnrichmentViewImpl#getGroupByClause <em>Group By Clause</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AggregateEnrichmentViewImpl extends SQLViewImpl implements AggregateEnrichmentView
{
	/**
	 * The cached value of the '{@link #getFunctions() <em>Functions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctions()
	 * @generated
	 * @ordered
	 */
	protected EList<AggregateColumnFunction> functions;

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
	protected AggregateEnrichmentViewImpl()
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
		return Trl_sql_viewsPackage.Literals.AGGREGATE_ENRICHMENT_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<AggregateColumnFunction> getFunctions()
	{
		if (functions == null)
		{
			functions = new EObjectContainmentEList<AggregateColumnFunction>(AggregateColumnFunction.class, this, Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS);
		}
		return functions;
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
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE, oldGroupByClause, newGroupByClause);
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
		if (newGroupByClause != groupByClause)
		{
			NotificationChain msgs = null;
			if (groupByClause != null)
				msgs = ((InternalEObject)groupByClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE, null, msgs);
			if (newGroupByClause != null)
				msgs = ((InternalEObject)newGroupByClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE, null, msgs);
			msgs = basicSetGroupByClause(newGroupByClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE, newGroupByClause, newGroupByClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS:
				return ((InternalEList<?>)getFunctions()).basicRemove(otherEnd, msgs);
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE:
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
		switch (featureID)
		{
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS:
				return getFunctions();
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE:
				return getGroupByClause();
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
		switch (featureID)
		{
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS:
				getFunctions().clear();
				getFunctions().addAll((Collection<? extends AggregateColumnFunction>)newValue);
				return;
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE:
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
		switch (featureID)
		{
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS:
				getFunctions().clear();
				return;
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE:
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
		switch (featureID)
		{
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS:
				return functions != null && !functions.isEmpty();
			case Trl_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE:
				return groupByClause != null;
		}
		return super.eIsSet(featureID);
	}

} //AggregateEnrichmentViewImpl
