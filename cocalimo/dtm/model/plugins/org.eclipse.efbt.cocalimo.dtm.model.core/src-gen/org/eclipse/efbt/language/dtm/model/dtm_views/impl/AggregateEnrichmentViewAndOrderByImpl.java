/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateColumnFunction;

import org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy;
import org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

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
 * An implementation of the model object '<em><b>Aggregate Enrichment View And Order By</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewAndOrderByImpl#getFunctions <em>Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewAndOrderByImpl#getGroupByClause <em>Group By Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewAndOrderByImpl#getOrderByVariable <em>Order By Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AggregateEnrichmentViewAndOrderByImpl extends DTMViewImpl implements AggregateEnrichmentViewAndOrderBy {
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
	 * The cached value of the '{@link #getOrderByVariable() <em>Order By Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrderByVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE orderByVariable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AggregateEnrichmentViewAndOrderByImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_viewsPackage.Literals.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<AggregateColumnFunction> getFunctions() {
		if (functions == null) {
			functions = new EObjectContainmentEList<AggregateColumnFunction>(AggregateColumnFunction.class, this, dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS);
		}
		return functions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GroupByClause getGroupByClause() {
		return groupByClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetGroupByClause(GroupByClause newGroupByClause, NotificationChain msgs) {
		GroupByClause oldGroupByClause = groupByClause;
		groupByClause = newGroupByClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE, oldGroupByClause, newGroupByClause);
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
	public void setGroupByClause(GroupByClause newGroupByClause) {
		if (newGroupByClause != groupByClause) {
			NotificationChain msgs = null;
			if (groupByClause != null)
				msgs = ((InternalEObject)groupByClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE, null, msgs);
			if (newGroupByClause != null)
				msgs = ((InternalEObject)newGroupByClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE, null, msgs);
			msgs = basicSetGroupByClause(newGroupByClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE, newGroupByClause, newGroupByClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getOrderByVariable() {
		if (orderByVariable != null && orderByVariable.eIsProxy()) {
			InternalEObject oldOrderByVariable = (InternalEObject)orderByVariable;
			orderByVariable = (VARIABLE)eResolveProxy(oldOrderByVariable);
			if (orderByVariable != oldOrderByVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE, oldOrderByVariable, orderByVariable));
			}
		}
		return orderByVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetOrderByVariable() {
		return orderByVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setOrderByVariable(VARIABLE newOrderByVariable) {
		VARIABLE oldOrderByVariable = orderByVariable;
		orderByVariable = newOrderByVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE, oldOrderByVariable, orderByVariable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS:
				return ((InternalEList<?>)getFunctions()).basicRemove(otherEnd, msgs);
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE:
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
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS:
				return getFunctions();
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE:
				return getGroupByClause();
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE:
				if (resolve) return getOrderByVariable();
				return basicGetOrderByVariable();
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
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS:
				getFunctions().clear();
				getFunctions().addAll((Collection<? extends AggregateColumnFunction>)newValue);
				return;
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE:
				setGroupByClause((GroupByClause)newValue);
				return;
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE:
				setOrderByVariable((VARIABLE)newValue);
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
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS:
				getFunctions().clear();
				return;
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE:
				setGroupByClause((GroupByClause)null);
				return;
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE:
				setOrderByVariable((VARIABLE)null);
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
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS:
				return functions != null && !functions.isEmpty();
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE:
				return groupByClause != null;
			case dtm_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE:
				return orderByVariable != null;
		}
		return super.eIsSet(featureID);
	}

} //AggregateEnrichmentViewAndOrderByImpl
