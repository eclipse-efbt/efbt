/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.impl;

import java.util.Collection;

import org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn;
import org.eclipse.efbt.language.dtm.model.dtm_views.JoinView;
import org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs;
import org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause;
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
 * An implementation of the model object '<em><b>Join View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.JoinViewImpl#getWhereClause <em>Where Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.JoinViewImpl#getRenames <em>Renames</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.JoinViewImpl#getIgnores <em>Ignores</em>}</li>
 * </ul>
 *
 * @generated
 */
public class JoinViewImpl extends DTMViewImpl implements JoinView {
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
	 * The cached value of the '{@link #getRenames() <em>Renames</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRenames()
	 * @generated
	 * @ordered
	 */
	protected EList<RenameAs> renames;

	/**
	 * The cached value of the '{@link #getIgnores() <em>Ignores</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIgnores()
	 * @generated
	 * @ordered
	 */
	protected EList<IgnoreColumn> ignores;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected JoinViewImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_viewsPackage.Literals.JOIN_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public WhereClause getWhereClause() {
		return whereClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetWhereClause(WhereClause newWhereClause, NotificationChain msgs) {
		WhereClause oldWhereClause = whereClause;
		whereClause = newWhereClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE, oldWhereClause, newWhereClause);
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
	public void setWhereClause(WhereClause newWhereClause) {
		if (newWhereClause != whereClause) {
			NotificationChain msgs = null;
			if (whereClause != null)
				msgs = ((InternalEObject)whereClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE, null, msgs);
			if (newWhereClause != null)
				msgs = ((InternalEObject)newWhereClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE, null, msgs);
			msgs = basicSetWhereClause(newWhereClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE, newWhereClause, newWhereClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<RenameAs> getRenames() {
		if (renames == null) {
			renames = new EObjectContainmentEList<RenameAs>(RenameAs.class, this, dtm_viewsPackage.JOIN_VIEW__RENAMES);
		}
		return renames;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<IgnoreColumn> getIgnores() {
		if (ignores == null) {
			ignores = new EObjectContainmentEList<IgnoreColumn>(IgnoreColumn.class, this, dtm_viewsPackage.JOIN_VIEW__IGNORES);
		}
		return ignores;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE:
				return basicSetWhereClause(null, msgs);
			case dtm_viewsPackage.JOIN_VIEW__RENAMES:
				return ((InternalEList<?>)getRenames()).basicRemove(otherEnd, msgs);
			case dtm_viewsPackage.JOIN_VIEW__IGNORES:
				return ((InternalEList<?>)getIgnores()).basicRemove(otherEnd, msgs);
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
			case dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE:
				return getWhereClause();
			case dtm_viewsPackage.JOIN_VIEW__RENAMES:
				return getRenames();
			case dtm_viewsPackage.JOIN_VIEW__IGNORES:
				return getIgnores();
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
			case dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE:
				setWhereClause((WhereClause)newValue);
				return;
			case dtm_viewsPackage.JOIN_VIEW__RENAMES:
				getRenames().clear();
				getRenames().addAll((Collection<? extends RenameAs>)newValue);
				return;
			case dtm_viewsPackage.JOIN_VIEW__IGNORES:
				getIgnores().clear();
				getIgnores().addAll((Collection<? extends IgnoreColumn>)newValue);
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
			case dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE:
				setWhereClause((WhereClause)null);
				return;
			case dtm_viewsPackage.JOIN_VIEW__RENAMES:
				getRenames().clear();
				return;
			case dtm_viewsPackage.JOIN_VIEW__IGNORES:
				getIgnores().clear();
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
			case dtm_viewsPackage.JOIN_VIEW__WHERE_CLAUSE:
				return whereClause != null;
			case dtm_viewsPackage.JOIN_VIEW__RENAMES:
				return renames != null && !renames.isEmpty();
			case dtm_viewsPackage.JOIN_VIEW__IGNORES:
				return ignores != null && !ignores.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //JoinViewImpl
