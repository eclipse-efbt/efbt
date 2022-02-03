/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.FromClause;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectClause;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.View;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.WhereClause;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.ViewImpl#getSelectClause <em>Select Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.ViewImpl#getFromClause <em>From Clause</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.ViewImpl#getWhereClause <em>Where Clause</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ViewImpl extends SQLEntityImpl implements View {
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
	 * The cached value of the '{@link #getFromClause() <em>From Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFromClause()
	 * @generated
	 * @ordered
	 */
	protected FromClause fromClause;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ViewImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Sql_litePackage.Literals.VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectClause getSelectClause() {
		return selectClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSelectClause(SelectClause newSelectClause, NotificationChain msgs) {
		SelectClause oldSelectClause = selectClause;
		selectClause = newSelectClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Sql_litePackage.VIEW__SELECT_CLAUSE, oldSelectClause, newSelectClause);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSelectClause(SelectClause newSelectClause) {
		if (newSelectClause != selectClause) {
			NotificationChain msgs = null;
			if (selectClause != null)
				msgs = ((InternalEObject)selectClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.VIEW__SELECT_CLAUSE, null, msgs);
			if (newSelectClause != null)
				msgs = ((InternalEObject)newSelectClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.VIEW__SELECT_CLAUSE, null, msgs);
			msgs = basicSetSelectClause(newSelectClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.VIEW__SELECT_CLAUSE, newSelectClause, newSelectClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FromClause getFromClause() {
		return fromClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetFromClause(FromClause newFromClause, NotificationChain msgs) {
		FromClause oldFromClause = fromClause;
		fromClause = newFromClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Sql_litePackage.VIEW__FROM_CLAUSE, oldFromClause, newFromClause);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFromClause(FromClause newFromClause) {
		if (newFromClause != fromClause) {
			NotificationChain msgs = null;
			if (fromClause != null)
				msgs = ((InternalEObject)fromClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.VIEW__FROM_CLAUSE, null, msgs);
			if (newFromClause != null)
				msgs = ((InternalEObject)newFromClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.VIEW__FROM_CLAUSE, null, msgs);
			msgs = basicSetFromClause(newFromClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.VIEW__FROM_CLAUSE, newFromClause, newFromClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
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
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Sql_litePackage.VIEW__WHERE_CLAUSE, oldWhereClause, newWhereClause);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setWhereClause(WhereClause newWhereClause) {
		if (newWhereClause != whereClause) {
			NotificationChain msgs = null;
			if (whereClause != null)
				msgs = ((InternalEObject)whereClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.VIEW__WHERE_CLAUSE, null, msgs);
			if (newWhereClause != null)
				msgs = ((InternalEObject)newWhereClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.VIEW__WHERE_CLAUSE, null, msgs);
			msgs = basicSetWhereClause(newWhereClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.VIEW__WHERE_CLAUSE, newWhereClause, newWhereClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Sql_litePackage.VIEW__SELECT_CLAUSE:
				return basicSetSelectClause(null, msgs);
			case Sql_litePackage.VIEW__FROM_CLAUSE:
				return basicSetFromClause(null, msgs);
			case Sql_litePackage.VIEW__WHERE_CLAUSE:
				return basicSetWhereClause(null, msgs);
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
			case Sql_litePackage.VIEW__SELECT_CLAUSE:
				return getSelectClause();
			case Sql_litePackage.VIEW__FROM_CLAUSE:
				return getFromClause();
			case Sql_litePackage.VIEW__WHERE_CLAUSE:
				return getWhereClause();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Sql_litePackage.VIEW__SELECT_CLAUSE:
				setSelectClause((SelectClause)newValue);
				return;
			case Sql_litePackage.VIEW__FROM_CLAUSE:
				setFromClause((FromClause)newValue);
				return;
			case Sql_litePackage.VIEW__WHERE_CLAUSE:
				setWhereClause((WhereClause)newValue);
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
			case Sql_litePackage.VIEW__SELECT_CLAUSE:
				setSelectClause((SelectClause)null);
				return;
			case Sql_litePackage.VIEW__FROM_CLAUSE:
				setFromClause((FromClause)null);
				return;
			case Sql_litePackage.VIEW__WHERE_CLAUSE:
				setWhereClause((WhereClause)null);
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
			case Sql_litePackage.VIEW__SELECT_CLAUSE:
				return selectClause != null;
			case Sql_litePackage.VIEW__FROM_CLAUSE:
				return fromClause != null;
			case Sql_litePackage.VIEW__WHERE_CLAUSE:
				return whereClause != null;
		}
		return super.eIsSet(featureID);
	}

} //ViewImpl
