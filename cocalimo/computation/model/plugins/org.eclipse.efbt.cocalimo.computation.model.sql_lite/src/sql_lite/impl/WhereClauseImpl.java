/**
 */
package sql_lite.impl;

import functions.BooleanFunction;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import sql_lite.Sql_litePackage;
import sql_lite.WhereClause;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Where Clause</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link sql_lite.impl.WhereClauseImpl#getWhere_function <em>Where function</em>}</li>
 * </ul>
 *
 * @generated
 */
public class WhereClauseImpl extends MinimalEObjectImpl.Container implements WhereClause {
	/**
	 * The cached value of the '{@link #getWhere_function() <em>Where function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWhere_function()
	 * @generated
	 * @ordered
	 */
	protected BooleanFunction where_function;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected WhereClauseImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Sql_litePackage.Literals.WHERE_CLAUSE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BooleanFunction getWhere_function() {
		return where_function;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetWhere_function(BooleanFunction newWhere_function, NotificationChain msgs) {
		BooleanFunction oldWhere_function = where_function;
		where_function = newWhere_function;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION, oldWhere_function, newWhere_function);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setWhere_function(BooleanFunction newWhere_function) {
		if (newWhere_function != where_function) {
			NotificationChain msgs = null;
			if (where_function != null)
				msgs = ((InternalEObject)where_function).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION, null, msgs);
			if (newWhere_function != null)
				msgs = ((InternalEObject)newWhere_function).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION, null, msgs);
			msgs = basicSetWhere_function(newWhere_function, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION, newWhere_function, newWhere_function));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION:
				return basicSetWhere_function(null, msgs);
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
			case Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION:
				return getWhere_function();
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
			case Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION:
				setWhere_function((BooleanFunction)newValue);
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
			case Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION:
				setWhere_function((BooleanFunction)null);
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
			case Sql_litePackage.WHERE_CLAUSE__WHERE_FUNCTION:
				return where_function != null;
		}
		return super.eIsSet(featureID);
	}

} //WhereClauseImpl
