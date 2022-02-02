/**
 */
package column_transformation_logic.impl;

import column_transformation_logic.BasicColumnFunction;
import column_transformation_logic.Column_transformation_logicPackage;

import functions.BasicFunction;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Basic Column Function</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link column_transformation_logic.impl.BasicColumnFunctionImpl#getBasicFunction <em>Basic Function</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BasicColumnFunctionImpl extends ColumnFunctionImpl implements BasicColumnFunction {
	/**
	 * The cached value of the '{@link #getBasicFunction() <em>Basic Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBasicFunction()
	 * @generated
	 * @ordered
	 */
	protected BasicFunction basicFunction;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BasicColumnFunctionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Column_transformation_logicPackage.Literals.BASIC_COLUMN_FUNCTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicFunction getBasicFunction() {
		return basicFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetBasicFunction(BasicFunction newBasicFunction, NotificationChain msgs) {
		BasicFunction oldBasicFunction = basicFunction;
		basicFunction = newBasicFunction;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION, oldBasicFunction, newBasicFunction);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setBasicFunction(BasicFunction newBasicFunction) {
		if (newBasicFunction != basicFunction) {
			NotificationChain msgs = null;
			if (basicFunction != null)
				msgs = ((InternalEObject)basicFunction).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION, null, msgs);
			if (newBasicFunction != null)
				msgs = ((InternalEObject)newBasicFunction).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION, null, msgs);
			msgs = basicSetBasicFunction(newBasicFunction, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION, newBasicFunction, newBasicFunction));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION:
				return basicSetBasicFunction(null, msgs);
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
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION:
				return getBasicFunction();
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
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION:
				setBasicFunction((BasicFunction)newValue);
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
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION:
				setBasicFunction((BasicFunction)null);
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
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION__BASIC_FUNCTION:
				return basicFunction != null;
		}
		return super.eIsSet(featureID);
	}

} //BasicColumnFunctionImpl
