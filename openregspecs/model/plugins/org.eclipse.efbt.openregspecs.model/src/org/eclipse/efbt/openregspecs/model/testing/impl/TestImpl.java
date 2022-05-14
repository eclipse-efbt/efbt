/**
 */
package org.eclipse.efbt.openregspecs.model.testing.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.testing.CSVFile;
import org.eclipse.efbt.openregspecs.model.testing.Test;
import org.eclipse.efbt.openregspecs.model.testing.TestScope;
import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl#getExpectedResult <em>Expected Result</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl#getScope <em>Scope</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestImpl extends MinimalEObjectImpl.Container implements Test {
	/**
	 * The cached value of the '{@link #getInputData() <em>Input Data</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputData()
	 * @generated
	 * @ordered
	 */
	protected EList<CSVFile> inputData;

	/**
	 * The cached value of the '{@link #getExpectedResult() <em>Expected Result</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpectedResult()
	 * @generated
	 * @ordered
	 */
	protected EList<CSVFile> expectedResult;

	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The cached value of the '{@link #getScope() <em>Scope</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScope()
	 * @generated
	 * @ordered
	 */
	protected TestScope scope;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestingPackage.Literals.TEST;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<CSVFile> getInputData() {
		if (inputData == null) {
			inputData = new EObjectResolvingEList<CSVFile>(CSVFile.class, this, TestingPackage.TEST__INPUT_DATA);
		}
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<CSVFile> getExpectedResult() {
		if (expectedResult == null) {
			expectedResult = new EObjectResolvingEList<CSVFile>(CSVFile.class, this, TestingPackage.TEST__EXPECTED_RESULT);
		}
		return expectedResult;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.TEST__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestScope getScope() {
		return scope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetScope(TestScope newScope, NotificationChain msgs) {
		TestScope oldScope = scope;
		scope = newScope;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestingPackage.TEST__SCOPE, oldScope, newScope);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setScope(TestScope newScope) {
		if (newScope != scope) {
			NotificationChain msgs = null;
			if (scope != null)
				msgs = ((InternalEObject)scope).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestingPackage.TEST__SCOPE, null, msgs);
			if (newScope != null)
				msgs = ((InternalEObject)newScope).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestingPackage.TEST__SCOPE, null, msgs);
			msgs = basicSetScope(newScope, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.TEST__SCOPE, newScope, newScope));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestingPackage.TEST__SCOPE:
				return basicSetScope(null, msgs);
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
			case TestingPackage.TEST__INPUT_DATA:
				return getInputData();
			case TestingPackage.TEST__EXPECTED_RESULT:
				return getExpectedResult();
			case TestingPackage.TEST__NAME:
				return getName();
			case TestingPackage.TEST__SCOPE:
				return getScope();
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
			case TestingPackage.TEST__INPUT_DATA:
				getInputData().clear();
				getInputData().addAll((Collection<? extends CSVFile>)newValue);
				return;
			case TestingPackage.TEST__EXPECTED_RESULT:
				getExpectedResult().clear();
				getExpectedResult().addAll((Collection<? extends CSVFile>)newValue);
				return;
			case TestingPackage.TEST__NAME:
				setName((String)newValue);
				return;
			case TestingPackage.TEST__SCOPE:
				setScope((TestScope)newValue);
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
			case TestingPackage.TEST__INPUT_DATA:
				getInputData().clear();
				return;
			case TestingPackage.TEST__EXPECTED_RESULT:
				getExpectedResult().clear();
				return;
			case TestingPackage.TEST__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TestingPackage.TEST__SCOPE:
				setScope((TestScope)null);
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
			case TestingPackage.TEST__INPUT_DATA:
				return inputData != null && !inputData.isEmpty();
			case TestingPackage.TEST__EXPECTED_RESULT:
				return expectedResult != null && !expectedResult.isEmpty();
			case TestingPackage.TEST__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TestingPackage.TEST__SCOPE:
				return scope != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //TestImpl
