/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.ReportResults;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition;

import org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl#getExpectedResults <em>Expected Results</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl#getTestDefinition <em>Test Definition</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestImpl extends MinimalEObjectImpl.Container implements Test {
	/**
	 * The cached value of the '{@link #getInputData() <em>Input Data</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputData()
	 * @generated
	 * @ordered
	 */
	protected InputData inputData;

	/**
	 * The cached value of the '{@link #getExpectedResults() <em>Expected Results</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpectedResults()
	 * @generated
	 * @ordered
	 */
	protected ReportResults expectedResults;

	/**
	 * The cached value of the '{@link #getTestDefinition() <em>Test Definition</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestDefinition()
	 * @generated
	 * @ordered
	 */
	protected TestDefinition testDefinition;

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
		return Aorta_smcubesPackage.Literals.TEST;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public InputData getInputData() {
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetInputData(InputData newInputData, NotificationChain msgs) {
		InputData oldInputData = inputData;
		inputData = newInputData;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST__INPUT_DATA, oldInputData, newInputData);
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
	public void setInputData(InputData newInputData) {
		if (newInputData != inputData) {
			NotificationChain msgs = null;
			if (inputData != null)
				msgs = ((InternalEObject)inputData).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Aorta_smcubesPackage.TEST__INPUT_DATA, null, msgs);
			if (newInputData != null)
				msgs = ((InternalEObject)newInputData).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Aorta_smcubesPackage.TEST__INPUT_DATA, null, msgs);
			msgs = basicSetInputData(newInputData, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST__INPUT_DATA, newInputData, newInputData));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportResults getExpectedResults() {
		return expectedResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetExpectedResults(ReportResults newExpectedResults, NotificationChain msgs) {
		ReportResults oldExpectedResults = expectedResults;
		expectedResults = newExpectedResults;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST__EXPECTED_RESULTS, oldExpectedResults, newExpectedResults);
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
	public void setExpectedResults(ReportResults newExpectedResults) {
		if (newExpectedResults != expectedResults) {
			NotificationChain msgs = null;
			if (expectedResults != null)
				msgs = ((InternalEObject)expectedResults).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Aorta_smcubesPackage.TEST__EXPECTED_RESULTS, null, msgs);
			if (newExpectedResults != null)
				msgs = ((InternalEObject)newExpectedResults).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Aorta_smcubesPackage.TEST__EXPECTED_RESULTS, null, msgs);
			msgs = basicSetExpectedResults(newExpectedResults, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST__EXPECTED_RESULTS, newExpectedResults, newExpectedResults));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestDefinition getTestDefinition() {
		if (testDefinition != null && testDefinition.eIsProxy()) {
			InternalEObject oldTestDefinition = (InternalEObject)testDefinition;
			testDefinition = (TestDefinition)eResolveProxy(oldTestDefinition);
			if (testDefinition != oldTestDefinition) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.TEST__TEST_DEFINITION, oldTestDefinition, testDefinition));
			}
		}
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestDefinition basicGetTestDefinition() {
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTestDefinition(TestDefinition newTestDefinition) {
		TestDefinition oldTestDefinition = testDefinition;
		testDefinition = newTestDefinition;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST__TEST_DEFINITION, oldTestDefinition, testDefinition));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Aorta_smcubesPackage.TEST__INPUT_DATA:
				return basicSetInputData(null, msgs);
			case Aorta_smcubesPackage.TEST__EXPECTED_RESULTS:
				return basicSetExpectedResults(null, msgs);
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
			case Aorta_smcubesPackage.TEST__INPUT_DATA:
				return getInputData();
			case Aorta_smcubesPackage.TEST__EXPECTED_RESULTS:
				return getExpectedResults();
			case Aorta_smcubesPackage.TEST__TEST_DEFINITION:
				if (resolve) return getTestDefinition();
				return basicGetTestDefinition();
			case Aorta_smcubesPackage.TEST__NAME:
				return getName();
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
			case Aorta_smcubesPackage.TEST__INPUT_DATA:
				setInputData((InputData)newValue);
				return;
			case Aorta_smcubesPackage.TEST__EXPECTED_RESULTS:
				setExpectedResults((ReportResults)newValue);
				return;
			case Aorta_smcubesPackage.TEST__TEST_DEFINITION:
				setTestDefinition((TestDefinition)newValue);
				return;
			case Aorta_smcubesPackage.TEST__NAME:
				setName((String)newValue);
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
			case Aorta_smcubesPackage.TEST__INPUT_DATA:
				setInputData((InputData)null);
				return;
			case Aorta_smcubesPackage.TEST__EXPECTED_RESULTS:
				setExpectedResults((ReportResults)null);
				return;
			case Aorta_smcubesPackage.TEST__TEST_DEFINITION:
				setTestDefinition((TestDefinition)null);
				return;
			case Aorta_smcubesPackage.TEST__NAME:
				setName(NAME_EDEFAULT);
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
			case Aorta_smcubesPackage.TEST__INPUT_DATA:
				return inputData != null;
			case Aorta_smcubesPackage.TEST__EXPECTED_RESULTS:
				return expectedResults != null;
			case Aorta_smcubesPackage.TEST__TEST_DEFINITION:
				return testDefinition != null;
			case Aorta_smcubesPackage.TEST__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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
