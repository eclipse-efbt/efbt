/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing.impl;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.ReportResults;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.Test;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage;

import org.eclipse.emf.common.notify.Notification;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl#getReportResults <em>Report Results</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl#getScenario <em>Scenario</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestImpl extends MinimalEObjectImpl.Container implements Test {
	/**
	 * The cached value of the '{@link #getInputData() <em>Input Data</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputData()
	 * @generated
	 * @ordered
	 */
	protected InputData inputData;

	/**
	 * The cached value of the '{@link #getReportResults() <em>Report Results</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportResults()
	 * @generated
	 * @ordered
	 */
	protected ReportResults reportResults;

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
	 * The cached value of the '{@link #getScenario() <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenario()
	 * @generated
	 * @ordered
	 */
	protected Scenario scenario;

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
	@Override
	public InputData getInputData() {
		if (inputData != null && inputData.eIsProxy()) {
			InternalEObject oldInputData = (InternalEObject)inputData;
			inputData = (InputData)eResolveProxy(oldInputData);
			if (inputData != oldInputData) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestingPackage.TEST__INPUT_DATA, oldInputData, inputData));
			}
		}
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InputData basicGetInputData() {
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setInputData(InputData newInputData) {
		InputData oldInputData = inputData;
		inputData = newInputData;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.TEST__INPUT_DATA, oldInputData, inputData));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportResults getReportResults() {
		if (reportResults != null && reportResults.eIsProxy()) {
			InternalEObject oldReportResults = (InternalEObject)reportResults;
			reportResults = (ReportResults)eResolveProxy(oldReportResults);
			if (reportResults != oldReportResults) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestingPackage.TEST__REPORT_RESULTS, oldReportResults, reportResults));
			}
		}
		return reportResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportResults basicGetReportResults() {
		return reportResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setReportResults(ReportResults newReportResults) {
		ReportResults oldReportResults = reportResults;
		reportResults = newReportResults;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.TEST__REPORT_RESULTS, oldReportResults, reportResults));
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
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.TEST__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Scenario getScenario() {
		if (scenario != null && scenario.eIsProxy()) {
			InternalEObject oldScenario = (InternalEObject)scenario;
			scenario = (Scenario)eResolveProxy(oldScenario);
			if (scenario != oldScenario) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestingPackage.TEST__SCENARIO, oldScenario, scenario));
			}
		}
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario basicGetScenario() {
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setScenario(Scenario newScenario) {
		Scenario oldScenario = scenario;
		scenario = newScenario;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.TEST__SCENARIO, oldScenario, scenario));
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
				if (resolve) return getInputData();
				return basicGetInputData();
			case TestingPackage.TEST__REPORT_RESULTS:
				if (resolve) return getReportResults();
				return basicGetReportResults();
			case TestingPackage.TEST__NAME:
				return getName();
			case TestingPackage.TEST__SCENARIO:
				if (resolve) return getScenario();
				return basicGetScenario();
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
			case TestingPackage.TEST__INPUT_DATA:
				setInputData((InputData)newValue);
				return;
			case TestingPackage.TEST__REPORT_RESULTS:
				setReportResults((ReportResults)newValue);
				return;
			case TestingPackage.TEST__NAME:
				setName((String)newValue);
				return;
			case TestingPackage.TEST__SCENARIO:
				setScenario((Scenario)newValue);
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
				setInputData((InputData)null);
				return;
			case TestingPackage.TEST__REPORT_RESULTS:
				setReportResults((ReportResults)null);
				return;
			case TestingPackage.TEST__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TestingPackage.TEST__SCENARIO:
				setScenario((Scenario)null);
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
				return inputData != null;
			case TestingPackage.TEST__REPORT_RESULTS:
				return reportResults != null;
			case TestingPackage.TEST__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TestingPackage.TEST__SCENARIO:
				return scenario != null;
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
