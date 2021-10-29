/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.Scenario;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestDefinitionImpl extends MinimalEObjectImpl.Container implements TestDefinition {
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
	protected TestDefinitionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Aorta_smcubesPackage.Literals.TEST_DEFINITION;
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.TEST_DEFINITION__SCENARIO, oldScenario, scenario));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST_DEFINITION__SCENARIO, oldScenario, scenario));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST_DEFINITION__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIO:
				if (resolve) return getScenario();
				return basicGetScenario();
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
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
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIO:
				setScenario((Scenario)newValue);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
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
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIO:
				setScenario((Scenario)null);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
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
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIO:
				return scenario != null;
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
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

} //TestDefinitionImpl
