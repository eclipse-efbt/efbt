/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Basic Operation Scenario Set</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationScenarioSetImpl#getBasicScenarioLogic <em>Basic Scenario Logic</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BasicOperationScenarioSetImpl extends OperationImpl implements BasicOperationScenarioSet {
	/**
	 * The cached value of the '{@link #getBasicScenarioLogic() <em>Basic Scenario Logic</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBasicScenarioLogic()
	 * @generated
	 * @ordered
	 */
	protected EList<BasicScenarioOperation> basicScenarioLogic;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BasicOperationScenarioSetImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.BASIC_OPERATION_SCENARIO_SET;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BasicScenarioOperation> getBasicScenarioLogic() {
		if (basicScenarioLogic == null) {
			basicScenarioLogic = new EObjectContainmentEList<BasicScenarioOperation>(BasicScenarioOperation.class, this, Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC);
		}
		return basicScenarioLogic;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC:
				return ((InternalEList<?>)getBasicScenarioLogic()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC:
				return getBasicScenarioLogic();
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
			case Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC:
				getBasicScenarioLogic().clear();
				getBasicScenarioLogic().addAll((Collection<? extends BasicScenarioOperation>)newValue);
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
			case Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC:
				getBasicScenarioLogic().clear();
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
			case Ecore_plus_logicPackage.BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC:
				return basicScenarioLogic != null && !basicScenarioLogic.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BasicOperationScenarioSetImpl
