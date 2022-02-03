/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Basic Derived Attribute</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicDerivedAttributeImpl#getBasicScenarioLogic <em>Basic Scenario Logic</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BasicDerivedAttributeImpl extends DerivedAttributeImpl implements BasicDerivedAttribute {
	/**
	 * The cached value of the '{@link #getBasicScenarioLogic() <em>Basic Scenario Logic</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBasicScenarioLogic()
	 * @generated
	 * @ordered
	 */
	protected EList<BasicScenarioLogic> basicScenarioLogic;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BasicDerivedAttributeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.BASIC_DERIVED_ATTRIBUTE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BasicScenarioLogic> getBasicScenarioLogic() {
		if (basicScenarioLogic == null) {
			basicScenarioLogic = new EObjectContainmentEList<BasicScenarioLogic>(BasicScenarioLogic.class, this, Ecore_plus_logicPackage.BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC);
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
			case Ecore_plus_logicPackage.BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC:
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
			case Ecore_plus_logicPackage.BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC:
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
			case Ecore_plus_logicPackage.BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC:
				getBasicScenarioLogic().clear();
				getBasicScenarioLogic().addAll((Collection<? extends BasicScenarioLogic>)newValue);
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
			case Ecore_plus_logicPackage.BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC:
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
			case Ecore_plus_logicPackage.BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC:
				return basicScenarioLogic != null && !basicScenarioLogic.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BasicDerivedAttributeImpl
