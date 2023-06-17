/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Generation Rules Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.GenerationRulesModuleImpl#getRulesForReport <em>Rules For Report</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GenerationRulesModuleImpl extends ModuleImpl implements GenerationRulesModule {
	/**
	 * The cached value of the '{@link #getRulesForReport() <em>Rules For Report</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRulesForReport()
	 * @generated
	 * @ordered
	 */
	protected EList<RulesForReport> rulesForReport;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GenerationRulesModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getGenerationRulesModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RulesForReport> getRulesForReport() {
		if (rulesForReport == null) {
			rulesForReport = new EObjectContainmentEList<RulesForReport>(RulesForReport.class, this, Ecore4regPackage.GENERATION_RULES_MODULE__RULES_FOR_REPORT);
		}
		return rulesForReport;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.GENERATION_RULES_MODULE__RULES_FOR_REPORT:
				return ((InternalEList<?>)getRulesForReport()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.GENERATION_RULES_MODULE__RULES_FOR_REPORT:
				return getRulesForReport();
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
			case Ecore4regPackage.GENERATION_RULES_MODULE__RULES_FOR_REPORT:
				getRulesForReport().clear();
				getRulesForReport().addAll((Collection<? extends RulesForReport>)newValue);
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
			case Ecore4regPackage.GENERATION_RULES_MODULE__RULES_FOR_REPORT:
				getRulesForReport().clear();
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
			case Ecore4regPackage.GENERATION_RULES_MODULE__RULES_FOR_REPORT:
				return rulesForReport != null && !rulesForReport.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //GenerationRulesModuleImpl
