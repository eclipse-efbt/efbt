/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Function Spec Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecModuleImpl#getFunctionSpecs <em>Function Specs</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FunctionSpecModuleImpl extends ModuleImpl implements FunctionSpecModule {
	/**
	 * The cached value of the '{@link #getFunctionSpecs() <em>Function Specs</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionSpecs()
	 * @generated
	 * @ordered
	 */
	protected EList<FunctionSpec> functionSpecs;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FunctionSpecModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.FUNCTION_SPEC_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<FunctionSpec> getFunctionSpecs() {
		if (functionSpecs == null) {
			functionSpecs = new EObjectContainmentEList<FunctionSpec>(FunctionSpec.class, this, Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE__FUNCTION_SPECS);
		}
		return functionSpecs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE__FUNCTION_SPECS:
				return ((InternalEList<?>)getFunctionSpecs()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE__FUNCTION_SPECS:
				return getFunctionSpecs();
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE__FUNCTION_SPECS:
				getFunctionSpecs().clear();
				getFunctionSpecs().addAll((Collection<? extends FunctionSpec>)newValue);
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE__FUNCTION_SPECS:
				getFunctionSpecs().clear();
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC_MODULE__FUNCTION_SPECS:
				return functionSpecs != null && !functionSpecs.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //FunctionSpecModuleImpl
