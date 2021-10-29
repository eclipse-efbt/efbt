/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test Definition Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionModuleImpl#getTestDefinitions <em>Test Definitions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestDefinitionModuleImpl extends ModuleImpl implements TestDefinitionModule {
	/**
	 * The cached value of the '{@link #getTestDefinitions() <em>Test Definitions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestDefinitions()
	 * @generated
	 * @ordered
	 */
	protected EList<TestDefinition> testDefinitions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestDefinitionModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Aorta_smcubesPackage.Literals.TEST_DEFINITION_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TestDefinition> getTestDefinitions() {
		if (testDefinitions == null) {
			testDefinitions = new EObjectContainmentEList<TestDefinition>(TestDefinition.class, this, Aorta_smcubesPackage.TEST_DEFINITION_MODULE__TEST_DEFINITIONS);
		}
		return testDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Aorta_smcubesPackage.TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
				return ((InternalEList<?>)getTestDefinitions()).basicRemove(otherEnd, msgs);
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
			case Aorta_smcubesPackage.TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
				return getTestDefinitions();
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
			case Aorta_smcubesPackage.TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
				getTestDefinitions().clear();
				getTestDefinitions().addAll((Collection<? extends TestDefinition>)newValue);
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
			case Aorta_smcubesPackage.TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
				getTestDefinitions().clear();
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
			case Aorta_smcubesPackage.TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
				return testDefinitions != null && !testDefinitions.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //TestDefinitionModuleImpl
