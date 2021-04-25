/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Reg Functionality Test Definition Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.RegFunctionalityTestDefinitionModuleImpl#getTestDefinitions <em>Test Definitions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RegFunctionalityTestDefinitionModuleImpl extends ModuleImpl implements RegFunctionalityTestDefinitionModule
{
	/**
	 * The cached value of the '{@link #getTestDefinitions() <em>Test Definitions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestDefinitions()
	 * @generated
	 * @ordered
	 */
	protected EList<RegFunctionalityTestDefinition> testDefinitions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RegFunctionalityTestDefinitionModuleImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Test_definitionPackage.Literals.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<RegFunctionalityTestDefinition> getTestDefinitions()
	{
		if (testDefinitions == null) {
			testDefinitions = new EObjectContainmentEList<RegFunctionalityTestDefinition>(RegFunctionalityTestDefinition.class, this, Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS);
		}
		return testDefinitions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
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
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
				getTestDefinitions().clear();
				getTestDefinitions().addAll((Collection<? extends RegFunctionalityTestDefinition>)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case Test_definitionPackage.REG_FUNCTIONALITY_TEST_DEFINITION_MODULE__TEST_DEFINITIONS:
				return testDefinitions != null && !testDefinitions.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //RegFunctionalityTestDefinitionModuleImpl
