/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestConstraintsModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>BDD Test Constraints Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestConstraintsModuleImpl#getCoverageTestSets <em>Coverage Test Sets</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BDDTestConstraintsModuleImpl extends ModuleImpl implements BDDTestConstraintsModule
{
	/**
	 * The cached value of the '{@link #getCoverageTestSets() <em>Coverage Test Sets</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCoverageTestSets()
	 * @generated
	 * @ordered
	 */
	protected EList<BDDTestContraints> coverageTestSets;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BDDTestConstraintsModuleImpl()
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
		return Test_definitionPackage.Literals.BDD_TEST_CONSTRAINTS_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BDDTestContraints> getCoverageTestSets()
	{
		if (coverageTestSets == null) {
			coverageTestSets = new EObjectContainmentEList<BDDTestContraints>(BDDTestContraints.class, this, Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS);
		}
		return coverageTestSets;
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
			case Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS:
				return ((InternalEList<?>)getCoverageTestSets()).basicRemove(otherEnd, msgs);
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
			case Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS:
				return getCoverageTestSets();
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
			case Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS:
				getCoverageTestSets().clear();
				getCoverageTestSets().addAll((Collection<? extends BDDTestContraints>)newValue);
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
			case Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS:
				getCoverageTestSets().clear();
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
			case Test_definitionPackage.BDD_TEST_CONSTRAINTS_MODULE__COVERAGE_TEST_SETS:
				return coverageTestSets != null && !coverageTestSets.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BDDTestConstraintsModuleImpl
