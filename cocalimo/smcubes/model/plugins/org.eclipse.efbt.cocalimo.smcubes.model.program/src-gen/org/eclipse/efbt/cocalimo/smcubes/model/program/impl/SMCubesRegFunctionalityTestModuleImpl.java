/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.test.impl.TestModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTestModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SM Cubes Reg Functionality Test Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestModuleImpl#getRegFunctionalityTests <em>Reg Functionality Tests</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesRegFunctionalityTestModuleImpl extends TestModuleImpl implements SMCubesRegFunctionalityTestModule {
	/**
	 * The cached value of the '{@link #getRegFunctionalityTests() <em>Reg Functionality Tests</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRegFunctionalityTests()
	 * @generated
	 * @ordered
	 */
	protected EList<SMCubesRegFunctionalityTest> regFunctionalityTests;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SMCubesRegFunctionalityTestModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ProgramPackage.Literals.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SMCubesRegFunctionalityTest> getRegFunctionalityTests() {
		if (regFunctionalityTests == null) {
			regFunctionalityTests = new EObjectContainmentEList<SMCubesRegFunctionalityTest>(SMCubesRegFunctionalityTest.class, this, ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS);
		}
		return regFunctionalityTests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS:
				return ((InternalEList<?>)getRegFunctionalityTests()).basicRemove(otherEnd, msgs);
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
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS:
				return getRegFunctionalityTests();
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
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS:
				getRegFunctionalityTests().clear();
				getRegFunctionalityTests().addAll((Collection<? extends SMCubesRegFunctionalityTest>)newValue);
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
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS:
				getRegFunctionalityTests().clear();
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
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS:
				return regFunctionalityTests != null && !regFunctionalityTests.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //SMCubesRegFunctionalityTestModuleImpl
