/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.test.impl.TestModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTest;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTestModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SM Cubes BDD Test Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesBDDTestModuleImpl#getTests <em>Tests</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesBDDTestModuleImpl extends TestModuleImpl implements SMCubesBDDTestModule
{
	/**
	 * The cached value of the '{@link #getTests() <em>Tests</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTests()
	 * @generated
	 * @ordered
	 */
	protected EList<SMCubesBDDTest> tests;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SMCubesBDDTestModuleImpl()
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
		return CocamoPackage.Literals.SM_CUBES_BDD_TEST_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SMCubesBDDTest> getTests()
	{
		if (tests == null) {
			tests = new EObjectContainmentEList<SMCubesBDDTest>(SMCubesBDDTest.class, this, CocamoPackage.SM_CUBES_BDD_TEST_MODULE__TESTS);
		}
		return tests;
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
			case CocamoPackage.SM_CUBES_BDD_TEST_MODULE__TESTS:
				return ((InternalEList<?>)getTests()).basicRemove(otherEnd, msgs);
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
			case CocamoPackage.SM_CUBES_BDD_TEST_MODULE__TESTS:
				return getTests();
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
			case CocamoPackage.SM_CUBES_BDD_TEST_MODULE__TESTS:
				getTests().clear();
				getTests().addAll((Collection<? extends SMCubesBDDTest>)newValue);
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
			case CocamoPackage.SM_CUBES_BDD_TEST_MODULE__TESTS:
				getTests().clear();
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
			case CocamoPackage.SM_CUBES_BDD_TEST_MODULE__TESTS:
				return tests != null && !tests.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //SMCubesBDDTestModuleImpl
