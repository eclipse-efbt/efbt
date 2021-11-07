/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.Test;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestModuleImpl#getSMCubesModel <em>SM Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestModuleImpl#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestModuleImpl#getLogicalTransformations <em>Logical Transformations</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestModuleImpl extends MinimalEObjectImpl.Container implements TestModule {
	/**
	 * The cached value of the '{@link #getSMCubesModel() <em>SM Cubes Model</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSMCubesModel()
	 * @generated
	 * @ordered
	 */
	protected EList<org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel> smCubesModel;

	/**
	 * The cached value of the '{@link #getTests() <em>Tests</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTests()
	 * @generated
	 * @ordered
	 */
	protected EList<Test> tests;

	/**
	 * The cached value of the '{@link #getLogicalTransformations() <em>Logical Transformations</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLogicalTransformations()
	 * @generated
	 * @ordered
	 */
	protected LogicalTransformationModule logicalTransformations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestingPackage.Literals.TEST_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel> getSMCubesModel() {
		if (smCubesModel == null) {
			smCubesModel = new EObjectResolvingEList<org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel>(org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel.class, this, TestingPackage.TEST_MODULE__SM_CUBES_MODEL);
		}
		return smCubesModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Test> getTests() {
		if (tests == null) {
			tests = new EObjectContainmentEList<Test>(Test.class, this, TestingPackage.TEST_MODULE__TESTS);
		}
		return tests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LogicalTransformationModule getLogicalTransformations() {
		if (logicalTransformations != null && logicalTransformations.eIsProxy()) {
			InternalEObject oldLogicalTransformations = (InternalEObject)logicalTransformations;
			logicalTransformations = (LogicalTransformationModule)eResolveProxy(oldLogicalTransformations);
			if (logicalTransformations != oldLogicalTransformations) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestingPackage.TEST_MODULE__LOGICAL_TRANSFORMATIONS, oldLogicalTransformations, logicalTransformations));
			}
		}
		return logicalTransformations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LogicalTransformationModule basicGetLogicalTransformations() {
		return logicalTransformations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setLogicalTransformations(LogicalTransformationModule newLogicalTransformations) {
		LogicalTransformationModule oldLogicalTransformations = logicalTransformations;
		logicalTransformations = newLogicalTransformations;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.TEST_MODULE__LOGICAL_TRANSFORMATIONS, oldLogicalTransformations, logicalTransformations));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestingPackage.TEST_MODULE__TESTS:
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
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TestingPackage.TEST_MODULE__SM_CUBES_MODEL:
				return getSMCubesModel();
			case TestingPackage.TEST_MODULE__TESTS:
				return getTests();
			case TestingPackage.TEST_MODULE__LOGICAL_TRANSFORMATIONS:
				if (resolve) return getLogicalTransformations();
				return basicGetLogicalTransformations();
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
			case TestingPackage.TEST_MODULE__SM_CUBES_MODEL:
				getSMCubesModel().clear();
				getSMCubesModel().addAll((Collection<? extends org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel>)newValue);
				return;
			case TestingPackage.TEST_MODULE__TESTS:
				getTests().clear();
				getTests().addAll((Collection<? extends Test>)newValue);
				return;
			case TestingPackage.TEST_MODULE__LOGICAL_TRANSFORMATIONS:
				setLogicalTransformations((LogicalTransformationModule)newValue);
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
			case TestingPackage.TEST_MODULE__SM_CUBES_MODEL:
				getSMCubesModel().clear();
				return;
			case TestingPackage.TEST_MODULE__TESTS:
				getTests().clear();
				return;
			case TestingPackage.TEST_MODULE__LOGICAL_TRANSFORMATIONS:
				setLogicalTransformations((LogicalTransformationModule)null);
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
			case TestingPackage.TEST_MODULE__SM_CUBES_MODEL:
				return smCubesModel != null && !smCubesModel.isEmpty();
			case TestingPackage.TEST_MODULE__TESTS:
				return tests != null && !tests.isEmpty();
			case TestingPackage.TEST_MODULE__LOGICAL_TRANSFORMATIONS:
				return logicalTransformations != null;
		}
		return super.eIsSet(featureID);
	}

} //TestModuleImpl
