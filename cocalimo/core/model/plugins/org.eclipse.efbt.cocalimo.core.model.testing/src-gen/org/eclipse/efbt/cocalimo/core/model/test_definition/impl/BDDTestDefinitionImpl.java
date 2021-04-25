/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Given;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Then;
import org.eclipse.efbt.cocalimo.core.model.test_definition.When;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>BDD Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl#getTestContraints <em>Test Contraints</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl#getGiven <em>Given</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl#getWhen <em>When</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestDefinitionImpl#getThen <em>Then</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BDDTestDefinitionImpl extends TestDefinitionImpl implements BDDTestDefinition
{
	/**
	 * The cached value of the '{@link #getTestContraints() <em>Test Contraints</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestContraints()
	 * @generated
	 * @ordered
	 */
	protected BDDTestContraints testContraints;

	/**
	 * The cached value of the '{@link #getGiven() <em>Given</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGiven()
	 * @generated
	 * @ordered
	 */
	protected Given given;

	/**
	 * The cached value of the '{@link #getWhen() <em>When</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWhen()
	 * @generated
	 * @ordered
	 */
	protected When when;

	/**
	 * The cached value of the '{@link #getThen() <em>Then</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getThen()
	 * @generated
	 * @ordered
	 */
	protected Then then;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BDDTestDefinitionImpl()
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
		return Test_definitionPackage.Literals.BDD_TEST_DEFINITION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestContraints getTestContraints()
	{
		if (testContraints != null && testContraints.eIsProxy()) {
			InternalEObject oldTestContraints = (InternalEObject)testContraints;
			testContraints = (BDDTestContraints)eResolveProxy(oldTestContraints);
			if (testContraints != oldTestContraints) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Test_definitionPackage.BDD_TEST_DEFINITION__TEST_CONTRAINTS, oldTestContraints, testContraints));
			}
		}
		return testContraints;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestContraints basicGetTestContraints()
	{
		return testContraints;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTestContraints(BDDTestContraints newTestContraints)
	{
		BDDTestContraints oldTestContraints = testContraints;
		testContraints = newTestContraints;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_DEFINITION__TEST_CONTRAINTS, oldTestContraints, testContraints));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Given getGiven()
	{
		return given;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetGiven(Given newGiven, NotificationChain msgs)
	{
		Given oldGiven = given;
		given = newGiven;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN, oldGiven, newGiven);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setGiven(Given newGiven)
	{
		if (newGiven != given) {
			NotificationChain msgs = null;
			if (given != null)
				msgs = ((InternalEObject)given).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN, null, msgs);
			if (newGiven != null)
				msgs = ((InternalEObject)newGiven).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN, null, msgs);
			msgs = basicSetGiven(newGiven, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN, newGiven, newGiven));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public When getWhen()
	{
		return when;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetWhen(When newWhen, NotificationChain msgs)
	{
		When oldWhen = when;
		when = newWhen;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_DEFINITION__WHEN, oldWhen, newWhen);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setWhen(When newWhen)
	{
		if (newWhen != when) {
			NotificationChain msgs = null;
			if (when != null)
				msgs = ((InternalEObject)when).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_DEFINITION__WHEN, null, msgs);
			if (newWhen != null)
				msgs = ((InternalEObject)newWhen).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_DEFINITION__WHEN, null, msgs);
			msgs = basicSetWhen(newWhen, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_DEFINITION__WHEN, newWhen, newWhen));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Then getThen()
	{
		return then;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetThen(Then newThen, NotificationChain msgs)
	{
		Then oldThen = then;
		then = newThen;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_DEFINITION__THEN, oldThen, newThen);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setThen(Then newThen)
	{
		if (newThen != then) {
			NotificationChain msgs = null;
			if (then != null)
				msgs = ((InternalEObject)then).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_DEFINITION__THEN, null, msgs);
			if (newThen != null)
				msgs = ((InternalEObject)newThen).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_DEFINITION__THEN, null, msgs);
			msgs = basicSetThen(newThen, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_DEFINITION__THEN, newThen, newThen));
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
			case Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN:
				return basicSetGiven(null, msgs);
			case Test_definitionPackage.BDD_TEST_DEFINITION__WHEN:
				return basicSetWhen(null, msgs);
			case Test_definitionPackage.BDD_TEST_DEFINITION__THEN:
				return basicSetThen(null, msgs);
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
			case Test_definitionPackage.BDD_TEST_DEFINITION__TEST_CONTRAINTS:
				if (resolve) return getTestContraints();
				return basicGetTestContraints();
			case Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN:
				return getGiven();
			case Test_definitionPackage.BDD_TEST_DEFINITION__WHEN:
				return getWhen();
			case Test_definitionPackage.BDD_TEST_DEFINITION__THEN:
				return getThen();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Test_definitionPackage.BDD_TEST_DEFINITION__TEST_CONTRAINTS:
				setTestContraints((BDDTestContraints)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN:
				setGiven((Given)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_DEFINITION__WHEN:
				setWhen((When)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_DEFINITION__THEN:
				setThen((Then)newValue);
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
			case Test_definitionPackage.BDD_TEST_DEFINITION__TEST_CONTRAINTS:
				setTestContraints((BDDTestContraints)null);
				return;
			case Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN:
				setGiven((Given)null);
				return;
			case Test_definitionPackage.BDD_TEST_DEFINITION__WHEN:
				setWhen((When)null);
				return;
			case Test_definitionPackage.BDD_TEST_DEFINITION__THEN:
				setThen((Then)null);
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
			case Test_definitionPackage.BDD_TEST_DEFINITION__TEST_CONTRAINTS:
				return testContraints != null;
			case Test_definitionPackage.BDD_TEST_DEFINITION__GIVEN:
				return given != null;
			case Test_definitionPackage.BDD_TEST_DEFINITION__WHEN:
				return when != null;
			case Test_definitionPackage.BDD_TEST_DEFINITION__THEN:
				return then != null;
		}
		return super.eIsSet(featureID);
	}

} //BDDTestDefinitionImpl
