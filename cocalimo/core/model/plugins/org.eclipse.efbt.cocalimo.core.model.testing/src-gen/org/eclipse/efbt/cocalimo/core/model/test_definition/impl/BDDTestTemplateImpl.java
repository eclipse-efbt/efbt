/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate;
import org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Param;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>BDD Test Template</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl#getGivenText <em>Given Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl#getWhenText <em>When Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl#getThenText <em>Then Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl#getGivenParams <em>Given Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl#getWhenParams <em>When Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestTemplateImpl#getThenParams <em>Then Params</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BDDTestTemplateImpl extends MinimalEObjectImpl.Container implements BDDTestTemplate
{
	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The cached value of the '{@link #getGivenText() <em>Given Text</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGivenText()
	 * @generated
	 * @ordered
	 */
	protected ClauseText givenText;

	/**
	 * The cached value of the '{@link #getWhenText() <em>When Text</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWhenText()
	 * @generated
	 * @ordered
	 */
	protected ClauseText whenText;

	/**
	 * The cached value of the '{@link #getThenText() <em>Then Text</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getThenText()
	 * @generated
	 * @ordered
	 */
	protected ClauseText thenText;

	/**
	 * The cached value of the '{@link #getGivenParams() <em>Given Params</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGivenParams()
	 * @generated
	 * @ordered
	 */
	protected EList<Param> givenParams;

	/**
	 * The cached value of the '{@link #getWhenParams() <em>When Params</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWhenParams()
	 * @generated
	 * @ordered
	 */
	protected EList<Param> whenParams;

	/**
	 * The cached value of the '{@link #getThenParams() <em>Then Params</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getThenParams()
	 * @generated
	 * @ordered
	 */
	protected EList<Param> thenParams;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BDDTestTemplateImpl()
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
		return Test_definitionPackage.Literals.BDD_TEST_TEMPLATE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_TEMPLATE__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ClauseText getGivenText()
	{
		return givenText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetGivenText(ClauseText newGivenText, NotificationChain msgs)
	{
		ClauseText oldGivenText = givenText;
		givenText = newGivenText;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT, oldGivenText, newGivenText);
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
	public void setGivenText(ClauseText newGivenText)
	{
		if (newGivenText != givenText) {
			NotificationChain msgs = null;
			if (givenText != null)
				msgs = ((InternalEObject)givenText).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT, null, msgs);
			if (newGivenText != null)
				msgs = ((InternalEObject)newGivenText).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT, null, msgs);
			msgs = basicSetGivenText(newGivenText, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT, newGivenText, newGivenText));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ClauseText getWhenText()
	{
		return whenText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetWhenText(ClauseText newWhenText, NotificationChain msgs)
	{
		ClauseText oldWhenText = whenText;
		whenText = newWhenText;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT, oldWhenText, newWhenText);
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
	public void setWhenText(ClauseText newWhenText)
	{
		if (newWhenText != whenText) {
			NotificationChain msgs = null;
			if (whenText != null)
				msgs = ((InternalEObject)whenText).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT, null, msgs);
			if (newWhenText != null)
				msgs = ((InternalEObject)newWhenText).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT, null, msgs);
			msgs = basicSetWhenText(newWhenText, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT, newWhenText, newWhenText));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ClauseText getThenText()
	{
		return thenText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetThenText(ClauseText newThenText, NotificationChain msgs)
	{
		ClauseText oldThenText = thenText;
		thenText = newThenText;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT, oldThenText, newThenText);
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
	public void setThenText(ClauseText newThenText)
	{
		if (newThenText != thenText) {
			NotificationChain msgs = null;
			if (thenText != null)
				msgs = ((InternalEObject)thenText).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT, null, msgs);
			if (newThenText != null)
				msgs = ((InternalEObject)newThenText).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT, null, msgs);
			msgs = basicSetThenText(newThenText, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT, newThenText, newThenText));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Param> getGivenParams()
	{
		if (givenParams == null) {
			givenParams = new EObjectContainmentEList<Param>(Param.class, this, Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_PARAMS);
		}
		return givenParams;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Param> getWhenParams()
	{
		if (whenParams == null) {
			whenParams = new EObjectContainmentEList<Param>(Param.class, this, Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_PARAMS);
		}
		return whenParams;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Param> getThenParams()
	{
		if (thenParams == null) {
			thenParams = new EObjectContainmentEList<Param>(Param.class, this, Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_PARAMS);
		}
		return thenParams;
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
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT:
				return basicSetGivenText(null, msgs);
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT:
				return basicSetWhenText(null, msgs);
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT:
				return basicSetThenText(null, msgs);
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_PARAMS:
				return ((InternalEList<?>)getGivenParams()).basicRemove(otherEnd, msgs);
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_PARAMS:
				return ((InternalEList<?>)getWhenParams()).basicRemove(otherEnd, msgs);
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_PARAMS:
				return ((InternalEList<?>)getThenParams()).basicRemove(otherEnd, msgs);
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
			case Test_definitionPackage.BDD_TEST_TEMPLATE__NAME:
				return getName();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT:
				return getGivenText();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT:
				return getWhenText();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT:
				return getThenText();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_PARAMS:
				return getGivenParams();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_PARAMS:
				return getWhenParams();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_PARAMS:
				return getThenParams();
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
			case Test_definitionPackage.BDD_TEST_TEMPLATE__NAME:
				setName((String)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT:
				setGivenText((ClauseText)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT:
				setWhenText((ClauseText)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT:
				setThenText((ClauseText)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_PARAMS:
				getGivenParams().clear();
				getGivenParams().addAll((Collection<? extends Param>)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_PARAMS:
				getWhenParams().clear();
				getWhenParams().addAll((Collection<? extends Param>)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_PARAMS:
				getThenParams().clear();
				getThenParams().addAll((Collection<? extends Param>)newValue);
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
			case Test_definitionPackage.BDD_TEST_TEMPLATE__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT:
				setGivenText((ClauseText)null);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT:
				setWhenText((ClauseText)null);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT:
				setThenText((ClauseText)null);
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_PARAMS:
				getGivenParams().clear();
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_PARAMS:
				getWhenParams().clear();
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_PARAMS:
				getThenParams().clear();
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
			case Test_definitionPackage.BDD_TEST_TEMPLATE__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT:
				return givenText != null;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT:
				return whenText != null;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT:
				return thenText != null;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_PARAMS:
				return givenParams != null && !givenParams.isEmpty();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_PARAMS:
				return whenParams != null && !whenParams.isEmpty();
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_PARAMS:
				return thenParams != null && !thenParams.isEmpty();
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //BDDTestTemplateImpl
