/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate;
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
 * An implementation of the model object '<em><b>BDD Test Contraints</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl#getTemplate <em>Template</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl#getGivenParams <em>Given Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl#getWhenParams <em>When Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.BDDTestContraintsImpl#getThenParams <em>Then Params</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BDDTestContraintsImpl extends MinimalEObjectImpl.Container implements BDDTestContraints
{
	/**
	 * The cached value of the '{@link #getTemplate() <em>Template</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTemplate()
	 * @generated
	 * @ordered
	 */
	protected BDDTestTemplate template;

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
	protected BDDTestContraintsImpl()
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
		return Test_definitionPackage.Literals.BDD_TEST_CONTRAINTS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestTemplate getTemplate()
	{
		if (template != null && template.eIsProxy()) {
			InternalEObject oldTemplate = (InternalEObject)template;
			template = (BDDTestTemplate)eResolveProxy(oldTemplate);
			if (template != oldTemplate) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Test_definitionPackage.BDD_TEST_CONTRAINTS__TEMPLATE, oldTemplate, template));
			}
		}
		return template;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestTemplate basicGetTemplate()
	{
		return template;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTemplate(BDDTestTemplate newTemplate)
	{
		BDDTestTemplate oldTemplate = template;
		template = newTemplate;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_CONTRAINTS__TEMPLATE, oldTemplate, template));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.BDD_TEST_CONTRAINTS__NAME, oldName, name));
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
			givenParams = new EObjectContainmentEList<Param>(Param.class, this, Test_definitionPackage.BDD_TEST_CONTRAINTS__GIVEN_PARAMS);
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
			whenParams = new EObjectContainmentEList<Param>(Param.class, this, Test_definitionPackage.BDD_TEST_CONTRAINTS__WHEN_PARAMS);
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
			thenParams = new EObjectContainmentEList<Param>(Param.class, this, Test_definitionPackage.BDD_TEST_CONTRAINTS__THEN_PARAMS);
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
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__GIVEN_PARAMS:
				return ((InternalEList<?>)getGivenParams()).basicRemove(otherEnd, msgs);
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__WHEN_PARAMS:
				return ((InternalEList<?>)getWhenParams()).basicRemove(otherEnd, msgs);
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__THEN_PARAMS:
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
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__TEMPLATE:
				if (resolve) return getTemplate();
				return basicGetTemplate();
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__NAME:
				return getName();
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__GIVEN_PARAMS:
				return getGivenParams();
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__WHEN_PARAMS:
				return getWhenParams();
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__THEN_PARAMS:
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
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__TEMPLATE:
				setTemplate((BDDTestTemplate)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__NAME:
				setName((String)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__GIVEN_PARAMS:
				getGivenParams().clear();
				getGivenParams().addAll((Collection<? extends Param>)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__WHEN_PARAMS:
				getWhenParams().clear();
				getWhenParams().addAll((Collection<? extends Param>)newValue);
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__THEN_PARAMS:
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
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__TEMPLATE:
				setTemplate((BDDTestTemplate)null);
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__GIVEN_PARAMS:
				getGivenParams().clear();
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__WHEN_PARAMS:
				getWhenParams().clear();
				return;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__THEN_PARAMS:
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
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__TEMPLATE:
				return template != null;
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__GIVEN_PARAMS:
				return givenParams != null && !givenParams.isEmpty();
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__WHEN_PARAMS:
				return whenParams != null && !whenParams.isEmpty();
			case Test_definitionPackage.BDD_TEST_CONTRAINTS__THEN_PARAMS:
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

} //BDDTestContraintsImpl
