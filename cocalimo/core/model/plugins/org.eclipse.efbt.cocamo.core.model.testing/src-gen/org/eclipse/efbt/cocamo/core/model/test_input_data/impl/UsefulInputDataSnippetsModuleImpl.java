/**
 */
package org.eclipse.efbt.cocamo.core.model.test_input_data.impl;

import java.util.Collection;

import org.eclipse.efbt.cocamo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocamo.core.model.test_input_data.Test_input_dataPackage;
import org.eclipse.efbt.cocamo.core.model.test_input_data.UsefulInputDataSnippets;
import org.eclipse.efbt.cocamo.core.model.test_input_data.UsefulInputDataSnippetsModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Useful Input Data Snippets Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.core.model.test_input_data.impl.UsefulInputDataSnippetsModuleImpl#getSnippets <em>Snippets</em>}</li>
 * </ul>
 *
 * @generated
 */
public class UsefulInputDataSnippetsModuleImpl extends ModuleImpl implements UsefulInputDataSnippetsModule
{
	/**
	 * The cached value of the '{@link #getSnippets() <em>Snippets</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSnippets()
	 * @generated
	 * @ordered
	 */
	protected EList<UsefulInputDataSnippets> snippets;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected UsefulInputDataSnippetsModuleImpl()
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
		return Test_input_dataPackage.Literals.USEFUL_INPUT_DATA_SNIPPETS_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<UsefulInputDataSnippets> getSnippets()
	{
		if (snippets == null)
		{
			snippets = new EObjectContainmentEList<UsefulInputDataSnippets>(UsefulInputDataSnippets.class, this, Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS);
		}
		return snippets;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS:
				return ((InternalEList<?>)getSnippets()).basicRemove(otherEnd, msgs);
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
		switch (featureID)
		{
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS:
				return getSnippets();
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
		switch (featureID)
		{
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS:
				getSnippets().clear();
				getSnippets().addAll((Collection<? extends UsefulInputDataSnippets>)newValue);
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
		switch (featureID)
		{
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS:
				getSnippets().clear();
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
		switch (featureID)
		{
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS:
				return snippets != null && !snippets.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //UsefulInputDataSnippetsModuleImpl
