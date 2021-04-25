/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Array Typed Variable Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableModuleImpl#getArrayTypedVariables <em>Array Typed Variables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ArrayTypedVariableModuleImpl extends ModuleImpl implements ArrayTypedVariableModule
{
	/**
	 * The cached value of the '{@link #getArrayTypedVariables() <em>Array Typed Variables</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getArrayTypedVariables()
	 * @generated
	 * @ordered
	 */
	protected EList<ArrayTypedVariable> arrayTypedVariables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ArrayTypedVariableModuleImpl()
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
		return Efbt_advanced_data_definitionPackage.Literals.ARRAY_TYPED_VARIABLE_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ArrayTypedVariable> getArrayTypedVariables()
	{
		if (arrayTypedVariables == null) {
			arrayTypedVariables = new EObjectContainmentEList<ArrayTypedVariable>(ArrayTypedVariable.class, this, Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES);
		}
		return arrayTypedVariables;
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES:
				return ((InternalEList<?>)getArrayTypedVariables()).basicRemove(otherEnd, msgs);
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES:
				return getArrayTypedVariables();
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES:
				getArrayTypedVariables().clear();
				getArrayTypedVariables().addAll((Collection<? extends ArrayTypedVariable>)newValue);
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES:
				getArrayTypedVariables().clear();
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES:
				return arrayTypedVariables != null && !arrayTypedVariables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ArrayTypedVariableModuleImpl
