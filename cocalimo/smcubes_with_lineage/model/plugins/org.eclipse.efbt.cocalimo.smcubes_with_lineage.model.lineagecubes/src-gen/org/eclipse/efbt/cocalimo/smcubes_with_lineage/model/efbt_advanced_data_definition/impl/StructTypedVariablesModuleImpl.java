/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Struct Typed Variables Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariablesModuleImpl#getStructTypeVariables <em>Struct Type Variables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class StructTypedVariablesModuleImpl extends ModuleImpl implements StructTypedVariablesModule
{
	/**
	 * The cached value of the '{@link #getStructTypeVariables() <em>Struct Type Variables</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStructTypeVariables()
	 * @generated
	 * @ordered
	 */
	protected EList<StructTypedVariable> structTypeVariables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected StructTypedVariablesModuleImpl()
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
		return Efbt_advanced_data_definitionPackage.Literals.STRUCT_TYPED_VARIABLES_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<StructTypedVariable> getStructTypeVariables()
	{
		if (structTypeVariables == null) {
			structTypeVariables = new EObjectContainmentEList<StructTypedVariable>(StructTypedVariable.class, this, Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES);
		}
		return structTypeVariables;
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
			case Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES:
				return ((InternalEList<?>)getStructTypeVariables()).basicRemove(otherEnd, msgs);
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
			case Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES:
				return getStructTypeVariables();
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
			case Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES:
				getStructTypeVariables().clear();
				getStructTypeVariables().addAll((Collection<? extends StructTypedVariable>)newValue);
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
			case Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES:
				getStructTypeVariables().clear();
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
			case Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES:
				return structTypeVariables != null && !structTypeVariables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //StructTypedVariablesModuleImpl
