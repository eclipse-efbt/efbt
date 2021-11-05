/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Combination Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CombinationModuleImpl#getCombinations <em>Combinations</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CombinationModuleImpl#getCombination_items <em>Combination items</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CombinationModuleImpl#getCubeToCombinationLinks <em>Cube To Combination Links</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CombinationModuleImpl extends ModuleImpl implements CombinationModule {
	/**
	 * The cached value of the '{@link #getCombinations() <em>Combinations</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCombinations()
	 * @generated
	 * @ordered
	 */
	protected EList<COMBINATION> combinations;

	/**
	 * The cached value of the '{@link #getCombination_items() <em>Combination items</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCombination_items()
	 * @generated
	 * @ordered
	 */
	protected EList<COMBINATION_ITEM> combination_items;

	/**
	 * The cached value of the '{@link #getCubeToCombinationLinks() <em>Cube To Combination Links</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeToCombinationLinks()
	 * @generated
	 * @ordered
	 */
	protected EList<CUBE_TO_COMBINATION> cubeToCombinationLinks;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CombinationModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Efbt_data_definitionPackage.Literals.COMBINATION_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<COMBINATION> getCombinations() {
		if (combinations == null) {
			combinations = new EObjectContainmentEList<COMBINATION>(COMBINATION.class, this, Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATIONS);
		}
		return combinations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<COMBINATION_ITEM> getCombination_items() {
		if (combination_items == null) {
			combination_items = new EObjectContainmentEList<COMBINATION_ITEM>(COMBINATION_ITEM.class, this, Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATION_ITEMS);
		}
		return combination_items;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<CUBE_TO_COMBINATION> getCubeToCombinationLinks() {
		if (cubeToCombinationLinks == null) {
			cubeToCombinationLinks = new EObjectContainmentEList<CUBE_TO_COMBINATION>(CUBE_TO_COMBINATION.class, this, Efbt_data_definitionPackage.COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS);
		}
		return cubeToCombinationLinks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATIONS:
				return ((InternalEList<?>)getCombinations()).basicRemove(otherEnd, msgs);
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATION_ITEMS:
				return ((InternalEList<?>)getCombination_items()).basicRemove(otherEnd, msgs);
			case Efbt_data_definitionPackage.COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS:
				return ((InternalEList<?>)getCubeToCombinationLinks()).basicRemove(otherEnd, msgs);
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
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATIONS:
				return getCombinations();
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATION_ITEMS:
				return getCombination_items();
			case Efbt_data_definitionPackage.COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS:
				return getCubeToCombinationLinks();
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
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATIONS:
				getCombinations().clear();
				getCombinations().addAll((Collection<? extends COMBINATION>)newValue);
				return;
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATION_ITEMS:
				getCombination_items().clear();
				getCombination_items().addAll((Collection<? extends COMBINATION_ITEM>)newValue);
				return;
			case Efbt_data_definitionPackage.COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS:
				getCubeToCombinationLinks().clear();
				getCubeToCombinationLinks().addAll((Collection<? extends CUBE_TO_COMBINATION>)newValue);
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
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATIONS:
				getCombinations().clear();
				return;
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATION_ITEMS:
				getCombination_items().clear();
				return;
			case Efbt_data_definitionPackage.COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS:
				getCubeToCombinationLinks().clear();
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
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATIONS:
				return combinations != null && !combinations.isEmpty();
			case Efbt_data_definitionPackage.COMBINATION_MODULE__COMBINATION_ITEMS:
				return combination_items != null && !combination_items.isEmpty();
			case Efbt_data_definitionPackage.COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS:
				return cubeToCombinationLinks != null && !cubeToCombinationLinks.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //CombinationModuleImpl
