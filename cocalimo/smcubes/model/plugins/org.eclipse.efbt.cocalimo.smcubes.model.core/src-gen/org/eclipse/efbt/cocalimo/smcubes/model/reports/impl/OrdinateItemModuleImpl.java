/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.OrdinateItemModule;
import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Ordinate Item Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.impl.OrdinateItemModuleImpl#getOrdinateItems <em>Ordinate Items</em>}</li>
 * </ul>
 *
 * @generated
 */
public class OrdinateItemModuleImpl extends ModuleImpl implements OrdinateItemModule {
	/**
	 * The cached value of the '{@link #getOrdinateItems() <em>Ordinate Items</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrdinateItems()
	 * @generated
	 * @ordered
	 */
	protected EList<ORDINATE_ITEM> ordinateItems;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected OrdinateItemModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ReportsPackage.Literals.ORDINATE_ITEM_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ORDINATE_ITEM> getOrdinateItems() {
		if (ordinateItems == null) {
			ordinateItems = new EObjectContainmentEList<ORDINATE_ITEM>(ORDINATE_ITEM.class, this, ReportsPackage.ORDINATE_ITEM_MODULE__ORDINATE_ITEMS);
		}
		return ordinateItems;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case ReportsPackage.ORDINATE_ITEM_MODULE__ORDINATE_ITEMS:
				return ((InternalEList<?>)getOrdinateItems()).basicRemove(otherEnd, msgs);
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
			case ReportsPackage.ORDINATE_ITEM_MODULE__ORDINATE_ITEMS:
				return getOrdinateItems();
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
			case ReportsPackage.ORDINATE_ITEM_MODULE__ORDINATE_ITEMS:
				getOrdinateItems().clear();
				getOrdinateItems().addAll((Collection<? extends ORDINATE_ITEM>)newValue);
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
			case ReportsPackage.ORDINATE_ITEM_MODULE__ORDINATE_ITEMS:
				getOrdinateItems().clear();
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
			case ReportsPackage.ORDINATE_ITEM_MODULE__ORDINATE_ITEMS:
				return ordinateItems != null && !ordinateItems.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //OrdinateItemModuleImpl
