/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.ELClass;
import org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart;
import org.eclipse.efbt.regdna.model.regdna.RulesForILTable;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

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
 * An implementation of the model object '<em><b>Rules For IL Table</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RulesForILTableImpl#getRulesForTablePart <em>Rules For Table Part</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RulesForILTableImpl#getInputLayerTable <em>Input Layer Table</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RulesForILTableImpl extends MinimalEObjectImpl.Container implements RulesForILTable {
	/**
	 * The cached value of the '{@link #getRulesForTablePart() <em>Rules For Table Part</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRulesForTablePart()
	 * @generated
	 * @ordered
	 */
	protected EList<RuleForILTablePart> rulesForTablePart;

	/**
	 * The cached value of the '{@link #getInputLayerTable() <em>Input Layer Table</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputLayerTable()
	 * @generated
	 * @ordered
	 */
	protected ELClass inputLayerTable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RulesForILTableImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getRulesForILTable();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RuleForILTablePart> getRulesForTablePart() {
		if (rulesForTablePart == null) {
			rulesForTablePart = new EObjectContainmentEList<RuleForILTablePart>(RuleForILTablePart.class, this, regdnaPackage.RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART);
		}
		return rulesForTablePart;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClass getInputLayerTable() {
		if (inputLayerTable != null && inputLayerTable.eIsProxy()) {
			InternalEObject oldInputLayerTable = (InternalEObject)inputLayerTable;
			inputLayerTable = (ELClass)eResolveProxy(oldInputLayerTable);
			if (inputLayerTable != oldInputLayerTable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE, oldInputLayerTable, inputLayerTable));
			}
		}
		return inputLayerTable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClass basicGetInputLayerTable() {
		return inputLayerTable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setInputLayerTable(ELClass newInputLayerTable) {
		ELClass oldInputLayerTable = inputLayerTable;
		inputLayerTable = newInputLayerTable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE, oldInputLayerTable, inputLayerTable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART:
				return ((InternalEList<?>)getRulesForTablePart()).basicRemove(otherEnd, msgs);
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
			case regdnaPackage.RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART:
				return getRulesForTablePart();
			case regdnaPackage.RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE:
				if (resolve) return getInputLayerTable();
				return basicGetInputLayerTable();
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
			case regdnaPackage.RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART:
				getRulesForTablePart().clear();
				getRulesForTablePart().addAll((Collection<? extends RuleForILTablePart>)newValue);
				return;
			case regdnaPackage.RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE:
				setInputLayerTable((ELClass)newValue);
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
			case regdnaPackage.RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART:
				getRulesForTablePart().clear();
				return;
			case regdnaPackage.RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE:
				setInputLayerTable((ELClass)null);
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
			case regdnaPackage.RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART:
				return rulesForTablePart != null && !rulesForTablePart.isEmpty();
			case regdnaPackage.RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE:
				return inputLayerTable != null;
		}
		return super.eIsSet(featureID);
	}

} //RulesForILTableImpl
