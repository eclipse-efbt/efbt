/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.EOperationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl#getCreatedCubeColumns <em>Created Cube Columns</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RowCreationApproachImpl extends EOperationImpl implements RowCreationApproach {
	/**
	 * The cached value of the '{@link #getCreatedCubeColumns() <em>Created Cube Columns</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCreatedCubeColumns()
	 * @generated
	 * @ordered
	 */
	protected EList<EAttribute> createdCubeColumns;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RowCreationApproachImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EAttribute> getCreatedCubeColumns() {
		if (createdCubeColumns == null) {
			createdCubeColumns = new EObjectContainmentEList<EAttribute>(EAttribute.class, this, Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS);
		}
		return createdCubeColumns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				return ((InternalEList<?>)getCreatedCubeColumns()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				return getCreatedCubeColumns();
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				getCreatedCubeColumns().clear();
				getCreatedCubeColumns().addAll((Collection<? extends EAttribute>)newValue);
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				getCreatedCubeColumns().clear();
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				return createdCubeColumns != null && !createdCubeColumns.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //RowCreationApproachImpl
