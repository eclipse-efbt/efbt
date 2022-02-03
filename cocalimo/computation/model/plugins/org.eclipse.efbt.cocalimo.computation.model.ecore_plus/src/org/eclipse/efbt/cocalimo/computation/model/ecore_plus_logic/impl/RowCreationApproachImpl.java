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
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl#getDependantCubeColumns <em>Dependant Cube Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl#getCreatedCubeColumns <em>Created Cube Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl#getDependantStructItemColumns <em>Dependant Struct Item Columns</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RowCreationApproachImpl extends MinimalEObjectImpl.Container implements RowCreationApproach {
	/**
	 * The cached value of the '{@link #getDependantCubeColumns() <em>Dependant Cube Columns</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDependantCubeColumns()
	 * @generated
	 * @ordered
	 */
	protected EList<EStructuralFeature> dependantCubeColumns;

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
	 * The cached value of the '{@link #getDependantStructItemColumns() <em>Dependant Struct Item Columns</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDependantStructItemColumns()
	 * @generated
	 * @ordered
	 */
	protected EList<EStructuralFeature> dependantStructItemColumns;

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
	public EList<EStructuralFeature> getDependantCubeColumns() {
		if (dependantCubeColumns == null) {
			dependantCubeColumns = new EObjectContainmentEList<EStructuralFeature>(EStructuralFeature.class, this, Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS);
		}
		return dependantCubeColumns;
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
	public EList<EStructuralFeature> getDependantStructItemColumns() {
		if (dependantStructItemColumns == null) {
			dependantStructItemColumns = new EObjectContainmentEList<EStructuralFeature>(EStructuralFeature.class, this, Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS);
		}
		return dependantStructItemColumns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS:
				return ((InternalEList<?>)getDependantCubeColumns()).basicRemove(otherEnd, msgs);
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				return ((InternalEList<?>)getCreatedCubeColumns()).basicRemove(otherEnd, msgs);
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS:
				return ((InternalEList<?>)getDependantStructItemColumns()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS:
				return getDependantCubeColumns();
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				return getCreatedCubeColumns();
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS:
				return getDependantStructItemColumns();
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS:
				getDependantCubeColumns().clear();
				getDependantCubeColumns().addAll((Collection<? extends EStructuralFeature>)newValue);
				return;
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				getCreatedCubeColumns().clear();
				getCreatedCubeColumns().addAll((Collection<? extends EAttribute>)newValue);
				return;
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS:
				getDependantStructItemColumns().clear();
				getDependantStructItemColumns().addAll((Collection<? extends EStructuralFeature>)newValue);
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS:
				getDependantCubeColumns().clear();
				return;
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				getCreatedCubeColumns().clear();
				return;
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS:
				getDependantStructItemColumns().clear();
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
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS:
				return dependantCubeColumns != null && !dependantCubeColumns.isEmpty();
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS:
				return createdCubeColumns != null && !createdCubeColumns.isEmpty();
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS:
				return dependantStructItemColumns != null && !dependantStructItemColumns.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //RowCreationApproachImpl
