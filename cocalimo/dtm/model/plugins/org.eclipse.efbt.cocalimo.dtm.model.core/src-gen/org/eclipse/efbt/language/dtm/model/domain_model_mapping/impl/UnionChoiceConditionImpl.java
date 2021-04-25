/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Union Choice Condition</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.impl.UnionChoiceConditionImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.impl.UnionChoiceConditionImpl#getChosenCubes <em>Chosen Cubes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class UnionChoiceConditionImpl extends CodeConditionImpl implements UnionChoiceCondition {
	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube cube;

	/**
	 * The cached value of the '{@link #getChosenCubes() <em>Chosen Cubes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getChosenCubes()
	 * @generated
	 * @ordered
	 */
	protected EList<FreeBirdToolsCube> chosenCubes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected UnionChoiceConditionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Domain_model_mappingPackage.Literals.UNION_CHOICE_CONDITION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getCube() {
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (FreeBirdToolsCube)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetCube() {
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube(FreeBirdToolsCube newCube) {
		FreeBirdToolsCube oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<FreeBirdToolsCube> getChosenCubes() {
		if (chosenCubes == null) {
			chosenCubes = new EObjectResolvingEList<FreeBirdToolsCube>(FreeBirdToolsCube.class, this, Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CHOSEN_CUBES);
		}
		return chosenCubes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CHOSEN_CUBES:
				return getChosenCubes();
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
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CUBE:
				setCube((FreeBirdToolsCube)newValue);
				return;
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CHOSEN_CUBES:
				getChosenCubes().clear();
				getChosenCubes().addAll((Collection<? extends FreeBirdToolsCube>)newValue);
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
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CUBE:
				setCube((FreeBirdToolsCube)null);
				return;
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CHOSEN_CUBES:
				getChosenCubes().clear();
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
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CUBE:
				return cube != null;
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION__CHOSEN_CUBES:
				return chosenCubes != null && !chosenCubes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //UnionChoiceConditionImpl
