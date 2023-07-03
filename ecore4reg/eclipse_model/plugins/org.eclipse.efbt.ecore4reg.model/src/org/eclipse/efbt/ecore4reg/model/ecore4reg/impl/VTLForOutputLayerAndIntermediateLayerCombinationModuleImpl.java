/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL For Output Layer And Intermediate Layer Combination Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl#getVTLForOutputLayerAndIntermediateLayerCombinations <em>VTL For Output Layer And Intermediate Layer Combinations</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl extends ModuleImpl implements VTLForOutputLayerAndIntermediateLayerCombinationModule {
	/**
	 * The cached value of the '{@link #getVTLForOutputLayerAndIntermediateLayerCombinations() <em>VTL For Output Layer And Intermediate Layer Combinations</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLForOutputLayerAndIntermediateLayerCombinations()
	 * @generated
	 * @ordered
	 */
	protected EList<VTLForOutputLayerAndIntermediateLayerCombination> vTLForOutputLayerAndIntermediateLayerCombinations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLForOutputLayerAndIntermediateLayerCombinationModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLForOutputLayerAndIntermediateLayerCombination> getVTLForOutputLayerAndIntermediateLayerCombinations() {
		if (vTLForOutputLayerAndIntermediateLayerCombinations == null) {
			vTLForOutputLayerAndIntermediateLayerCombinations = new EObjectContainmentEList<VTLForOutputLayerAndIntermediateLayerCombination>(VTLForOutputLayerAndIntermediateLayerCombination.class, this, Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATIONS);
		}
		return vTLForOutputLayerAndIntermediateLayerCombinations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATIONS:
				return ((InternalEList<?>)getVTLForOutputLayerAndIntermediateLayerCombinations()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATIONS:
				return getVTLForOutputLayerAndIntermediateLayerCombinations();
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
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATIONS:
				getVTLForOutputLayerAndIntermediateLayerCombinations().clear();
				getVTLForOutputLayerAndIntermediateLayerCombinations().addAll((Collection<? extends VTLForOutputLayerAndIntermediateLayerCombination>)newValue);
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
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATIONS:
				getVTLForOutputLayerAndIntermediateLayerCombinations().clear();
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
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATIONS:
				return vTLForOutputLayerAndIntermediateLayerCombinations != null && !vTLForOutputLayerAndIntermediateLayerCombinations.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl
