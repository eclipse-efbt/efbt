/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL Generated Intermediate Layer Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerModuleImpl#getVTLGeneratedIntermediateLayers <em>VTL Generated Intermediate Layers</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLGeneratedIntermediateLayerModuleImpl extends ModuleImpl implements VTLGeneratedIntermediateLayerModule {
	/**
	 * The cached value of the '{@link #getVTLGeneratedIntermediateLayers() <em>VTL Generated Intermediate Layers</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLGeneratedIntermediateLayers()
	 * @generated
	 * @ordered
	 */
	protected EList<VTLGeneratedIntermediateLayer> vTLGeneratedIntermediateLayers;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLGeneratedIntermediateLayerModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLGeneratedIntermediateLayerModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLGeneratedIntermediateLayer> getVTLGeneratedIntermediateLayers() {
		if (vTLGeneratedIntermediateLayers == null) {
			vTLGeneratedIntermediateLayers = new EObjectContainmentEList<VTLGeneratedIntermediateLayer>(VTLGeneratedIntermediateLayer.class, this, Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS);
		}
		return vTLGeneratedIntermediateLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				return ((InternalEList<?>)getVTLGeneratedIntermediateLayers()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				return getVTLGeneratedIntermediateLayers();
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
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				getVTLGeneratedIntermediateLayers().clear();
				getVTLGeneratedIntermediateLayers().addAll((Collection<? extends VTLGeneratedIntermediateLayer>)newValue);
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
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				getVTLGeneratedIntermediateLayers().clear();
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
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				return vTLGeneratedIntermediateLayers != null && !vTLGeneratedIntermediateLayers.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VTLGeneratedIntermediateLayerModuleImpl
