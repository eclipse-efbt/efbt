/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL For Selection Layer Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerModuleImpl#getVTLForSelectionLayers <em>VTL For Selection Layers</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLForSelectionLayerModuleImpl extends ModuleImpl implements VTLForSelectionLayerModule {
	/**
	 * The cached value of the '{@link #getVTLForSelectionLayers() <em>VTL For Selection Layers</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLForSelectionLayers()
	 * @generated
	 * @ordered
	 */
	protected EList<VTLForSelectionLayer> vTLForSelectionLayers;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLForSelectionLayerModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLForSelectionLayerModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLForSelectionLayer> getVTLForSelectionLayers() {
		if (vTLForSelectionLayers == null) {
			vTLForSelectionLayers = new EObjectContainmentEList<VTLForSelectionLayer>(VTLForSelectionLayer.class, this, Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE__VTL_FOR_SELECTION_LAYERS);
		}
		return vTLForSelectionLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE__VTL_FOR_SELECTION_LAYERS:
				return ((InternalEList<?>)getVTLForSelectionLayers()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE__VTL_FOR_SELECTION_LAYERS:
				return getVTLForSelectionLayers();
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
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE__VTL_FOR_SELECTION_LAYERS:
				getVTLForSelectionLayers().clear();
				getVTLForSelectionLayers().addAll((Collection<? extends VTLForSelectionLayer>)newValue);
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
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE__VTL_FOR_SELECTION_LAYERS:
				getVTLForSelectionLayers().clear();
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
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE__VTL_FOR_SELECTION_LAYERS:
				return vTLForSelectionLayers != null && !vTLForSelectionLayers.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VTLForSelectionLayerModuleImpl
