/**
 */
package org.eclipse.efbt.openregspecs.model.bpmn_lite.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.openregspecs.model.bpmn_lite.ScriptTask;

import org.eclipse.efbt.openregspecs.model.data_meta_model.GeneratedEntity;

import org.eclipse.efbt.openregspecs.model.testing.SelectionLayer;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Script Task</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.ScriptTaskImpl#getSelectionLayers <em>Selection Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.ScriptTaskImpl#getOutputLayer <em>Output Layer</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ScriptTaskImpl extends TaskImpl implements ScriptTask {
	/**
	 * The cached value of the '{@link #getSelectionLayers() <em>Selection Layers</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSelectionLayers()
	 * @generated
	 * @ordered
	 */
	protected EList<SelectionLayer> selectionLayers;

	/**
	 * The cached value of the '{@link #getOutputLayer() <em>Output Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutputLayer()
	 * @generated
	 * @ordered
	 */
	protected GeneratedEntity outputLayer;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ScriptTaskImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bpmn_litePackage.Literals.SCRIPT_TASK;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SelectionLayer> getSelectionLayers() {
		if (selectionLayers == null) {
			selectionLayers = new EObjectContainmentEList<SelectionLayer>(SelectionLayer.class, this, Bpmn_litePackage.SCRIPT_TASK__SELECTION_LAYERS);
		}
		return selectionLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntity getOutputLayer() {
		if (outputLayer != null && outputLayer.eIsProxy()) {
			InternalEObject oldOutputLayer = (InternalEObject)outputLayer;
			outputLayer = (GeneratedEntity)eResolveProxy(oldOutputLayer);
			if (outputLayer != oldOutputLayer) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bpmn_litePackage.SCRIPT_TASK__OUTPUT_LAYER, oldOutputLayer, outputLayer));
			}
		}
		return outputLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntity basicGetOutputLayer() {
		return outputLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOutputLayer(GeneratedEntity newOutputLayer) {
		GeneratedEntity oldOutputLayer = outputLayer;
		outputLayer = newOutputLayer;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bpmn_litePackage.SCRIPT_TASK__OUTPUT_LAYER, oldOutputLayer, outputLayer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Bpmn_litePackage.SCRIPT_TASK__SELECTION_LAYERS:
				return ((InternalEList<?>)getSelectionLayers()).basicRemove(otherEnd, msgs);
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
			case Bpmn_litePackage.SCRIPT_TASK__SELECTION_LAYERS:
				return getSelectionLayers();
			case Bpmn_litePackage.SCRIPT_TASK__OUTPUT_LAYER:
				if (resolve) return getOutputLayer();
				return basicGetOutputLayer();
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
			case Bpmn_litePackage.SCRIPT_TASK__SELECTION_LAYERS:
				getSelectionLayers().clear();
				getSelectionLayers().addAll((Collection<? extends SelectionLayer>)newValue);
				return;
			case Bpmn_litePackage.SCRIPT_TASK__OUTPUT_LAYER:
				setOutputLayer((GeneratedEntity)newValue);
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
			case Bpmn_litePackage.SCRIPT_TASK__SELECTION_LAYERS:
				getSelectionLayers().clear();
				return;
			case Bpmn_litePackage.SCRIPT_TASK__OUTPUT_LAYER:
				setOutputLayer((GeneratedEntity)null);
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
			case Bpmn_litePackage.SCRIPT_TASK__SELECTION_LAYERS:
				return selectionLayers != null && !selectionLayers.isEmpty();
			case Bpmn_litePackage.SCRIPT_TASK__OUTPUT_LAYER:
				return outputLayer != null;
		}
		return super.eIsSet(featureID);
	}

} //ScriptTaskImpl
