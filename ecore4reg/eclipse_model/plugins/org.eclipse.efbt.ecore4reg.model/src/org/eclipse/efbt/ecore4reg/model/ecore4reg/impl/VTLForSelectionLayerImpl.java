/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL For Selection Layer</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerImpl#getSelectionLayer <em>Selection Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerImpl#getOutputLayer <em>Output Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerImpl#getIntermediateLayer <em>Intermediate Layer</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLForSelectionLayerImpl extends MinimalEObjectImpl.Container implements VTLForSelectionLayer {
	/**
	 * The cached value of the '{@link #getSelectionLayer() <em>Selection Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSelectionLayer()
	 * @generated
	 * @ordered
	 */
	protected RuleForILTablePart selectionLayer;

	/**
	 * The cached value of the '{@link #getOutputLayer() <em>Output Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutputLayer()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedOutputlayer outputLayer;

	/**
	 * The cached value of the '{@link #getIntermediateLayer() <em>Intermediate Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIntermediateLayer()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedIntermediateLayer intermediateLayer;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLForSelectionLayerImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLForSelectionLayer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RuleForILTablePart getSelectionLayer() {
		if (selectionLayer != null && selectionLayer.eIsProxy()) {
			InternalEObject oldSelectionLayer = (InternalEObject)selectionLayer;
			selectionLayer = (RuleForILTablePart)eResolveProxy(oldSelectionLayer);
			if (selectionLayer != oldSelectionLayer) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_FOR_SELECTION_LAYER__SELECTION_LAYER, oldSelectionLayer, selectionLayer));
			}
		}
		return selectionLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RuleForILTablePart basicGetSelectionLayer() {
		return selectionLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSelectionLayer(RuleForILTablePart newSelectionLayer) {
		RuleForILTablePart oldSelectionLayer = selectionLayer;
		selectionLayer = newSelectionLayer;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_SELECTION_LAYER__SELECTION_LAYER, oldSelectionLayer, selectionLayer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedOutputlayer getOutputLayer() {
		if (outputLayer != null && outputLayer.eIsProxy()) {
			InternalEObject oldOutputLayer = (InternalEObject)outputLayer;
			outputLayer = (VTLGeneratedOutputlayer)eResolveProxy(oldOutputLayer);
			if (outputLayer != oldOutputLayer) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_FOR_SELECTION_LAYER__OUTPUT_LAYER, oldOutputLayer, outputLayer));
			}
		}
		return outputLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedOutputlayer basicGetOutputLayer() {
		return outputLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOutputLayer(VTLGeneratedOutputlayer newOutputLayer) {
		VTLGeneratedOutputlayer oldOutputLayer = outputLayer;
		outputLayer = newOutputLayer;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_SELECTION_LAYER__OUTPUT_LAYER, oldOutputLayer, outputLayer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayer getIntermediateLayer() {
		if (intermediateLayer != null && intermediateLayer.eIsProxy()) {
			InternalEObject oldIntermediateLayer = (InternalEObject)intermediateLayer;
			intermediateLayer = (VTLGeneratedIntermediateLayer)eResolveProxy(oldIntermediateLayer);
			if (intermediateLayer != oldIntermediateLayer) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_FOR_SELECTION_LAYER__INTERMEDIATE_LAYER, oldIntermediateLayer, intermediateLayer));
			}
		}
		return intermediateLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayer basicGetIntermediateLayer() {
		return intermediateLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIntermediateLayer(VTLGeneratedIntermediateLayer newIntermediateLayer) {
		VTLGeneratedIntermediateLayer oldIntermediateLayer = intermediateLayer;
		intermediateLayer = newIntermediateLayer;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_SELECTION_LAYER__INTERMEDIATE_LAYER, oldIntermediateLayer, intermediateLayer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__SELECTION_LAYER:
				if (resolve) return getSelectionLayer();
				return basicGetSelectionLayer();
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__OUTPUT_LAYER:
				if (resolve) return getOutputLayer();
				return basicGetOutputLayer();
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__INTERMEDIATE_LAYER:
				if (resolve) return getIntermediateLayer();
				return basicGetIntermediateLayer();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__SELECTION_LAYER:
				setSelectionLayer((RuleForILTablePart)newValue);
				return;
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__OUTPUT_LAYER:
				setOutputLayer((VTLGeneratedOutputlayer)newValue);
				return;
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__INTERMEDIATE_LAYER:
				setIntermediateLayer((VTLGeneratedIntermediateLayer)newValue);
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
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__SELECTION_LAYER:
				setSelectionLayer((RuleForILTablePart)null);
				return;
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__OUTPUT_LAYER:
				setOutputLayer((VTLGeneratedOutputlayer)null);
				return;
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__INTERMEDIATE_LAYER:
				setIntermediateLayer((VTLGeneratedIntermediateLayer)null);
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
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__SELECTION_LAYER:
				return selectionLayer != null;
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__OUTPUT_LAYER:
				return outputLayer != null;
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER__INTERMEDIATE_LAYER:
				return intermediateLayer != null;
		}
		return super.eIsSet(featureID);
	}

} //VTLForSelectionLayerImpl
