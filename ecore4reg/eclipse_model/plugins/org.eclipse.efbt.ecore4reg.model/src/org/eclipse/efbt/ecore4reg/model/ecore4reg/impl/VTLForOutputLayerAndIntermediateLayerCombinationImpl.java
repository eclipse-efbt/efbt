/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL For Output Layer And Intermediate Layer Combination</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationImpl#getTransformations <em>Transformations</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationImpl#getOutputLayer <em>Output Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationImpl#getIntermediateLayer <em>Intermediate Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLForOutputLayerAndIntermediateLayerCombinationImpl extends MinimalEObjectImpl.Container implements VTLForOutputLayerAndIntermediateLayerCombination {
	/**
	 * The cached value of the '{@link #getTransformations() <em>Transformations</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformations()
	 * @generated
	 * @ordered
	 */
	protected EList<VTLTransformation> transformations;

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
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLForOutputLayerAndIntermediateLayerCombinationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLForOutputLayerAndIntermediateLayerCombination();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLTransformation> getTransformations() {
		if (transformations == null) {
			transformations = new EObjectResolvingEList<VTLTransformation>(VTLTransformation.class, this, Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__TRANSFORMATIONS);
		}
		return transformations;
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__OUTPUT_LAYER, oldOutputLayer, outputLayer));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__OUTPUT_LAYER, oldOutputLayer, outputLayer));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__INTERMEDIATE_LAYER, oldIntermediateLayer, intermediateLayer));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__INTERMEDIATE_LAYER, oldIntermediateLayer, intermediateLayer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__TRANSFORMATIONS:
				return getTransformations();
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__OUTPUT_LAYER:
				if (resolve) return getOutputLayer();
				return basicGetOutputLayer();
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__INTERMEDIATE_LAYER:
				if (resolve) return getIntermediateLayer();
				return basicGetIntermediateLayer();
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__NAME:
				return getName();
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
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__TRANSFORMATIONS:
				getTransformations().clear();
				getTransformations().addAll((Collection<? extends VTLTransformation>)newValue);
				return;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__OUTPUT_LAYER:
				setOutputLayer((VTLGeneratedOutputlayer)newValue);
				return;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__INTERMEDIATE_LAYER:
				setIntermediateLayer((VTLGeneratedIntermediateLayer)newValue);
				return;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__NAME:
				setName((String)newValue);
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
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__TRANSFORMATIONS:
				getTransformations().clear();
				return;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__OUTPUT_LAYER:
				setOutputLayer((VTLGeneratedOutputlayer)null);
				return;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__INTERMEDIATE_LAYER:
				setIntermediateLayer((VTLGeneratedIntermediateLayer)null);
				return;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__NAME:
				setName(NAME_EDEFAULT);
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
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__TRANSFORMATIONS:
				return transformations != null && !transformations.isEmpty();
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__OUTPUT_LAYER:
				return outputLayer != null;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__INTERMEDIATE_LAYER:
				return intermediateLayer != null;
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //VTLForOutputLayerAndIntermediateLayerCombinationImpl
