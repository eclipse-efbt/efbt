/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl#getVTLSchemes <em>VTL Schemes</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl#getVTLGeneratedOutputLayers <em>VTL Generated Output Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl#getVTLGeneratedIntermediateLayers <em>VTL Generated Intermediate Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl#getVTLEnrichedLayers <em>VTL Enriched Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl#getVTLForSelectionLayers <em>VTL For Selection Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl#getEntityToVTLIntermediateLayerLinks <em>Entity To VTL Intermediate Layer Links</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl#getVTLForViews <em>VTL For Views</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLModuleImpl extends ModuleImpl implements VTLModule {
	/**
	 * The cached value of the '{@link #getVTLSchemes() <em>VTL Schemes</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLSchemes()
	 * @generated
	 * @ordered
	 */
	protected VTLSchemesModule vtlSchemes;

	/**
	 * The cached value of the '{@link #getVTLGeneratedOutputLayers() <em>VTL Generated Output Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLGeneratedOutputLayers()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedOutputlayerModule vtlGeneratedOutputLayers;

	/**
	 * The cached value of the '{@link #getVTLGeneratedIntermediateLayers() <em>VTL Generated Intermediate Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLGeneratedIntermediateLayers()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedIntermediateLayerModule vtlGeneratedIntermediateLayers;

	/**
	 * The cached value of the '{@link #getVTLEnrichedLayers() <em>VTL Enriched Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLEnrichedLayers()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedIntermediateLayerModule vtlEnrichedLayers;

	/**
	 * The cached value of the '{@link #getVTLForSelectionLayers() <em>VTL For Selection Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLForSelectionLayers()
	 * @generated
	 * @ordered
	 */
	protected VTLForSelectionLayerModule vtlForSelectionLayers;

	/**
	 * The cached value of the '{@link #getEntityToVTLIntermediateLayerLinks() <em>Entity To VTL Intermediate Layer Links</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntityToVTLIntermediateLayerLinks()
	 * @generated
	 * @ordered
	 */
	protected EntityToVTLIntermediateLayerLinkModule entityToVTLIntermediateLayerLinks;

	/**
	 * The cached value of the '{@link #getVTLForViews() <em>VTL For Views</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLForViews()
	 * @generated
	 * @ordered
	 */
	protected VTLForViewModule vtlForViews;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLSchemesModule getVTLSchemes() {
		return vtlSchemes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetVTLSchemes(VTLSchemesModule newVTLSchemes, NotificationChain msgs) {
		VTLSchemesModule oldVTLSchemes = vtlSchemes;
		vtlSchemes = newVTLSchemes;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_SCHEMES, oldVTLSchemes, newVTLSchemes);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVTLSchemes(VTLSchemesModule newVTLSchemes) {
		if (newVTLSchemes != vtlSchemes) {
			NotificationChain msgs = null;
			if (vtlSchemes != null)
				msgs = ((InternalEObject)vtlSchemes).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_SCHEMES, null, msgs);
			if (newVTLSchemes != null)
				msgs = ((InternalEObject)newVTLSchemes).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_SCHEMES, null, msgs);
			msgs = basicSetVTLSchemes(newVTLSchemes, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_SCHEMES, newVTLSchemes, newVTLSchemes));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedOutputlayerModule getVTLGeneratedOutputLayers() {
		return vtlGeneratedOutputLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetVTLGeneratedOutputLayers(VTLGeneratedOutputlayerModule newVTLGeneratedOutputLayers, NotificationChain msgs) {
		VTLGeneratedOutputlayerModule oldVTLGeneratedOutputLayers = vtlGeneratedOutputLayers;
		vtlGeneratedOutputLayers = newVTLGeneratedOutputLayers;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS, oldVTLGeneratedOutputLayers, newVTLGeneratedOutputLayers);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVTLGeneratedOutputLayers(VTLGeneratedOutputlayerModule newVTLGeneratedOutputLayers) {
		if (newVTLGeneratedOutputLayers != vtlGeneratedOutputLayers) {
			NotificationChain msgs = null;
			if (vtlGeneratedOutputLayers != null)
				msgs = ((InternalEObject)vtlGeneratedOutputLayers).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS, null, msgs);
			if (newVTLGeneratedOutputLayers != null)
				msgs = ((InternalEObject)newVTLGeneratedOutputLayers).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS, null, msgs);
			msgs = basicSetVTLGeneratedOutputLayers(newVTLGeneratedOutputLayers, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS, newVTLGeneratedOutputLayers, newVTLGeneratedOutputLayers));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayerModule getVTLGeneratedIntermediateLayers() {
		return vtlGeneratedIntermediateLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetVTLGeneratedIntermediateLayers(VTLGeneratedIntermediateLayerModule newVTLGeneratedIntermediateLayers, NotificationChain msgs) {
		VTLGeneratedIntermediateLayerModule oldVTLGeneratedIntermediateLayers = vtlGeneratedIntermediateLayers;
		vtlGeneratedIntermediateLayers = newVTLGeneratedIntermediateLayers;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS, oldVTLGeneratedIntermediateLayers, newVTLGeneratedIntermediateLayers);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVTLGeneratedIntermediateLayers(VTLGeneratedIntermediateLayerModule newVTLGeneratedIntermediateLayers) {
		if (newVTLGeneratedIntermediateLayers != vtlGeneratedIntermediateLayers) {
			NotificationChain msgs = null;
			if (vtlGeneratedIntermediateLayers != null)
				msgs = ((InternalEObject)vtlGeneratedIntermediateLayers).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS, null, msgs);
			if (newVTLGeneratedIntermediateLayers != null)
				msgs = ((InternalEObject)newVTLGeneratedIntermediateLayers).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS, null, msgs);
			msgs = basicSetVTLGeneratedIntermediateLayers(newVTLGeneratedIntermediateLayers, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS, newVTLGeneratedIntermediateLayers, newVTLGeneratedIntermediateLayers));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayerModule getVTLEnrichedLayers() {
		return vtlEnrichedLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetVTLEnrichedLayers(VTLGeneratedIntermediateLayerModule newVTLEnrichedLayers, NotificationChain msgs) {
		VTLGeneratedIntermediateLayerModule oldVTLEnrichedLayers = vtlEnrichedLayers;
		vtlEnrichedLayers = newVTLEnrichedLayers;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS, oldVTLEnrichedLayers, newVTLEnrichedLayers);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVTLEnrichedLayers(VTLGeneratedIntermediateLayerModule newVTLEnrichedLayers) {
		if (newVTLEnrichedLayers != vtlEnrichedLayers) {
			NotificationChain msgs = null;
			if (vtlEnrichedLayers != null)
				msgs = ((InternalEObject)vtlEnrichedLayers).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS, null, msgs);
			if (newVTLEnrichedLayers != null)
				msgs = ((InternalEObject)newVTLEnrichedLayers).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS, null, msgs);
			msgs = basicSetVTLEnrichedLayers(newVTLEnrichedLayers, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS, newVTLEnrichedLayers, newVTLEnrichedLayers));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForSelectionLayerModule getVTLForSelectionLayers() {
		return vtlForSelectionLayers;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetVTLForSelectionLayers(VTLForSelectionLayerModule newVTLForSelectionLayers, NotificationChain msgs) {
		VTLForSelectionLayerModule oldVTLForSelectionLayers = vtlForSelectionLayers;
		vtlForSelectionLayers = newVTLForSelectionLayers;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS, oldVTLForSelectionLayers, newVTLForSelectionLayers);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVTLForSelectionLayers(VTLForSelectionLayerModule newVTLForSelectionLayers) {
		if (newVTLForSelectionLayers != vtlForSelectionLayers) {
			NotificationChain msgs = null;
			if (vtlForSelectionLayers != null)
				msgs = ((InternalEObject)vtlForSelectionLayers).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS, null, msgs);
			if (newVTLForSelectionLayers != null)
				msgs = ((InternalEObject)newVTLForSelectionLayers).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS, null, msgs);
			msgs = basicSetVTLForSelectionLayers(newVTLForSelectionLayers, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS, newVTLForSelectionLayers, newVTLForSelectionLayers));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityToVTLIntermediateLayerLinkModule getEntityToVTLIntermediateLayerLinks() {
		return entityToVTLIntermediateLayerLinks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetEntityToVTLIntermediateLayerLinks(EntityToVTLIntermediateLayerLinkModule newEntityToVTLIntermediateLayerLinks, NotificationChain msgs) {
		EntityToVTLIntermediateLayerLinkModule oldEntityToVTLIntermediateLayerLinks = entityToVTLIntermediateLayerLinks;
		entityToVTLIntermediateLayerLinks = newEntityToVTLIntermediateLayerLinks;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS, oldEntityToVTLIntermediateLayerLinks, newEntityToVTLIntermediateLayerLinks);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEntityToVTLIntermediateLayerLinks(EntityToVTLIntermediateLayerLinkModule newEntityToVTLIntermediateLayerLinks) {
		if (newEntityToVTLIntermediateLayerLinks != entityToVTLIntermediateLayerLinks) {
			NotificationChain msgs = null;
			if (entityToVTLIntermediateLayerLinks != null)
				msgs = ((InternalEObject)entityToVTLIntermediateLayerLinks).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS, null, msgs);
			if (newEntityToVTLIntermediateLayerLinks != null)
				msgs = ((InternalEObject)newEntityToVTLIntermediateLayerLinks).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS, null, msgs);
			msgs = basicSetEntityToVTLIntermediateLayerLinks(newEntityToVTLIntermediateLayerLinks, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS, newEntityToVTLIntermediateLayerLinks, newEntityToVTLIntermediateLayerLinks));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForViewModule getVTLForViews() {
		return vtlForViews;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetVTLForViews(VTLForViewModule newVTLForViews, NotificationChain msgs) {
		VTLForViewModule oldVTLForViews = vtlForViews;
		vtlForViews = newVTLForViews;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS, oldVTLForViews, newVTLForViews);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVTLForViews(VTLForViewModule newVTLForViews) {
		if (newVTLForViews != vtlForViews) {
			NotificationChain msgs = null;
			if (vtlForViews != null)
				msgs = ((InternalEObject)vtlForViews).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS, null, msgs);
			if (newVTLForViews != null)
				msgs = ((InternalEObject)newVTLForViews).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS, null, msgs);
			msgs = basicSetVTLForViews(newVTLForViews, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS, newVTLForViews, newVTLForViews));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.VTL_MODULE__VTL_SCHEMES:
				return basicSetVTLSchemes(null, msgs);
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS:
				return basicSetVTLGeneratedOutputLayers(null, msgs);
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				return basicSetVTLGeneratedIntermediateLayers(null, msgs);
			case Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS:
				return basicSetVTLEnrichedLayers(null, msgs);
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS:
				return basicSetVTLForSelectionLayers(null, msgs);
			case Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				return basicSetEntityToVTLIntermediateLayerLinks(null, msgs);
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS:
				return basicSetVTLForViews(null, msgs);
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
			case Ecore4regPackage.VTL_MODULE__VTL_SCHEMES:
				return getVTLSchemes();
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS:
				return getVTLGeneratedOutputLayers();
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				return getVTLGeneratedIntermediateLayers();
			case Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS:
				return getVTLEnrichedLayers();
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS:
				return getVTLForSelectionLayers();
			case Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				return getEntityToVTLIntermediateLayerLinks();
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS:
				return getVTLForViews();
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
			case Ecore4regPackage.VTL_MODULE__VTL_SCHEMES:
				setVTLSchemes((VTLSchemesModule)newValue);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS:
				setVTLGeneratedOutputLayers((VTLGeneratedOutputlayerModule)newValue);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				setVTLGeneratedIntermediateLayers((VTLGeneratedIntermediateLayerModule)newValue);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS:
				setVTLEnrichedLayers((VTLGeneratedIntermediateLayerModule)newValue);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS:
				setVTLForSelectionLayers((VTLForSelectionLayerModule)newValue);
				return;
			case Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				setEntityToVTLIntermediateLayerLinks((EntityToVTLIntermediateLayerLinkModule)newValue);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS:
				setVTLForViews((VTLForViewModule)newValue);
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
			case Ecore4regPackage.VTL_MODULE__VTL_SCHEMES:
				setVTLSchemes((VTLSchemesModule)null);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS:
				setVTLGeneratedOutputLayers((VTLGeneratedOutputlayerModule)null);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				setVTLGeneratedIntermediateLayers((VTLGeneratedIntermediateLayerModule)null);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS:
				setVTLEnrichedLayers((VTLGeneratedIntermediateLayerModule)null);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS:
				setVTLForSelectionLayers((VTLForSelectionLayerModule)null);
				return;
			case Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				setEntityToVTLIntermediateLayerLinks((EntityToVTLIntermediateLayerLinkModule)null);
				return;
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS:
				setVTLForViews((VTLForViewModule)null);
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
			case Ecore4regPackage.VTL_MODULE__VTL_SCHEMES:
				return vtlSchemes != null;
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS:
				return vtlGeneratedOutputLayers != null;
			case Ecore4regPackage.VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS:
				return vtlGeneratedIntermediateLayers != null;
			case Ecore4regPackage.VTL_MODULE__VTL_ENRICHED_LAYERS:
				return vtlEnrichedLayers != null;
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_SELECTION_LAYERS:
				return vtlForSelectionLayers != null;
			case Ecore4regPackage.VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				return entityToVTLIntermediateLayerLinks != null;
			case Ecore4regPackage.VTL_MODULE__VTL_FOR_VIEWS:
				return vtlForViews != null;
		}
		return super.eIsSet(featureID);
	}

} //VTLModuleImpl
