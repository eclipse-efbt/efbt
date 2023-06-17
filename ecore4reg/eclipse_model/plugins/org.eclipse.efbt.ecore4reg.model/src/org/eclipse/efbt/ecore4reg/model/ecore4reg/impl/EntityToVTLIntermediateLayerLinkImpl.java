/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Entity To VTL Intermediate Layer Link</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkImpl#getVTLIntermediateLayer <em>VTL Intermediate Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkImpl#getEntity <em>Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkImpl#getFilter <em>Filter</em>}</li>
 * </ul>
 *
 * @generated
 */
public class EntityToVTLIntermediateLayerLinkImpl extends MinimalEObjectImpl.Container implements EntityToVTLIntermediateLayerLink {
	/**
	 * The cached value of the '{@link #getVTLIntermediateLayer() <em>VTL Intermediate Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLIntermediateLayer()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedIntermediateLayer vtlIntermediateLayer;

	/**
	 * The cached value of the '{@link #getEntity() <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntity()
	 * @generated
	 * @ordered
	 */
	protected ELClass entity;

	/**
	 * The default value of the '{@link #getFilter() <em>Filter</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFilter()
	 * @generated
	 * @ordered
	 */
	protected static final String FILTER_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getFilter() <em>Filter</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFilter()
	 * @generated
	 * @ordered
	 */
	protected String filter = FILTER_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected EntityToVTLIntermediateLayerLinkImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getEntityToVTLIntermediateLayerLink();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayer getVTLIntermediateLayer() {
		if (vtlIntermediateLayer != null && vtlIntermediateLayer.eIsProxy()) {
			InternalEObject oldVTLIntermediateLayer = (InternalEObject)vtlIntermediateLayer;
			vtlIntermediateLayer = (VTLGeneratedIntermediateLayer)eResolveProxy(oldVTLIntermediateLayer);
			if (vtlIntermediateLayer != oldVTLIntermediateLayer) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__VTL_INTERMEDIATE_LAYER, oldVTLIntermediateLayer, vtlIntermediateLayer));
			}
		}
		return vtlIntermediateLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayer basicGetVTLIntermediateLayer() {
		return vtlIntermediateLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVTLIntermediateLayer(VTLGeneratedIntermediateLayer newVTLIntermediateLayer) {
		VTLGeneratedIntermediateLayer oldVTLIntermediateLayer = vtlIntermediateLayer;
		vtlIntermediateLayer = newVTLIntermediateLayer;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__VTL_INTERMEDIATE_LAYER, oldVTLIntermediateLayer, vtlIntermediateLayer));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClass getEntity() {
		if (entity != null && entity.eIsProxy()) {
			InternalEObject oldEntity = (InternalEObject)entity;
			entity = (ELClass)eResolveProxy(oldEntity);
			if (entity != oldEntity) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__ENTITY, oldEntity, entity));
			}
		}
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClass basicGetEntity() {
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEntity(ELClass newEntity) {
		ELClass oldEntity = entity;
		entity = newEntity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__ENTITY, oldEntity, entity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getFilter() {
		return filter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFilter(String newFilter) {
		String oldFilter = filter;
		filter = newFilter;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__FILTER, oldFilter, filter));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__VTL_INTERMEDIATE_LAYER:
				if (resolve) return getVTLIntermediateLayer();
				return basicGetVTLIntermediateLayer();
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__ENTITY:
				if (resolve) return getEntity();
				return basicGetEntity();
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__FILTER:
				return getFilter();
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
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__VTL_INTERMEDIATE_LAYER:
				setVTLIntermediateLayer((VTLGeneratedIntermediateLayer)newValue);
				return;
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__ENTITY:
				setEntity((ELClass)newValue);
				return;
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__FILTER:
				setFilter((String)newValue);
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
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__VTL_INTERMEDIATE_LAYER:
				setVTLIntermediateLayer((VTLGeneratedIntermediateLayer)null);
				return;
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__ENTITY:
				setEntity((ELClass)null);
				return;
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__FILTER:
				setFilter(FILTER_EDEFAULT);
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
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__VTL_INTERMEDIATE_LAYER:
				return vtlIntermediateLayer != null;
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__ENTITY:
				return entity != null;
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__FILTER:
				return FILTER_EDEFAULT == null ? filter != null : !FILTER_EDEFAULT.equals(filter);
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
		result.append(" (filter: ");
		result.append(filter);
		result.append(')');
		return result.toString();
	}

} //EntityToVTLIntermediateLayerLinkImpl
