/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Entity To VTL Intermediate Layer Link Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkModuleImpl#getEntityToVTLIntermediateLayerLinks <em>Entity To VTL Intermediate Layer Links</em>}</li>
 * </ul>
 *
 * @generated
 */
public class EntityToVTLIntermediateLayerLinkModuleImpl extends ModuleImpl implements EntityToVTLIntermediateLayerLinkModule {
	/**
	 * The cached value of the '{@link #getEntityToVTLIntermediateLayerLinks() <em>Entity To VTL Intermediate Layer Links</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntityToVTLIntermediateLayerLinks()
	 * @generated
	 * @ordered
	 */
	protected EList<EntityToVTLIntermediateLayerLink> entityToVTLIntermediateLayerLinks;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected EntityToVTLIntermediateLayerLinkModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getEntityToVTLIntermediateLayerLinkModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EntityToVTLIntermediateLayerLink> getEntityToVTLIntermediateLayerLinks() {
		if (entityToVTLIntermediateLayerLinks == null) {
			entityToVTLIntermediateLayerLinks = new EObjectContainmentEList<EntityToVTLIntermediateLayerLink>(EntityToVTLIntermediateLayerLink.class, this, Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS);
		}
		return entityToVTLIntermediateLayerLinks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				return ((InternalEList<?>)getEntityToVTLIntermediateLayerLinks()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				return getEntityToVTLIntermediateLayerLinks();
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
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				getEntityToVTLIntermediateLayerLinks().clear();
				getEntityToVTLIntermediateLayerLinks().addAll((Collection<? extends EntityToVTLIntermediateLayerLink>)newValue);
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
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				getEntityToVTLIntermediateLayerLinks().clear();
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
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS:
				return entityToVTLIntermediateLayerLinks != null && !entityToVTLIntermediateLayerLinks.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //EntityToVTLIntermediateLayerLinkModuleImpl
