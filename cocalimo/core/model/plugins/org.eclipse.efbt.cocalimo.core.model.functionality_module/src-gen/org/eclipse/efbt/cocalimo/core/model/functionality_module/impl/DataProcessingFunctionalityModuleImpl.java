/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Data Processing Functionality Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.DataProcessingFunctionalityModuleImpl#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DataProcessingFunctionalityModuleImpl extends LeafFunctionalityModuleImpl implements DataProcessingFunctionalityModule {
	/**
	 * The cached value of the '{@link #getEnrichedAttribute() <em>Enriched Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEnrichedAttribute()
	 * @generated
	 * @ordered
	 */
	protected EAttribute enrichedAttribute;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DataProcessingFunctionalityModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Functionality_modulePackage.Literals.DATA_PROCESSING_FUNCTIONALITY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getEnrichedAttribute() {
		if (enrichedAttribute != null && enrichedAttribute.eIsProxy()) {
			InternalEObject oldEnrichedAttribute = (InternalEObject)enrichedAttribute;
			enrichedAttribute = (EAttribute)eResolveProxy(oldEnrichedAttribute);
			if (enrichedAttribute != oldEnrichedAttribute) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE, oldEnrichedAttribute, enrichedAttribute));
			}
		}
		return enrichedAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute basicGetEnrichedAttribute() {
		return enrichedAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEnrichedAttribute(EAttribute newEnrichedAttribute) {
		EAttribute oldEnrichedAttribute = enrichedAttribute;
		enrichedAttribute = newEnrichedAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE, oldEnrichedAttribute, enrichedAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE:
				if (resolve) return getEnrichedAttribute();
				return basicGetEnrichedAttribute();
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
			case Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE:
				setEnrichedAttribute((EAttribute)newValue);
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
			case Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE:
				setEnrichedAttribute((EAttribute)null);
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
			case Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE:
				return enrichedAttribute != null;
		}
		return super.eIsSet(featureID);
	}

} //DataProcessingFunctionalityModuleImpl
