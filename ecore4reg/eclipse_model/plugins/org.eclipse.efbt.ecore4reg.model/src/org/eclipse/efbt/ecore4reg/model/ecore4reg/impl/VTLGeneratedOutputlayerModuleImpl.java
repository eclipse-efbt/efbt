/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL Generated Outputlayer Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedOutputlayerModuleImpl#getVTLGeneratedOutputlayerModules <em>VTL Generated Outputlayer Modules</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLGeneratedOutputlayerModuleImpl extends ModuleImpl implements VTLGeneratedOutputlayerModule {
	/**
	 * The cached value of the '{@link #getVTLGeneratedOutputlayerModules() <em>VTL Generated Outputlayer Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVTLGeneratedOutputlayerModules()
	 * @generated
	 * @ordered
	 */
	protected EList<VTLGeneratedOutputlayer> vTLGeneratedOutputlayerModules;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLGeneratedOutputlayerModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLGeneratedOutputlayerModule();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLGeneratedOutputlayer> getVTLGeneratedOutputlayerModules() {
		if (vTLGeneratedOutputlayerModules == null) {
			vTLGeneratedOutputlayerModules = new EObjectContainmentEList<VTLGeneratedOutputlayer>(VTLGeneratedOutputlayer.class, this, Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE__VTL_GENERATED_OUTPUTLAYER_MODULES);
		}
		return vTLGeneratedOutputlayerModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE__VTL_GENERATED_OUTPUTLAYER_MODULES:
				return ((InternalEList<?>)getVTLGeneratedOutputlayerModules()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE__VTL_GENERATED_OUTPUTLAYER_MODULES:
				return getVTLGeneratedOutputlayerModules();
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
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE__VTL_GENERATED_OUTPUTLAYER_MODULES:
				getVTLGeneratedOutputlayerModules().clear();
				getVTLGeneratedOutputlayerModules().addAll((Collection<? extends VTLGeneratedOutputlayer>)newValue);
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
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE__VTL_GENERATED_OUTPUTLAYER_MODULES:
				getVTLGeneratedOutputlayerModules().clear();
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
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE__VTL_GENERATED_OUTPUTLAYER_MODULES:
				return vTLGeneratedOutputlayerModules != null && !vTLGeneratedOutputlayerModules.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VTLGeneratedOutputlayerModuleImpl
