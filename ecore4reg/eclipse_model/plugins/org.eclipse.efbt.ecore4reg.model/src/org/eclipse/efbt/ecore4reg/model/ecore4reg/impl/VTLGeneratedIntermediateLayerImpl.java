/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer;
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
 * An implementation of the model object '<em><b>VTL Generated Intermediate Layer</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerImpl#getDependant_enriched_cubes <em>Dependant enriched cubes</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerImpl#getTransformations <em>Transformations</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerImpl#isIsEnrichment <em>Is Enrichment</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLGeneratedIntermediateLayerImpl extends MinimalEObjectImpl.Container implements VTLGeneratedIntermediateLayer {
	/**
	 * The cached value of the '{@link #getDependant_enriched_cubes() <em>Dependant enriched cubes</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDependant_enriched_cubes()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedIntermediateLayer dependant_enriched_cubes;

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
	 * The default value of the '{@link #isIsEnrichment() <em>Is Enrichment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsEnrichment()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_ENRICHMENT_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIsEnrichment() <em>Is Enrichment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsEnrichment()
	 * @generated
	 * @ordered
	 */
	protected boolean isEnrichment = IS_ENRICHMENT_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLGeneratedIntermediateLayerImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLGeneratedIntermediateLayer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayer getDependant_enriched_cubes() {
		if (dependant_enriched_cubes != null && dependant_enriched_cubes.eIsProxy()) {
			InternalEObject oldDependant_enriched_cubes = (InternalEObject)dependant_enriched_cubes;
			dependant_enriched_cubes = (VTLGeneratedIntermediateLayer)eResolveProxy(oldDependant_enriched_cubes);
			if (dependant_enriched_cubes != oldDependant_enriched_cubes) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__DEPENDANT_ENRICHED_CUBES, oldDependant_enriched_cubes, dependant_enriched_cubes));
			}
		}
		return dependant_enriched_cubes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayer basicGetDependant_enriched_cubes() {
		return dependant_enriched_cubes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDependant_enriched_cubes(VTLGeneratedIntermediateLayer newDependant_enriched_cubes) {
		VTLGeneratedIntermediateLayer oldDependant_enriched_cubes = dependant_enriched_cubes;
		dependant_enriched_cubes = newDependant_enriched_cubes;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__DEPENDANT_ENRICHED_CUBES, oldDependant_enriched_cubes, dependant_enriched_cubes));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLTransformation> getTransformations() {
		if (transformations == null) {
			transformations = new EObjectResolvingEList<VTLTransformation>(VTLTransformation.class, this, Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__TRANSFORMATIONS);
		}
		return transformations;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isIsEnrichment() {
		return isEnrichment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIsEnrichment(boolean newIsEnrichment) {
		boolean oldIsEnrichment = isEnrichment;
		isEnrichment = newIsEnrichment;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__IS_ENRICHMENT, oldIsEnrichment, isEnrichment));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__DEPENDANT_ENRICHED_CUBES:
				if (resolve) return getDependant_enriched_cubes();
				return basicGetDependant_enriched_cubes();
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__TRANSFORMATIONS:
				return getTransformations();
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__NAME:
				return getName();
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__IS_ENRICHMENT:
				return isIsEnrichment();
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
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__DEPENDANT_ENRICHED_CUBES:
				setDependant_enriched_cubes((VTLGeneratedIntermediateLayer)newValue);
				return;
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__TRANSFORMATIONS:
				getTransformations().clear();
				getTransformations().addAll((Collection<? extends VTLTransformation>)newValue);
				return;
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__NAME:
				setName((String)newValue);
				return;
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__IS_ENRICHMENT:
				setIsEnrichment((Boolean)newValue);
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
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__DEPENDANT_ENRICHED_CUBES:
				setDependant_enriched_cubes((VTLGeneratedIntermediateLayer)null);
				return;
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__TRANSFORMATIONS:
				getTransformations().clear();
				return;
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__IS_ENRICHMENT:
				setIsEnrichment(IS_ENRICHMENT_EDEFAULT);
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
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__DEPENDANT_ENRICHED_CUBES:
				return dependant_enriched_cubes != null;
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__TRANSFORMATIONS:
				return transformations != null && !transformations.isEmpty();
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER__IS_ENRICHMENT:
				return isEnrichment != IS_ENRICHMENT_EDEFAULT;
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
		result.append(", isEnrichment: ");
		result.append(isEnrichment);
		result.append(')');
		return result.toString();
	}

} //VTLGeneratedIntermediateLayerImpl
