/**
 */
package bird_model.impl;

import bird_model.Bird_modelPackage;
import bird_model.FACET_COLLECTION;
import bird_model.FACET_ENUMERATION;
import bird_model.facet_type;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>FACET ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link bird_model.impl.FACET_ENUMERATIONImpl#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link bird_model.impl.FACET_ENUMERATIONImpl#getFacet_type <em>Facet type</em>}</li>
 *   <li>{@link bird_model.impl.FACET_ENUMERATIONImpl#getObservation_value <em>Observation value</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FACET_ENUMERATIONImpl extends MinimalEObjectImpl.Container implements FACET_ENUMERATION {
	/**
	 * The cached value of the '{@link #getFacet_id() <em>Facet id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_id()
	 * @generated
	 * @ordered
	 */
	protected FACET_COLLECTION facet_id;

	/**
	 * The cached value of the '{@link #getFacet_type() <em>Facet type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_type()
	 * @generated
	 * @ordered
	 */
	protected facet_type facet_type;

	/**
	 * The default value of the '{@link #getObservation_value() <em>Observation value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getObservation_value()
	 * @generated
	 * @ordered
	 */
	protected static final String OBSERVATION_VALUE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getObservation_value() <em>Observation value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getObservation_value()
	 * @generated
	 * @ordered
	 */
	protected String observation_value = OBSERVATION_VALUE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FACET_ENUMERATIONImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bird_modelPackage.Literals.FACET_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_COLLECTION getFacet_id() {
		if (facet_id != null && facet_id.eIsProxy()) {
			InternalEObject oldFacet_id = (InternalEObject)facet_id;
			facet_id = (FACET_COLLECTION)eResolveProxy(oldFacet_id);
			if (facet_id != oldFacet_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.FACET_ENUMERATION__FACET_ID, oldFacet_id, facet_id));
			}
		}
		return facet_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_COLLECTION basicGetFacet_id() {
		return facet_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFacet_id(FACET_COLLECTION newFacet_id) {
		FACET_COLLECTION oldFacet_id = facet_id;
		facet_id = newFacet_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.FACET_ENUMERATION__FACET_ID, oldFacet_id, facet_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public facet_type getFacet_type() {
		if (facet_type != null && facet_type.eIsProxy()) {
			InternalEObject oldFacet_type = (InternalEObject)facet_type;
			facet_type = (facet_type)eResolveProxy(oldFacet_type);
			if (facet_type != oldFacet_type) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.FACET_ENUMERATION__FACET_TYPE, oldFacet_type, facet_type));
			}
		}
		return facet_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public facet_type basicGetFacet_type() {
		return facet_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFacet_type(facet_type newFacet_type) {
		facet_type oldFacet_type = facet_type;
		facet_type = newFacet_type;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.FACET_ENUMERATION__FACET_TYPE, oldFacet_type, facet_type));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getObservation_value() {
		return observation_value;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setObservation_value(String newObservation_value) {
		String oldObservation_value = observation_value;
		observation_value = newObservation_value;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.FACET_ENUMERATION__OBSERVATION_VALUE, oldObservation_value, observation_value));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bird_modelPackage.FACET_ENUMERATION__FACET_ID:
				if (resolve) return getFacet_id();
				return basicGetFacet_id();
			case Bird_modelPackage.FACET_ENUMERATION__FACET_TYPE:
				if (resolve) return getFacet_type();
				return basicGetFacet_type();
			case Bird_modelPackage.FACET_ENUMERATION__OBSERVATION_VALUE:
				return getObservation_value();
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
			case Bird_modelPackage.FACET_ENUMERATION__FACET_ID:
				setFacet_id((FACET_COLLECTION)newValue);
				return;
			case Bird_modelPackage.FACET_ENUMERATION__FACET_TYPE:
				setFacet_type((facet_type)newValue);
				return;
			case Bird_modelPackage.FACET_ENUMERATION__OBSERVATION_VALUE:
				setObservation_value((String)newValue);
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
			case Bird_modelPackage.FACET_ENUMERATION__FACET_ID:
				setFacet_id((FACET_COLLECTION)null);
				return;
			case Bird_modelPackage.FACET_ENUMERATION__FACET_TYPE:
				setFacet_type((facet_type)null);
				return;
			case Bird_modelPackage.FACET_ENUMERATION__OBSERVATION_VALUE:
				setObservation_value(OBSERVATION_VALUE_EDEFAULT);
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
			case Bird_modelPackage.FACET_ENUMERATION__FACET_ID:
				return facet_id != null;
			case Bird_modelPackage.FACET_ENUMERATION__FACET_TYPE:
				return facet_type != null;
			case Bird_modelPackage.FACET_ENUMERATION__OBSERVATION_VALUE:
				return OBSERVATION_VALUE_EDEFAULT == null ? observation_value != null : !OBSERVATION_VALUE_EDEFAULT.equals(observation_value);
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
		result.append(" (observation_value: ");
		result.append(observation_value);
		result.append(')');
		return result.toString();
	}

} //FACET_ENUMERATIONImpl
