/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL Scheme</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLSchemeImpl#getExpressions <em>Expressions</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLSchemeImpl#getScheme_id <em>Scheme id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLSchemeImpl extends MinimalEObjectImpl.Container implements VTLScheme {
	/**
	 * The cached value of the '{@link #getExpressions() <em>Expressions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpressions()
	 * @generated
	 * @ordered
	 */
	protected EList<VTLTransformation> expressions;

	/**
	 * The default value of the '{@link #getScheme_id() <em>Scheme id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScheme_id()
	 * @generated
	 * @ordered
	 */
	protected static final String SCHEME_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getScheme_id() <em>Scheme id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScheme_id()
	 * @generated
	 * @ordered
	 */
	protected String scheme_id = SCHEME_ID_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLSchemeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLScheme();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VTLTransformation> getExpressions() {
		if (expressions == null) {
			expressions = new EObjectContainmentEList<VTLTransformation>(VTLTransformation.class, this, Ecore4regPackage.VTL_SCHEME__EXPRESSIONS);
		}
		return expressions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getScheme_id() {
		return scheme_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setScheme_id(String newScheme_id) {
		String oldScheme_id = scheme_id;
		scheme_id = newScheme_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_SCHEME__SCHEME_ID, oldScheme_id, scheme_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.VTL_SCHEME__EXPRESSIONS:
				return ((InternalEList<?>)getExpressions()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.VTL_SCHEME__EXPRESSIONS:
				return getExpressions();
			case Ecore4regPackage.VTL_SCHEME__SCHEME_ID:
				return getScheme_id();
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
			case Ecore4regPackage.VTL_SCHEME__EXPRESSIONS:
				getExpressions().clear();
				getExpressions().addAll((Collection<? extends VTLTransformation>)newValue);
				return;
			case Ecore4regPackage.VTL_SCHEME__SCHEME_ID:
				setScheme_id((String)newValue);
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
			case Ecore4regPackage.VTL_SCHEME__EXPRESSIONS:
				getExpressions().clear();
				return;
			case Ecore4regPackage.VTL_SCHEME__SCHEME_ID:
				setScheme_id(SCHEME_ID_EDEFAULT);
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
			case Ecore4regPackage.VTL_SCHEME__EXPRESSIONS:
				return expressions != null && !expressions.isEmpty();
			case Ecore4regPackage.VTL_SCHEME__SCHEME_ID:
				return SCHEME_ID_EDEFAULT == null ? scheme_id != null : !SCHEME_ID_EDEFAULT.equals(scheme_id);
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
		result.append(" (scheme_id: ");
		result.append(scheme_id);
		result.append(')');
		return result.toString();
	}

} //VTLSchemeImpl
