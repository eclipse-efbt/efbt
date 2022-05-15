/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.openregspecs.model.requirements_text.AllowedTypes;
import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule;
import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;
import org.eclipse.efbt.openregspecs.model.requirements_text.TitledRequirementsSection;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Requirements Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.impl.RequirementsModuleImpl#getRules <em>Rules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.impl.RequirementsModuleImpl#getAllowedtypes <em>Allowedtypes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RequirementsModuleImpl extends ModuleImpl implements RequirementsModule {
	/**
	 * The cached value of the '{@link #getRules() <em>Rules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRules()
	 * @generated
	 * @ordered
	 */
	protected EList<TitledRequirementsSection> rules;

	/**
	 * The cached value of the '{@link #getAllowedtypes() <em>Allowedtypes</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAllowedtypes()
	 * @generated
	 * @ordered
	 */
	protected AllowedTypes allowedtypes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RequirementsModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Requirements_textPackage.Literals.REQUIREMENTS_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TitledRequirementsSection> getRules() {
		if (rules == null) {
			rules = new EObjectContainmentEList<TitledRequirementsSection>(TitledRequirementsSection.class, this, Requirements_textPackage.REQUIREMENTS_MODULE__RULES);
		}
		return rules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AllowedTypes getAllowedtypes() {
		return allowedtypes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetAllowedtypes(AllowedTypes newAllowedtypes, NotificationChain msgs) {
		AllowedTypes oldAllowedtypes = allowedtypes;
		allowedtypes = newAllowedtypes;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES, oldAllowedtypes, newAllowedtypes);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAllowedtypes(AllowedTypes newAllowedtypes) {
		if (newAllowedtypes != allowedtypes) {
			NotificationChain msgs = null;
			if (allowedtypes != null)
				msgs = ((InternalEObject)allowedtypes).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES, null, msgs);
			if (newAllowedtypes != null)
				msgs = ((InternalEObject)newAllowedtypes).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES, null, msgs);
			msgs = basicSetAllowedtypes(newAllowedtypes, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES, newAllowedtypes, newAllowedtypes));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Requirements_textPackage.REQUIREMENTS_MODULE__RULES:
				return ((InternalEList<?>)getRules()).basicRemove(otherEnd, msgs);
			case Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES:
				return basicSetAllowedtypes(null, msgs);
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
			case Requirements_textPackage.REQUIREMENTS_MODULE__RULES:
				return getRules();
			case Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES:
				return getAllowedtypes();
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
			case Requirements_textPackage.REQUIREMENTS_MODULE__RULES:
				getRules().clear();
				getRules().addAll((Collection<? extends TitledRequirementsSection>)newValue);
				return;
			case Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES:
				setAllowedtypes((AllowedTypes)newValue);
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
			case Requirements_textPackage.REQUIREMENTS_MODULE__RULES:
				getRules().clear();
				return;
			case Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES:
				setAllowedtypes((AllowedTypes)null);
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
			case Requirements_textPackage.REQUIREMENTS_MODULE__RULES:
				return rules != null && !rules.isEmpty();
			case Requirements_textPackage.REQUIREMENTS_MODULE__ALLOWEDTYPES:
				return allowedtypes != null;
		}
		return super.eIsSet(featureID);
	}

} //RequirementsModuleImpl
