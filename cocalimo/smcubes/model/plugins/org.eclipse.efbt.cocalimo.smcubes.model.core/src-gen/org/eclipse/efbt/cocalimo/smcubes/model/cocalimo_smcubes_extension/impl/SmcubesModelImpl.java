/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SubDomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.VariableModule;

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
 * An implementation of the model object '<em><b>Smcubes Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getDomains <em>Domains</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getMembers <em>Members</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getVariables <em>Variables</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getSubdomains <em>Subdomains</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SmcubesModelImpl extends MinimalEObjectImpl.Container implements SmcubesModel {
	/**
	 * The cached value of the '{@link #getDomains() <em>Domains</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDomains()
	 * @generated
	 * @ordered
	 */
	protected EList<DomainModule> domains;

	/**
	 * The cached value of the '{@link #getMembers() <em>Members</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMembers()
	 * @generated
	 * @ordered
	 */
	protected EList<MemberModule> members;

	/**
	 * The cached value of the '{@link #getVariables() <em>Variables</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariables()
	 * @generated
	 * @ordered
	 */
	protected EList<VariableModule> variables;

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
	 * The cached value of the '{@link #getSubdomains() <em>Subdomains</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomains()
	 * @generated
	 * @ordered
	 */
	protected EList<SubDomainModule> subdomains;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SmcubesModelImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Cocalimo_smcubes_extensionPackage.Literals.SMCUBES_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<DomainModule> getDomains() {
		if (domains == null) {
			domains = new EObjectContainmentEList<DomainModule>(DomainModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__DOMAINS);
		}
		return domains;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MemberModule> getMembers() {
		if (members == null) {
			members = new EObjectContainmentEList<MemberModule>(MemberModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MEMBERS);
		}
		return members;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<VariableModule> getVariables() {
		if (variables == null) {
			variables = new EObjectContainmentEList<VariableModule>(VariableModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__VARIABLES);
		}
		return variables;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SubDomainModule> getSubdomains() {
		if (subdomains == null) {
			subdomains = new EObjectContainmentEList<SubDomainModule>(SubDomainModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAINS);
		}
		return subdomains;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__DOMAINS:
				return ((InternalEList<?>)getDomains()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MEMBERS:
				return ((InternalEList<?>)getMembers()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__VARIABLES:
				return ((InternalEList<?>)getVariables()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAINS:
				return ((InternalEList<?>)getSubdomains()).basicRemove(otherEnd, msgs);
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__DOMAINS:
				return getDomains();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MEMBERS:
				return getMembers();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__VARIABLES:
				return getVariables();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				return getName();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAINS:
				return getSubdomains();
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__DOMAINS:
				getDomains().clear();
				getDomains().addAll((Collection<? extends DomainModule>)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MEMBERS:
				getMembers().clear();
				getMembers().addAll((Collection<? extends MemberModule>)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__VARIABLES:
				getVariables().clear();
				getVariables().addAll((Collection<? extends VariableModule>)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				setName((String)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAINS:
				getSubdomains().clear();
				getSubdomains().addAll((Collection<? extends SubDomainModule>)newValue);
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__DOMAINS:
				getDomains().clear();
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MEMBERS:
				getMembers().clear();
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__VARIABLES:
				getVariables().clear();
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAINS:
				getSubdomains().clear();
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__DOMAINS:
				return domains != null && !domains.isEmpty();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MEMBERS:
				return members != null && !members.isEmpty();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__VARIABLES:
				return variables != null && !variables.isEmpty();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAINS:
				return subdomains != null && !subdomains.isEmpty();
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

} //SmcubesModelImpl
