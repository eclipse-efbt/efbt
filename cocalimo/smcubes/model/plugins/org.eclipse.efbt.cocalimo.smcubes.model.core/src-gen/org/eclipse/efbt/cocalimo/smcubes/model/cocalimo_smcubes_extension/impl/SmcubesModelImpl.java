/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.CombinationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MappingDefinitionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SubDomainEnumerationModule;
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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getCubes <em>Cubes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getCombinations <em>Combinations</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getMappings <em>Mappings</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getSubDomainsModule <em>Sub Domains Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SmcubesModelImpl#getSubdomainEnumerationModules <em>Subdomain Enumeration Modules</em>}</li>
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
	 * The cached value of the '{@link #getCubes() <em>Cubes</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubes()
	 * @generated
	 * @ordered
	 */
	protected EList<CubeModule> cubes;

	/**
	 * The cached value of the '{@link #getCombinations() <em>Combinations</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCombinations()
	 * @generated
	 * @ordered
	 */
	protected EList<CombinationModule> combinations;

	/**
	 * The cached value of the '{@link #getMappings() <em>Mappings</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMappings()
	 * @generated
	 * @ordered
	 */
	protected EList<MappingDefinitionModule> mappings;

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
	 * The cached value of the '{@link #getSubDomainsModule() <em>Sub Domains Module</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubDomainsModule()
	 * @generated
	 * @ordered
	 */
	protected EList<SubDomainModule> subDomainsModule;

	/**
	 * The cached value of the '{@link #getSubdomainEnumerationModules() <em>Subdomain Enumeration Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomainEnumerationModules()
	 * @generated
	 * @ordered
	 */
	protected EList<SubDomainEnumerationModule> subdomainEnumerationModules;

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
	public EList<CubeModule> getCubes() {
		if (cubes == null) {
			cubes = new EObjectContainmentEList<CubeModule>(CubeModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__CUBES);
		}
		return cubes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<CombinationModule> getCombinations() {
		if (combinations == null) {
			combinations = new EObjectContainmentEList<CombinationModule>(CombinationModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__COMBINATIONS);
		}
		return combinations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MappingDefinitionModule> getMappings() {
		if (mappings == null) {
			mappings = new EObjectContainmentEList<MappingDefinitionModule>(MappingDefinitionModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MAPPINGS);
		}
		return mappings;
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
	public EList<SubDomainModule> getSubDomainsModule() {
		if (subDomainsModule == null) {
			subDomainsModule = new EObjectContainmentEList<SubDomainModule>(SubDomainModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUB_DOMAINS_MODULE);
		}
		return subDomainsModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SubDomainEnumerationModule> getSubdomainEnumerationModules() {
		if (subdomainEnumerationModules == null) {
			subdomainEnumerationModules = new EObjectContainmentEList<SubDomainEnumerationModule>(SubDomainEnumerationModule.class, this, Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAIN_ENUMERATION_MODULES);
		}
		return subdomainEnumerationModules;
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__CUBES:
				return ((InternalEList<?>)getCubes()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__COMBINATIONS:
				return ((InternalEList<?>)getCombinations()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MAPPINGS:
				return ((InternalEList<?>)getMappings()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUB_DOMAINS_MODULE:
				return ((InternalEList<?>)getSubDomainsModule()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAIN_ENUMERATION_MODULES:
				return ((InternalEList<?>)getSubdomainEnumerationModules()).basicRemove(otherEnd, msgs);
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__CUBES:
				return getCubes();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__COMBINATIONS:
				return getCombinations();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MAPPINGS:
				return getMappings();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				return getName();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUB_DOMAINS_MODULE:
				return getSubDomainsModule();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAIN_ENUMERATION_MODULES:
				return getSubdomainEnumerationModules();
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__CUBES:
				getCubes().clear();
				getCubes().addAll((Collection<? extends CubeModule>)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__COMBINATIONS:
				getCombinations().clear();
				getCombinations().addAll((Collection<? extends CombinationModule>)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MAPPINGS:
				getMappings().clear();
				getMappings().addAll((Collection<? extends MappingDefinitionModule>)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				setName((String)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUB_DOMAINS_MODULE:
				getSubDomainsModule().clear();
				getSubDomainsModule().addAll((Collection<? extends SubDomainModule>)newValue);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAIN_ENUMERATION_MODULES:
				getSubdomainEnumerationModules().clear();
				getSubdomainEnumerationModules().addAll((Collection<? extends SubDomainEnumerationModule>)newValue);
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__CUBES:
				getCubes().clear();
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__COMBINATIONS:
				getCombinations().clear();
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MAPPINGS:
				getMappings().clear();
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUB_DOMAINS_MODULE:
				getSubDomainsModule().clear();
				return;
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAIN_ENUMERATION_MODULES:
				getSubdomainEnumerationModules().clear();
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
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__CUBES:
				return cubes != null && !cubes.isEmpty();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__COMBINATIONS:
				return combinations != null && !combinations.isEmpty();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__MAPPINGS:
				return mappings != null && !mappings.isEmpty();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUB_DOMAINS_MODULE:
				return subDomainsModule != null && !subDomainsModule.isEmpty();
			case Cocalimo_smcubes_extensionPackage.SMCUBES_MODEL__SUBDOMAIN_ENUMERATION_MODULES:
				return subdomainEnumerationModules != null && !subdomainEnumerationModules.isEmpty();
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
