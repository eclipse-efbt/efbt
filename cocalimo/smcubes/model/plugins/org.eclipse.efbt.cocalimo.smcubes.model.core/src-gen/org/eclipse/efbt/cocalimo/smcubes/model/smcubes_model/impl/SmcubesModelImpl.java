/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MappingDefinitionModule;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl#getDomains <em>Domains</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl#getMembers <em>Members</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl#getVariables <em>Variables</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl#getCubes <em>Cubes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl#getCombinations <em>Combinations</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl#getMappings <em>Mappings</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl.SmcubesModelImpl#getName <em>Name</em>}</li>
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
		return Smcubes_modelPackage.Literals.SMCUBES_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<DomainModule> getDomains() {
		if (domains == null) {
			domains = new EObjectContainmentEList<DomainModule>(DomainModule.class, this, Smcubes_modelPackage.SMCUBES_MODEL__DOMAINS);
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
			members = new EObjectContainmentEList<MemberModule>(MemberModule.class, this, Smcubes_modelPackage.SMCUBES_MODEL__MEMBERS);
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
			variables = new EObjectContainmentEList<VariableModule>(VariableModule.class, this, Smcubes_modelPackage.SMCUBES_MODEL__VARIABLES);
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
			cubes = new EObjectContainmentEList<CubeModule>(CubeModule.class, this, Smcubes_modelPackage.SMCUBES_MODEL__CUBES);
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
			combinations = new EObjectContainmentEList<CombinationModule>(CombinationModule.class, this, Smcubes_modelPackage.SMCUBES_MODEL__COMBINATIONS);
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
			mappings = new EObjectContainmentEList<MappingDefinitionModule>(MappingDefinitionModule.class, this, Smcubes_modelPackage.SMCUBES_MODEL__MAPPINGS);
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
			eNotify(new ENotificationImpl(this, Notification.SET, Smcubes_modelPackage.SMCUBES_MODEL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Smcubes_modelPackage.SMCUBES_MODEL__DOMAINS:
				return ((InternalEList<?>)getDomains()).basicRemove(otherEnd, msgs);
			case Smcubes_modelPackage.SMCUBES_MODEL__MEMBERS:
				return ((InternalEList<?>)getMembers()).basicRemove(otherEnd, msgs);
			case Smcubes_modelPackage.SMCUBES_MODEL__VARIABLES:
				return ((InternalEList<?>)getVariables()).basicRemove(otherEnd, msgs);
			case Smcubes_modelPackage.SMCUBES_MODEL__CUBES:
				return ((InternalEList<?>)getCubes()).basicRemove(otherEnd, msgs);
			case Smcubes_modelPackage.SMCUBES_MODEL__COMBINATIONS:
				return ((InternalEList<?>)getCombinations()).basicRemove(otherEnd, msgs);
			case Smcubes_modelPackage.SMCUBES_MODEL__MAPPINGS:
				return ((InternalEList<?>)getMappings()).basicRemove(otherEnd, msgs);
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
			case Smcubes_modelPackage.SMCUBES_MODEL__DOMAINS:
				return getDomains();
			case Smcubes_modelPackage.SMCUBES_MODEL__MEMBERS:
				return getMembers();
			case Smcubes_modelPackage.SMCUBES_MODEL__VARIABLES:
				return getVariables();
			case Smcubes_modelPackage.SMCUBES_MODEL__CUBES:
				return getCubes();
			case Smcubes_modelPackage.SMCUBES_MODEL__COMBINATIONS:
				return getCombinations();
			case Smcubes_modelPackage.SMCUBES_MODEL__MAPPINGS:
				return getMappings();
			case Smcubes_modelPackage.SMCUBES_MODEL__NAME:
				return getName();
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
			case Smcubes_modelPackage.SMCUBES_MODEL__DOMAINS:
				getDomains().clear();
				getDomains().addAll((Collection<? extends DomainModule>)newValue);
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__MEMBERS:
				getMembers().clear();
				getMembers().addAll((Collection<? extends MemberModule>)newValue);
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__VARIABLES:
				getVariables().clear();
				getVariables().addAll((Collection<? extends VariableModule>)newValue);
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__CUBES:
				getCubes().clear();
				getCubes().addAll((Collection<? extends CubeModule>)newValue);
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__COMBINATIONS:
				getCombinations().clear();
				getCombinations().addAll((Collection<? extends CombinationModule>)newValue);
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__MAPPINGS:
				getMappings().clear();
				getMappings().addAll((Collection<? extends MappingDefinitionModule>)newValue);
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__NAME:
				setName((String)newValue);
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
			case Smcubes_modelPackage.SMCUBES_MODEL__DOMAINS:
				getDomains().clear();
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__MEMBERS:
				getMembers().clear();
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__VARIABLES:
				getVariables().clear();
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__CUBES:
				getCubes().clear();
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__COMBINATIONS:
				getCombinations().clear();
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__MAPPINGS:
				getMappings().clear();
				return;
			case Smcubes_modelPackage.SMCUBES_MODEL__NAME:
				setName(NAME_EDEFAULT);
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
			case Smcubes_modelPackage.SMCUBES_MODEL__DOMAINS:
				return domains != null && !domains.isEmpty();
			case Smcubes_modelPackage.SMCUBES_MODEL__MEMBERS:
				return members != null && !members.isEmpty();
			case Smcubes_modelPackage.SMCUBES_MODEL__VARIABLES:
				return variables != null && !variables.isEmpty();
			case Smcubes_modelPackage.SMCUBES_MODEL__CUBES:
				return cubes != null && !cubes.isEmpty();
			case Smcubes_modelPackage.SMCUBES_MODEL__COMBINATIONS:
				return combinations != null && !combinations.isEmpty();
			case Smcubes_modelPackage.SMCUBES_MODEL__MAPPINGS:
				return mappings != null && !mappings.isEmpty();
			case Smcubes_modelPackage.SMCUBES_MODEL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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
