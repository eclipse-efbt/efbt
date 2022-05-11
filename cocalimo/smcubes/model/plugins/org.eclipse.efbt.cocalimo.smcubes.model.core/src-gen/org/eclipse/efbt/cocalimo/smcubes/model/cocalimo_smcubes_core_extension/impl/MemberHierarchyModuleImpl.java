/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.Cocalimo_smcubes_core_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.MemberHierarchyModule;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Member Hierarchy Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.impl.MemberHierarchyModuleImpl#getMemberHierarchies <em>Member Hierarchies</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_core_extension.impl.MemberHierarchyModuleImpl#getMemberHierarchiesNodes <em>Member Hierarchies Nodes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MemberHierarchyModuleImpl extends ModuleImpl implements MemberHierarchyModule {
	/**
	 * The cached value of the '{@link #getMemberHierarchies() <em>Member Hierarchies</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMemberHierarchies()
	 * @generated
	 * @ordered
	 */
	protected EList<MEMBER_HIERARCHY> memberHierarchies;

	/**
	 * The cached value of the '{@link #getMemberHierarchiesNodes() <em>Member Hierarchies Nodes</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMemberHierarchiesNodes()
	 * @generated
	 * @ordered
	 */
	protected EList<MEMBER_HIERARCHY_NODE> memberHierarchiesNodes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MemberHierarchyModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Cocalimo_smcubes_core_extensionPackage.Literals.MEMBER_HIERARCHY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MEMBER_HIERARCHY> getMemberHierarchies() {
		if (memberHierarchies == null) {
			memberHierarchies = new EObjectContainmentEList<MEMBER_HIERARCHY>(MEMBER_HIERARCHY.class, this, Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES);
		}
		return memberHierarchies;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MEMBER_HIERARCHY_NODE> getMemberHierarchiesNodes() {
		if (memberHierarchiesNodes == null) {
			memberHierarchiesNodes = new EObjectContainmentEList<MEMBER_HIERARCHY_NODE>(MEMBER_HIERARCHY_NODE.class, this, Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES);
		}
		return memberHierarchiesNodes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				return ((InternalEList<?>)getMemberHierarchies()).basicRemove(otherEnd, msgs);
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
				return ((InternalEList<?>)getMemberHierarchiesNodes()).basicRemove(otherEnd, msgs);
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
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				return getMemberHierarchies();
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
				return getMemberHierarchiesNodes();
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
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				getMemberHierarchies().clear();
				getMemberHierarchies().addAll((Collection<? extends MEMBER_HIERARCHY>)newValue);
				return;
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
				getMemberHierarchiesNodes().clear();
				getMemberHierarchiesNodes().addAll((Collection<? extends MEMBER_HIERARCHY_NODE>)newValue);
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
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				getMemberHierarchies().clear();
				return;
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
				getMemberHierarchiesNodes().clear();
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
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				return memberHierarchies != null && !memberHierarchies.isEmpty();
			case Cocalimo_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
				return memberHierarchiesNodes != null && !memberHierarchiesNodes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //MemberHierarchyModuleImpl
