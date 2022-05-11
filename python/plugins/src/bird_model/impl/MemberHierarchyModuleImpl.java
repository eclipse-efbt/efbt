/**
 */
package bird_model.impl;

import bird_model.Bird_modelPackage;
import bird_model.MEMBER_HIERARCHY;
import bird_model.MEMBER_HIERARCHY_NODE;
import bird_model.MemberHierarchyModule;

import java.util.Collection;

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
 *   <li>{@link bird_model.impl.MemberHierarchyModuleImpl#getMemberHierarchies <em>Member Hierarchies</em>}</li>
 *   <li>{@link bird_model.impl.MemberHierarchyModuleImpl#getMemberHierarchiesNodes <em>Member Hierarchies Nodes</em>}</li>
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
		return Bird_modelPackage.Literals.MEMBER_HIERARCHY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<MEMBER_HIERARCHY> getMemberHierarchies() {
		if (memberHierarchies == null) {
			memberHierarchies = new EObjectContainmentEList<MEMBER_HIERARCHY>(MEMBER_HIERARCHY.class, this, Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES);
		}
		return memberHierarchies;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<MEMBER_HIERARCHY_NODE> getMemberHierarchiesNodes() {
		if (memberHierarchiesNodes == null) {
			memberHierarchiesNodes = new EObjectContainmentEList<MEMBER_HIERARCHY_NODE>(MEMBER_HIERARCHY_NODE.class, this, Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES);
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
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				return ((InternalEList<?>)getMemberHierarchies()).basicRemove(otherEnd, msgs);
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
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
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				return getMemberHierarchies();
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
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
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				getMemberHierarchies().clear();
				getMemberHierarchies().addAll((Collection<? extends MEMBER_HIERARCHY>)newValue);
				return;
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
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
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				getMemberHierarchies().clear();
				return;
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
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
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES:
				return memberHierarchies != null && !memberHierarchies.isEmpty();
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES:
				return memberHierarchiesNodes != null && !memberHierarchiesNodes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //MemberHierarchyModuleImpl
