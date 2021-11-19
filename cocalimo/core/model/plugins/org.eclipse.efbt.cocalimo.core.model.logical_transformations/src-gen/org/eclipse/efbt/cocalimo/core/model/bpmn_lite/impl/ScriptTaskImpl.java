/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Script Task</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl.ScriptTaskImpl#getLinkedAttributes <em>Linked Attributes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ScriptTaskImpl extends TaskImpl implements ScriptTask {
	/**
	 * The cached value of the '{@link #getLinkedAttributes() <em>Linked Attributes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLinkedAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<EStructuralFeature> linkedAttributes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ScriptTaskImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bpmn_litePackage.Literals.SCRIPT_TASK;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<EStructuralFeature> getLinkedAttributes() {
		if (linkedAttributes == null) {
			linkedAttributes = new EObjectResolvingEList<EStructuralFeature>(EStructuralFeature.class, this, Bpmn_litePackage.SCRIPT_TASK__LINKED_ATTRIBUTES);
		}
		return linkedAttributes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bpmn_litePackage.SCRIPT_TASK__LINKED_ATTRIBUTES:
				return getLinkedAttributes();
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
			case Bpmn_litePackage.SCRIPT_TASK__LINKED_ATTRIBUTES:
				getLinkedAttributes().clear();
				getLinkedAttributes().addAll((Collection<? extends EStructuralFeature>)newValue);
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
			case Bpmn_litePackage.SCRIPT_TASK__LINKED_ATTRIBUTES:
				getLinkedAttributes().clear();
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
			case Bpmn_litePackage.SCRIPT_TASK__LINKED_ATTRIBUTES:
				return linkedAttributes != null && !linkedAttributes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ScriptTaskImpl
