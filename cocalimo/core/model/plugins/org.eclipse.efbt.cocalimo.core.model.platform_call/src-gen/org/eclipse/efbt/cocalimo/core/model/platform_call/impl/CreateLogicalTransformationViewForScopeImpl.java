/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;

import org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Create Logical Transformation View For Scope</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl#getScriptTasksInScope <em>Script Tasks In Scope</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl#getScenariosOutOfScope <em>Scenarios Out Of Scope</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl#getLogicalTransformationModule <em>Logical Transformation Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl#getAttributeLineage <em>Attribute Lineage</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CreateLogicalTransformationViewForScopeImpl extends PlatformCallImpl implements CreateLogicalTransformationViewForScope {
	/**
	 * The cached value of the '{@link #getScriptTasksInScope() <em>Script Tasks In Scope</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScriptTasksInScope()
	 * @generated
	 * @ordered
	 */
	protected EList<ScriptTask> scriptTasksInScope;

	/**
	 * The cached value of the '{@link #getScenariosOutOfScope() <em>Scenarios Out Of Scope</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenariosOutOfScope()
	 * @generated
	 * @ordered
	 */
	protected EList<Scenario> scenariosOutOfScope;

	/**
	 * The cached value of the '{@link #getLogicalTransformationModule() <em>Logical Transformation Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLogicalTransformationModule()
	 * @generated
	 * @ordered
	 */
	protected LogicalTransformationModule logicalTransformationModule;

	/**
	 * The cached value of the '{@link #getAttributeLineage() <em>Attribute Lineage</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttributeLineage()
	 * @generated
	 * @ordered
	 */
	protected EList<EStructuralFeature> attributeLineage;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CreateLogicalTransformationViewForScopeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ScriptTask> getScriptTasksInScope() {
		if (scriptTasksInScope == null) {
			scriptTasksInScope = new EObjectResolvingEList<ScriptTask>(ScriptTask.class, this, Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCRIPT_TASKS_IN_SCOPE);
		}
		return scriptTasksInScope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Scenario> getScenariosOutOfScope() {
		if (scenariosOutOfScope == null) {
			scenariosOutOfScope = new EObjectResolvingEList<Scenario>(Scenario.class, this, Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCENARIOS_OUT_OF_SCOPE);
		}
		return scenariosOutOfScope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LogicalTransformationModule getLogicalTransformationModule() {
		if (logicalTransformationModule != null && logicalTransformationModule.eIsProxy()) {
			InternalEObject oldLogicalTransformationModule = (InternalEObject)logicalTransformationModule;
			logicalTransformationModule = (LogicalTransformationModule)eResolveProxy(oldLogicalTransformationModule);
			if (logicalTransformationModule != oldLogicalTransformationModule) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE, oldLogicalTransformationModule, logicalTransformationModule));
			}
		}
		return logicalTransformationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LogicalTransformationModule basicGetLogicalTransformationModule() {
		return logicalTransformationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setLogicalTransformationModule(LogicalTransformationModule newLogicalTransformationModule) {
		LogicalTransformationModule oldLogicalTransformationModule = logicalTransformationModule;
		logicalTransformationModule = newLogicalTransformationModule;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE, oldLogicalTransformationModule, logicalTransformationModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<EStructuralFeature> getAttributeLineage() {
		if (attributeLineage == null) {
			attributeLineage = new EObjectResolvingEList<EStructuralFeature>(EStructuralFeature.class, this, Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE);
		}
		return attributeLineage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCRIPT_TASKS_IN_SCOPE:
				return getScriptTasksInScope();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCENARIOS_OUT_OF_SCOPE:
				return getScenariosOutOfScope();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				if (resolve) return getLogicalTransformationModule();
				return basicGetLogicalTransformationModule();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE:
				return getAttributeLineage();
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCRIPT_TASKS_IN_SCOPE:
				getScriptTasksInScope().clear();
				getScriptTasksInScope().addAll((Collection<? extends ScriptTask>)newValue);
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCENARIOS_OUT_OF_SCOPE:
				getScenariosOutOfScope().clear();
				getScenariosOutOfScope().addAll((Collection<? extends Scenario>)newValue);
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				setLogicalTransformationModule((LogicalTransformationModule)newValue);
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE:
				getAttributeLineage().clear();
				getAttributeLineage().addAll((Collection<? extends EStructuralFeature>)newValue);
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCRIPT_TASKS_IN_SCOPE:
				getScriptTasksInScope().clear();
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCENARIOS_OUT_OF_SCOPE:
				getScenariosOutOfScope().clear();
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				setLogicalTransformationModule((LogicalTransformationModule)null);
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE:
				getAttributeLineage().clear();
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCRIPT_TASKS_IN_SCOPE:
				return scriptTasksInScope != null && !scriptTasksInScope.isEmpty();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCENARIOS_OUT_OF_SCOPE:
				return scenariosOutOfScope != null && !scenariosOutOfScope.isEmpty();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				return logicalTransformationModule != null;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE:
				return attributeLineage != null && !attributeLineage.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //CreateLogicalTransformationViewForScopeImpl
