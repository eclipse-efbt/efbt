/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.impl;

import java.util.Collection;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.E2ETestScope;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.notify.NotificationChain;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Create Logical Transformation View For Scope</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl#getLogicalTransformationModule <em>Logical Transformation Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl#getAttributeLineage <em>Attribute Lineage</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl#getTestScope <em>Test Scope</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CreateLogicalTransformationViewForScopeImpl extends PlatformCallImpl implements CreateLogicalTransformationViewForScope {
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
	 * The cached value of the '{@link #getTestScope() <em>Test Scope</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestScope()
	 * @generated
	 * @ordered
	 */
	protected EList<E2ETestScope> testScope;

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
			attributeLineage = new EObjectResolvingEList<EStructuralFeature>(EStructuralFeature.class, this, Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__computation_lineage);
		}
		return attributeLineage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<E2ETestScope> getTestScope() {
		if (testScope == null) {
			testScope = new EObjectContainmentEList<E2ETestScope>(E2ETestScope.class, this, Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE);
		}
		return testScope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE:
				return ((InternalEList<?>)getTestScope()).basicRemove(otherEnd, msgs);
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				if (resolve) return getLogicalTransformationModule();
				return basicGetLogicalTransformationModule();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__computation_lineage:
				return getAttributeLineage();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE:
				return getTestScope();
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				setLogicalTransformationModule((LogicalTransformationModule)newValue);
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__computation_lineage:
				getAttributeLineage().clear();
				getAttributeLineage().addAll((Collection<? extends EStructuralFeature>)newValue);
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE:
				getTestScope().clear();
				getTestScope().addAll((Collection<? extends E2ETestScope>)newValue);
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				setLogicalTransformationModule((LogicalTransformationModule)null);
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__computation_lineage:
				getAttributeLineage().clear();
				return;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE:
				getTestScope().clear();
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE:
				return logicalTransformationModule != null;
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__computation_lineage:
				return attributeLineage != null && !attributeLineage.isEmpty();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE:
				return testScope != null && !testScope.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //CreateLogicalTransformationViewForScopeImpl
