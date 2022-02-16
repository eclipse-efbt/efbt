/**
 */
package org.eclipse.efbt.cocalimo.platform_call.model.platform_call;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.E2ETestScope;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EStructuralFeature;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Create Logical Transformation View For Scope</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.CreateLogicalTransformationViewForScope#getLogicalTransformationModule <em>Logical Transformation Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.CreateLogicalTransformationViewForScope#getAttributeLineage <em>Attribute Lineage</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.CreateLogicalTransformationViewForScope#getTestScope <em>Test Scope</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope()
 * @model
 * @generated
 */
public interface CreateLogicalTransformationViewForScope extends PlatformCall {
	/**
	 * Returns the value of the '<em><b>Logical Transformation Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Logical Transformation Module</em>' reference.
	 * @see #setLogicalTransformationModule(LogicalTransformationModule)
	 * @see org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope_LogicalTransformationModule()
	 * @model
	 * @generated
	 */
	LogicalTransformationModule getLogicalTransformationModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.CreateLogicalTransformationViewForScope#getLogicalTransformationModule <em>Logical Transformation Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Logical Transformation Module</em>' reference.
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	void setLogicalTransformationModule(LogicalTransformationModule value);

	/**
	 * Returns the value of the '<em><b>Attribute Lineage</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EStructuralFeature}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attribute Lineage</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope_AttributeLineage()
	 * @model
	 * @generated
	 */
	EList<EStructuralFeature> getAttributeLineage();

	/**
	 * Returns the value of the '<em><b>Test Scope</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.E2ETestScope}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Test Scope</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope_TestScope()
	 * @model containment="true"
	 * @generated
	 */
	EList<E2ETestScope> getTestScope();

} // CreateLogicalTransformationViewForScope
