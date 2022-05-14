/**
 */
package org.eclipse.efbt.openregspecs.model.platform_call;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute;

import org.eclipse.efbt.openregspecs.model.testing.E2ETestScope;
import org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Create Logical Transformation View For Scope</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.platform_call.CreateLogicalTransformationViewForScope#getLogicalTransformationModule <em>Logical Transformation Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.platform_call.CreateLogicalTransformationViewForScope#getAttributeLineage <em>Attribute Lineage</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.platform_call.CreateLogicalTransformationViewForScope#getTestScope <em>Test Scope</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope()
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
	 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope_LogicalTransformationModule()
	 * @model
	 * @generated
	 */
	LogicalTransformationModule getLogicalTransformationModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.platform_call.CreateLogicalTransformationViewForScope#getLogicalTransformationModule <em>Logical Transformation Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Logical Transformation Module</em>' reference.
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	void setLogicalTransformationModule(LogicalTransformationModule value);

	/**
	 * Returns the value of the '<em><b>Attribute Lineage</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attribute Lineage</em>' reference list.
	 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope_AttributeLineage()
	 * @model
	 * @generated
	 */
	EList<Attribute> getAttributeLineage();

	/**
	 * Returns the value of the '<em><b>Test Scope</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Test Scope</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getCreateLogicalTransformationViewForScope_TestScope()
	 * @model containment="true"
	 * @generated
	 */
	EList<E2ETestScope> getTestScope();

} // CreateLogicalTransformationViewForScope
