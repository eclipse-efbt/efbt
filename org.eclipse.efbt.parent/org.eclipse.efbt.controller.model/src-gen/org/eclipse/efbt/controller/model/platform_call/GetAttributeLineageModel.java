/**
 */
package org.eclipse.efbt.controller.model.platform_call;

import org.eclipse.efbt.almengine.model.attribute_lineage.attribute_lineage.AttributeLineageModel;

import org.eclipse.efbt.almengine.model.functions.functions.SpeculativeCubeColumnParameter;

import org.eclipse.efbt.trl.model.trl.transformation.VersionedComponentsSet;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Get Attribute Lineage Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Platform call, which takes a VersionedComponentsSet as input and returns a program
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel#getResultingModel <em>Resulting Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel#getTransformationContext <em>Transformation Context</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel#getUnresolvedCubeColumnParams <em>Unresolved Cube Column Params</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageModel()
 * @model extendedMetaData="name='GetAttributeLineageModel' kind='elementOnly'"
 * @generated
 */
public interface GetAttributeLineageModel extends PlatformCall
{
	/**
	 * Returns the value of the '<em><b>Resulting Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The resulting program
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Resulting Model</em>' containment reference.
	 * @see #setResultingModel(AttributeLineageModel)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageModel_ResultingModel()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='resultingModel'"
	 * @generated
	 */
	AttributeLineageModel getResultingModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel#getResultingModel <em>Resulting Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Resulting Model</em>' containment reference.
	 * @see #getResultingModel()
	 * @generated
	 */
	void setResultingModel(AttributeLineageModel value);

	/**
	 * Returns the value of the '<em><b>Transformation Context</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The VersionedComponentsSet
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Transformation Context</em>' reference.
	 * @see #setTransformationContext(VersionedComponentsSet)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageModel_TransformationContext()
	 * @model extendedMetaData="kind='attribute' name='transformationContext'"
	 * @generated
	 */
	VersionedComponentsSet getTransformationContext();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel#getTransformationContext <em>Transformation Context</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Transformation Context</em>' reference.
	 * @see #getTransformationContext()
	 * @generated
	 */
	void setTransformationContext(VersionedComponentsSet value);

	/**
	 * Returns the value of the '<em><b>Unresolved Cube Column Params</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.almengine.model.functions.functions.SpeculativeCubeColumnParameter}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * If the VersionedComponentsSet includes DataSetTranformation which contains SpeculativeCubeColumnParameter which cannot be resolved then we list them here
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Unresolved Cube Column Params</em>' reference list.
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageModel_UnresolvedCubeColumnParams()
	 * @model extendedMetaData="kind='attribute' name='unresolvedCubeColumnParams'"
	 * @generated
	 */
	EList<SpeculativeCubeColumnParameter> getUnresolvedCubeColumnParams();

} // GetAttributeLineageModel
