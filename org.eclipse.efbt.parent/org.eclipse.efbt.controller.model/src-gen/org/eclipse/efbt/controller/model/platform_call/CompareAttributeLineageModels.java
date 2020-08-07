/**
 */
package org.eclipse.efbt.controller.model.platform_call;

import org.eclipse.efbt.almengine.model.attribute_lineage.attribute_lineage.AttributeLineageModel;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Compare Attribute Lineage Models</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This is a PlatformCall which compares 2 AttributeModels
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getResultingModel <em>Resulting Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getSubsetBoolean <em>Subset Boolean</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getNotIncludedModel <em>Not Included Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getFirstModel <em>First Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getSecondModel <em>Second Model</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getCompareAttributeLineageModels()
 * @model extendedMetaData="name='CompareAttributeLineageModels' kind='elementOnly'"
 * @generated
 */
public interface CompareAttributeLineageModels extends PlatformCall
{
	/**
	 * Returns the value of the '<em><b>Resulting Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * This is he resulting Attributemodel, it makes use of the isSubset field to say which items are inluded in both the first model and the second model
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Resulting Model</em>' containment reference.
	 * @see #setResultingModel(AttributeLineageModel)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getCompareAttributeLineageModels_ResultingModel()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='resultingModel'"
	 * @generated
	 */
	AttributeLineageModel getResultingModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getResultingModel <em>Resulting Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Resulting Model</em>' containment reference.
	 * @see #getResultingModel()
	 * @generated
	 */
	void setResultingModel(AttributeLineageModel value);

	/**
	 * Returns the value of the '<em><b>Subset Boolean</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * This field is used to state if the firt model is a subset of the second model or not.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Subset Boolean</em>' containment reference.
	 * @see #setSubsetBoolean(SubsetBoolean)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getCompareAttributeLineageModels_SubsetBoolean()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='subsetBoolean'"
	 * @generated
	 */
	SubsetBoolean getSubsetBoolean();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getSubsetBoolean <em>Subset Boolean</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Subset Boolean</em>' containment reference.
	 * @see #getSubsetBoolean()
	 * @generated
	 */
	void setSubsetBoolean(SubsetBoolean value);

	/**
	 * Returns the value of the '<em><b>Not Included Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Not Included Model</em>' containment reference.
	 * @see #setNotIncludedModel(AttributeLineageModel)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getCompareAttributeLineageModels_NotIncludedModel()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='notIncludedModel'"
	 * @generated
	 */
	AttributeLineageModel getNotIncludedModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getNotIncludedModel <em>Not Included Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Not Included Model</em>' containment reference.
	 * @see #getNotIncludedModel()
	 * @generated
	 */
	void setNotIncludedModel(AttributeLineageModel value);

	/**
	 * Returns the value of the '<em><b>First Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>First Model</em>' reference.
	 * @see #setFirstModel(AttributeLineageModel)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getCompareAttributeLineageModels_FirstModel()
	 * @model extendedMetaData="kind='attribute' name='firstModel'"
	 * @generated
	 */
	AttributeLineageModel getFirstModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getFirstModel <em>First Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>First Model</em>' reference.
	 * @see #getFirstModel()
	 * @generated
	 */
	void setFirstModel(AttributeLineageModel value);

	/**
	 * Returns the value of the '<em><b>Second Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Second Model</em>' reference.
	 * @see #setSecondModel(AttributeLineageModel)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getCompareAttributeLineageModels_SecondModel()
	 * @model extendedMetaData="kind='attribute' name='secondModel'"
	 * @generated
	 */
	AttributeLineageModel getSecondModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.CompareAttributeLineageModels#getSecondModel <em>Second Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Second Model</em>' reference.
	 * @see #getSecondModel()
	 * @generated
	 */
	void setSecondModel(AttributeLineageModel value);

} // CompareAttributeLineageModels
