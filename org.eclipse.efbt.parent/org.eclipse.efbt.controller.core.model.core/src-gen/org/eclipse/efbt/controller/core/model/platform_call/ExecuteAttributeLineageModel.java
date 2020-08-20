/**
 */
package org.eclipse.efbt.controller.core.model.platform_call;

import org.eclipse.efbt.input_data.smcubes.model.base_column_structured_data.InputData;

import org.eclipse.efbt.lineage.attributelineage.model.attribute_lineage.AttributeLineageModel;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Execute Attribute Lineage Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Platform call, which takes an AttributeLineageModel and creates a set of Scala files to execute that AttributeLineageModel. When it is run a number of Scla files will be available in the output directory specified by outputdir
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getAttributeLineageModel <em>Attribute Lineage Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getInputdir <em>Inputdir</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getOutputdir <em>Outputdir</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getProgramInputs <em>Program Inputs</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getExecuteAttributeLineageModel()
 * @model extendedMetaData="name='ExecuteAttributeLineageModel' kind='elementOnly'"
 * @generated
 */
public interface ExecuteAttributeLineageModel extends PlatformCall
{
	/**
	 * Returns the value of the '<em><b>Attribute Lineage Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The executable AttributeLineageModel
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Attribute Lineage Model</em>' reference.
	 * @see #setAttributeLineageModel(AttributeLineageModel)
	 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getExecuteAttributeLineageModel_AttributeLineageModel()
	 * @model extendedMetaData="kind='attribute' name='attributeLineageModel'"
	 * @generated
	 */
	AttributeLineageModel getAttributeLineageModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getAttributeLineageModel <em>Attribute Lineage Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attribute Lineage Model</em>' reference.
	 * @see #getAttributeLineageModel()
	 * @generated
	 */
	void setAttributeLineageModel(AttributeLineageModel value);

	/**
	 * Returns the value of the '<em><b>Inputdir</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The input directory with all the models, required to gett all the details of the AttributeLineageModel
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Inputdir</em>' attribute.
	 * @see #setInputdir(String)
	 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getExecuteAttributeLineageModel_Inputdir()
	 * @model extendedMetaData="kind='attribute' name='inputdir'"
	 * @generated
	 */
	String getInputdir();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getInputdir <em>Inputdir</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Inputdir</em>' attribute.
	 * @see #getInputdir()
	 * @generated
	 */
	void setInputdir(String value);

	/**
	 * Returns the value of the '<em><b>Outputdir</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The output directory for generated code
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Outputdir</em>' attribute.
	 * @see #setOutputdir(String)
	 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getExecuteAttributeLineageModel_Outputdir()
	 * @model extendedMetaData="kind='attribute' name='outputdir'"
	 * @generated
	 */
	String getOutputdir();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getOutputdir <em>Outputdir</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Outputdir</em>' attribute.
	 * @see #getOutputdir()
	 * @generated
	 */
	void setOutputdir(String value);

	/**
	 * Returns the value of the '<em><b>Program Inputs</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The program Inputs
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Program Inputs</em>' reference.
	 * @see #setProgramInputs(InputData)
	 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#getExecuteAttributeLineageModel_ProgramInputs()
	 * @model extendedMetaData="kind='attribute' name='programInputs'"
	 * @generated
	 */
	InputData getProgramInputs();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getProgramInputs <em>Program Inputs</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Program Inputs</em>' reference.
	 * @see #getProgramInputs()
	 * @generated
	 */
	void setProgramInputs(InputData value);

} // ExecuteAttributeLineageModel
