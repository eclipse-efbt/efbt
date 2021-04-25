/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>BDD Test Template</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A TestTemplates describes what parameters exist in the given when and then clauses of a TestDefinition. 
 * It also gives and example of one valid setting for each Parameter, this is an easy way to set the type of the parameter by example.
 * The actual parameters of the TestDefintion should be set to objects of the same type as the example parmater, and should meet the contraints defined int he TestConstraints associated with the TestDefinition.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getGivenText <em>Given Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getWhenText <em>When Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getThenText <em>Then Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getGivenParams <em>Given Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getWhenParams <em>When Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getThenParams <em>Then Params</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate()
 * @model
 * @generated
 */
public interface BDDTestTemplate extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name, and id, of the TestTemplate
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Given Text</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The text of the given clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Given Text</em>' containment reference.
	 * @see #setGivenText(ClauseText)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate_GivenText()
	 * @model containment="true"
	 * @generated
	 */
	ClauseText getGivenText();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getGivenText <em>Given Text</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Given Text</em>' containment reference.
	 * @see #getGivenText()
	 * @generated
	 */
	void setGivenText(ClauseText value);

	/**
	 * Returns the value of the '<em><b>When Text</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The text of the when  clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>When Text</em>' containment reference.
	 * @see #setWhenText(ClauseText)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate_WhenText()
	 * @model containment="true"
	 * @generated
	 */
	ClauseText getWhenText();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getWhenText <em>When Text</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>When Text</em>' containment reference.
	 * @see #getWhenText()
	 * @generated
	 */
	void setWhenText(ClauseText value);

	/**
	 * Returns the value of the '<em><b>Then Text</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The text of the then clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Then Text</em>' containment reference.
	 * @see #setThenText(ClauseText)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate_ThenText()
	 * @model containment="true"
	 * @generated
	 */
	ClauseText getThenText();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate#getThenText <em>Then Text</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Then Text</em>' containment reference.
	 * @see #getThenText()
	 * @generated
	 */
	void setThenText(ClauseText value);

	/**
	 * Returns the value of the '<em><b>Given Params</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.test_definition.Param}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The parameters of the given clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Given Params</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate_GivenParams()
	 * @model containment="true"
	 * @generated
	 */
	EList<Param> getGivenParams();

	/**
	 * Returns the value of the '<em><b>When Params</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.test_definition.Param}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The parameters of the when clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>When Params</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate_WhenParams()
	 * @model containment="true"
	 * @generated
	 */
	EList<Param> getWhenParams();

	/**
	 * Returns the value of the '<em><b>Then Params</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.test_definition.Param}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The parameters of the then clause
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Then Params</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getBDDTestTemplate_ThenParams()
	 * @model containment="true"
	 * @generated
	 */
	EList<Param> getThenParams();

} // BDDTestTemplate
