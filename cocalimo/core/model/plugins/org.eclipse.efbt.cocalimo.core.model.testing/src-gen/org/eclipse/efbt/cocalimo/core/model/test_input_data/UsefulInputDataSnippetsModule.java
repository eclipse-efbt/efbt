/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Useful Input Data Snippets Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module containing useful snippets of test data
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippetsModule#getSnippets <em>Snippets</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage#getUsefulInputDataSnippetsModule()
 * @model
 * @generated
 */
public interface UsefulInputDataSnippetsModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Snippets</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The useful snippets
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Snippets</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage#getUsefulInputDataSnippetsModule_Snippets()
	 * @model containment="true"
	 * @generated
	 */
	EList<UsefulInputDataSnippets> getSnippets();

} // UsefulInputDataSnippetsModule
