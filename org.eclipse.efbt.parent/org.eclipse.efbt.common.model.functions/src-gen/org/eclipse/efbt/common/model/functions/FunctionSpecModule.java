/**
 */
package org.eclipse.efbt.common.model.functions;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Function Spec Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module made up of a list of FunctionSpecs 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.common.model.functions.FunctionSpecModule#getFunctionSpecs <em>Function Specs</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.common.model.functions.FunctionsPackage#getFunctionSpecModule()
 * @model extendedMetaData="name='FunctionSpecModule' kind='elementOnly'"
 * @generated
 */
public interface FunctionSpecModule extends org.eclipse.efbt.common.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Function Specs</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.common.model.functions.FunctionSpec}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of FunctionSpecs contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Function Specs</em>' containment reference list.
	 * @see org.eclipse.efbt.common.model.functions.FunctionsPackage#getFunctionSpecModule_FunctionSpecs()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='functionSpecs'"
	 * @generated
	 */
	EList<FunctionSpec> getFunctionSpecs();

} // FunctionSpecModule
