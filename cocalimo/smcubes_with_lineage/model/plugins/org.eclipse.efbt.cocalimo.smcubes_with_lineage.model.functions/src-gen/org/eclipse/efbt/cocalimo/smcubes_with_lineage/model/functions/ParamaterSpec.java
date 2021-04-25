/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Paramater Spec</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Specification of  the parameters required by a function
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ParamaterSpec#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getParamaterSpec()
 * @model extendedMetaData="name='ParamaterSpec' kind='empty'"
 * @generated
 */
public interface ParamaterSpec extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A name for the parameter specification
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage#getParamaterSpec_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ParamaterSpec#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // ParamaterSpec
