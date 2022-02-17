/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Smcubes Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This class stores the reference to the Modules which contain the SMCubes items
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel#getDomains <em>Domains</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel#getMembers <em>Members</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel#getVariables <em>Variables</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel#getSubdomains <em>Subdomains</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSmcubesModel()
 * @model
 * @generated
 */
public interface SmcubesModel extends EObject {
	/**
	 * Returns the value of the '<em><b>Domains</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.DomainModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Domains
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Domains</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSmcubesModel_Domains()
	 * @model containment="true"
	 * @generated
	 */
	EList<DomainModule> getDomains();

	/**
	 * Returns the value of the '<em><b>Members</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.MemberModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Members
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Members</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSmcubesModel_Members()
	 * @model containment="true"
	 * @generated
	 */
	EList<MemberModule> getMembers();

	/**
	 * Returns the value of the '<em><b>Variables</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.VariableModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Variables
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Variables</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSmcubesModel_Variables()
	 * @model containment="true"
	 * @generated
	 */
	EList<VariableModule> getVariables();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the SMCubesModel
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSmcubesModel_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Subdomains</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SubDomainModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomains</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage#getSmcubesModel_Subdomains()
	 * @model containment="true"
	 * @generated
	 */
	EList<SubDomainModule> getSubdomains();

} // SmcubesModel
