/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension;

import org.eclipse.efbt.openregspecs.model.data_meta_model.TypesAndConcepts;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SM Cubes Core Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getVariableModules <em>Variable Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getDomainModules <em>Domain Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getMemberModules <em>Member Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getSubDomainModules <em>Sub Domain Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getMemberHierarchyModules <em>Member Hierarchy Modules</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSMCubesCoreModel()
 * @model
 * @generated
 */
public interface SMCubesCoreModel extends TypesAndConcepts {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSMCubesCoreModel_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Variable Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.VariableModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSMCubesCoreModel_VariableModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<VariableModule> getVariableModules();

	/**
	 * Returns the value of the '<em><b>Domain Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.DomainModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Domain Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSMCubesCoreModel_DomainModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<DomainModule> getDomainModules();

	/**
	 * Returns the value of the '<em><b>Member Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSMCubesCoreModel_MemberModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<MemberModule> getMemberModules();

	/**
	 * Returns the value of the '<em><b>Sub Domain Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SubDomainModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sub Domain Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSMCubesCoreModel_SubDomainModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<SubDomainModule> getSubDomainModules();

	/**
	 * Returns the value of the '<em><b>Member Hierarchy Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member Hierarchy Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getSMCubesCoreModel_MemberHierarchyModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<MemberHierarchyModule> getMemberHierarchyModules();

} // SMCubesCoreModel
