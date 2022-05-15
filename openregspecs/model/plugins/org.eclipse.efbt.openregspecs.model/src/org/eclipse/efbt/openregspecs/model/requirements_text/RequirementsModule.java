/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Requirements Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a Set of Requirements
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule#getRules <em>Rules</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule#getAllowedtypes <em>Allowedtypes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getRequirementsModule()
 * @model extendedMetaData="name='RequirementsModule' kind='elementOnly'"
 * @generated
 */
public interface RequirementsModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Rules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.requirements_text.TitledRequirementsSection}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A set of top level titled requirement sections
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Rules</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getRequirementsModule_Rules()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='rules'"
	 * @generated
	 */
	EList<TitledRequirementsSection> getRules();

	/**
	 * Returns the value of the '<em><b>Allowedtypes</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * a set of Allowed types, like article, chapter, rulebook
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Allowedtypes</em>' containment reference.
	 * @see #setAllowedtypes(AllowedTypes)
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getRequirementsModule_Allowedtypes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='allowedtypes'"
	 * @generated
	 */
	AllowedTypes getAllowedtypes();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule#getAllowedtypes <em>Allowedtypes</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Allowedtypes</em>' containment reference.
	 * @see #getAllowedtypes()
	 * @generated
	 */
	void setAllowedtypes(AllowedTypes value);

} // RequirementsModule
