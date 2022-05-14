/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Requirement Type</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A requirement type such as Article, Chapter, Rulebook
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementType#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getRequirementType()
 * @model extendedMetaData="name='RequirementType' kind='empty'"
 * @generated
 */
public interface RequirementType extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the name of the requirement type
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getRequirementType_Name()
	 * @model id="true" required="true"
	 *        extendedMetaData="kind='attribute' name='requirementType'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementType#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // RequirementType
