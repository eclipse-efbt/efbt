/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Allowed Types</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Allowed Types  like article, chapter, rulebook
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.AllowedTypes#getAllowedTypes <em>Allowed Types</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getAllowedTypes()
 * @model extendedMetaData="name='AllowedTypes' kind='elementOnly'"
 * @generated
 */
public interface AllowedTypes extends EObject {
	/**
	 * Returns the value of the '<em><b>Allowed Types</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementType}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A set of Allowed types, like article, chapter, rulebook
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Allowed Types</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getAllowedTypes_AllowedTypes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='allowedTypes'"
	 * @generated
	 */
	EList<RequirementType> getAllowedTypes();

} // AllowedTypes
