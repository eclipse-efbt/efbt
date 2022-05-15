/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Tag Group</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A group of Tags
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.requirements_text.TagGroup#getTags <em>Tags</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getTagGroup()
 * @model
 * @generated
 */
public interface TagGroup extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.requirements_text.Tag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the tags
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage#getTagGroup_Tags()
	 * @model containment="true"
	 * @generated
	 */
	EList<Tag> getTags();

} // TagGroup
