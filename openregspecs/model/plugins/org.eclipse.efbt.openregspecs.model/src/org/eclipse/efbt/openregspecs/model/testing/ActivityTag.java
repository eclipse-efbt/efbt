/**
 */
package org.eclipse.efbt.openregspecs.model.testing;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Activity;

import org.eclipse.efbt.openregspecs.model.requirements_text.Tag;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Activity Tag</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.ActivityTag#getActivity <em>Activity</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getActivityTag()
 * @model
 * @generated
 */
public interface ActivityTag extends Tag {
	/**
	 * Returns the value of the '<em><b>Activity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Activity</em>' reference.
	 * @see #setActivity(Activity)
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getActivityTag_Activity()
	 * @model
	 * @generated
	 */
	Activity getActivity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.testing.ActivityTag#getActivity <em>Activity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Activity</em>' reference.
	 * @see #getActivity()
	 * @generated
	 */
	void setActivity(Activity value);

} // ActivityTag
