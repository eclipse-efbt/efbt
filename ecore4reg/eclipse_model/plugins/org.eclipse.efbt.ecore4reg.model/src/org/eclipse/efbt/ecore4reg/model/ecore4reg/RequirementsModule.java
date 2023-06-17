/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Requirements Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule#getRules <em>Rules</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule#getAllowedtypes <em>Allowedtypes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRequirementsModule()
 * @model
 * @generated
 */
public interface RequirementsModule extends org.eclipse.efbt.ecore4reg.model.ecore4reg.Module {
	/**
	 * Returns the value of the '<em><b>Rules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Rules</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRequirementsModule_Rules()
	 * @model containment="true"
	 * @generated
	 */
	EList<TitledRequirementsSection> getRules();

	/**
	 * Returns the value of the '<em><b>Allowedtypes</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Allowedtypes</em>' containment reference.
	 * @see #setAllowedtypes(AllowedTypes)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getRequirementsModule_Allowedtypes()
	 * @model containment="true"
	 * @generated
	 */
	AllowedTypes getAllowedtypes();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule#getAllowedtypes <em>Allowedtypes</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Allowedtypes</em>' containment reference.
	 * @see #getAllowedtypes()
	 * @generated
	 */
	void setAllowedtypes(AllowedTypes value);

} // RequirementsModule
