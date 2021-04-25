/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>FACET ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_type <em>Facet type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getObservation_value <em>Observation value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_ENUMERATION()
 * @model extendedMetaData="name='FACET_ENUMERATION' kind='empty'"
 * @generated
 */
public interface FACET_ENUMERATION extends EObject
{
	/**
	 * Returns the value of the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Facet id</em>' reference.
	 * @see #setFacet_id(FACET_COLLECTION)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_ENUMERATION_Facet_id()
	 * @model extendedMetaData="kind='attribute' name='facet_id'"
	 * @generated
	 */
	FACET_COLLECTION getFacet_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_id <em>Facet id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Facet id</em>' reference.
	 * @see #getFacet_id()
	 * @generated
	 */
	void setFacet_id(FACET_COLLECTION value);

	/**
	 * Returns the value of the '<em><b>Facet type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Facet type</em>' reference.
	 * @see #setFacet_type(facet_type)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_ENUMERATION_Facet_type()
	 * @model extendedMetaData="kind='attribute' name='facet_type'"
	 * @generated
	 */
	facet_type getFacet_type();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getFacet_type <em>Facet type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Facet type</em>' reference.
	 * @see #getFacet_type()
	 * @generated
	 */
	void setFacet_type(facet_type value);

	/**
	 * Returns the value of the '<em><b>Observation value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Observation value</em>' attribute.
	 * @see #setObservation_value(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_ENUMERATION_Observation_value()
	 * @model extendedMetaData="kind='attribute' name='observation_value'"
	 * @generated
	 */
	String getObservation_value();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_ENUMERATION#getObservation_value <em>Observation value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Observation value</em>' attribute.
	 * @see #getObservation_value()
	 * @generated
	 */
	void setObservation_value(String value);

} // FACET_ENUMERATION
