/**
 */
package org.eclipse.efbt.openregspecs.model.mapping;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VARIABLE SET MAPPING</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_SET_MAPPING#getSourceMapping <em>Source Mapping</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_SET_MAPPING#getTargetMapping <em>Target Mapping</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.mapping.MappingPackage#getVARIABLE_SET_MAPPING()
 * @model
 * @generated
 */
public interface VARIABLE_SET_MAPPING extends EObject {
	/**
	 * Returns the value of the '<em><b>Source Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source Mapping</em>' reference.
	 * @see #setSourceMapping(MAPPING_DEFINITION)
	 * @see org.eclipse.efbt.openregspecs.model.mapping.MappingPackage#getVARIABLE_SET_MAPPING_SourceMapping()
	 * @model
	 * @generated
	 */
	MAPPING_DEFINITION getSourceMapping();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_SET_MAPPING#getSourceMapping <em>Source Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Source Mapping</em>' reference.
	 * @see #getSourceMapping()
	 * @generated
	 */
	void setSourceMapping(MAPPING_DEFINITION value);

	/**
	 * Returns the value of the '<em><b>Target Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target Mapping</em>' reference.
	 * @see #setTargetMapping(MAPPING_DEFINITION)
	 * @see org.eclipse.efbt.openregspecs.model.mapping.MappingPackage#getVARIABLE_SET_MAPPING_TargetMapping()
	 * @model
	 * @generated
	 */
	MAPPING_DEFINITION getTargetMapping();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_SET_MAPPING#getTargetMapping <em>Target Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target Mapping</em>' reference.
	 * @see #getTargetMapping()
	 * @generated
	 */
	void setTargetMapping(MAPPING_DEFINITION value);

} // VARIABLE_SET_MAPPING
