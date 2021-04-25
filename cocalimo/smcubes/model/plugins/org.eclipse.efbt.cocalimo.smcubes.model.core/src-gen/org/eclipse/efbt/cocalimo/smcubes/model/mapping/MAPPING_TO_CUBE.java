/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>MAPPING TO CUBE</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE#getMapping <em>Mapping</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_TO_CUBE()
 * @model
 * @generated
 */
public interface MAPPING_TO_CUBE extends EObject
{
	/**
	 * Returns the value of the '<em><b>Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Mapping</em>' reference.
	 * @see #setMapping(MAPPING_DEFINITION)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_TO_CUBE_Mapping()
	 * @model
	 * @generated
	 */
	MAPPING_DEFINITION getMapping();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE#getMapping <em>Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Mapping</em>' reference.
	 * @see #getMapping()
	 * @generated
	 */
	void setMapping(MAPPING_DEFINITION value);

} // MAPPING_TO_CUBE
