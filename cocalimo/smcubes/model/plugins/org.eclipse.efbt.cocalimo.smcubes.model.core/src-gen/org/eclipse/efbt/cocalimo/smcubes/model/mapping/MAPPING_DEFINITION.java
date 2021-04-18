/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>MAPPING DEFINITION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_id <em>Mapping id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_type <em>Mapping type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMemberMapping <em>Member Mapping</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getVariableMapping <em>Variable Mapping</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION()
 * @model
 * @generated
 */
public interface MAPPING_DEFINITION extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION_Code()
	 * @model
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' attribute.
	 * @see #setMaintenance_agency_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION_Maintenance_agency_id()
	 * @model
	 * @generated
	 */
	String getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMaintenance_agency_id <em>Maintenance agency id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' attribute.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(String value);

	/**
	 * Returns the value of the '<em><b>Mapping id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Mapping id</em>' attribute.
	 * @see #setMapping_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION_Mapping_id()
	 * @model id="true"
	 * @generated
	 */
	String getMapping_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_id <em>Mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Mapping id</em>' attribute.
	 * @see #getMapping_id()
	 * @generated
	 */
	void setMapping_id(String value);

	/**
	 * Returns the value of the '<em><b>Mapping type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Mapping type</em>' attribute.
	 * @see #setMapping_type(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION_Mapping_type()
	 * @model
	 * @generated
	 */
	String getMapping_type();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMapping_type <em>Mapping type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Mapping type</em>' attribute.
	 * @see #getMapping_type()
	 * @generated
	 */
	void setMapping_type(String value);

	/**
	 * Returns the value of the '<em><b>Member Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member Mapping</em>' reference.
	 * @see #setMemberMapping(MEMBER_MAPPING)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION_MemberMapping()
	 * @model
	 * @generated
	 */
	MEMBER_MAPPING getMemberMapping();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getMemberMapping <em>Member Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member Mapping</em>' reference.
	 * @see #getMemberMapping()
	 * @generated
	 */
	void setMemberMapping(MEMBER_MAPPING value);

	/**
	 * Returns the value of the '<em><b>Variable Mapping</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable Mapping</em>' reference.
	 * @see #setVariableMapping(VARIABLE_MAPPING)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage#getMAPPING_DEFINITION_VariableMapping()
	 * @model
	 * @generated
	 */
	VARIABLE_MAPPING getVariableMapping();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION#getVariableMapping <em>Variable Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable Mapping</em>' reference.
	 * @see #getVariableMapping()
	 * @generated
	 */
	void setVariableMapping(VARIABLE_MAPPING value);

} // MAPPING_DEFINITION
