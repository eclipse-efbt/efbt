/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity#getEAttributes <em>EAttributes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity#getEIDAttribute <em>EID Attribute</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getDerivedEntity()
 * @model
 * @generated
 */
public interface DerivedEntity extends Entity {
	/**
	 * Returns the value of the '<em><b>EAttributes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EAttributes</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getDerivedEntity_EAttributes()
	 * @model transient="true" changeable="false" volatile="true" derived="true"
	 * @generated
	 */
	EList<Attribute> getEAttributes();

	/**
	 * Returns the value of the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EID Attribute</em>' reference.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getDerivedEntity_EIDAttribute()
	 * @model resolveProxies="false" transient="true" changeable="false" volatile="true" derived="true"
	 * @generated
	 */
	Attribute getEIDAttribute();

} // DerivedEntity
