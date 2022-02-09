/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Generated Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity#getEAttributes <em>EAttributes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getGeneratedEntity()
 * @model
 * @generated
 */
public interface GeneratedEntity extends Entity {
	/**
	 * Returns the value of the '<em><b>EAttributes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EAttributes</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getGeneratedEntity_EAttributes()
	 * @model transient="true" changeable="false" volatile="true" derived="true"
	 * @generated
	 */
	EList<Attribute> getEAttributes();

} // GeneratedEntity
