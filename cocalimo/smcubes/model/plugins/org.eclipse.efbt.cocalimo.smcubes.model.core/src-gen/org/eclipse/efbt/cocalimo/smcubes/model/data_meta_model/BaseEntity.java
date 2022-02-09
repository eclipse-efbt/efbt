/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getSuperType <em>Super Type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getAttributes <em>Attributes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getBaseEntity()
 * @model
 * @generated
 */
public interface BaseEntity extends Entity {
	/**
	 * Returns the value of the '<em><b>Super Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Super Type</em>' reference.
	 * @see #isSetSuperType()
	 * @see #unsetSuperType()
	 * @see #setSuperType(BaseEntity)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getBaseEntity_SuperType()
	 * @model unsettable="true"
	 * @generated
	 */
	BaseEntity getSuperType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getSuperType <em>Super Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Super Type</em>' reference.
	 * @see #isSetSuperType()
	 * @see #unsetSuperType()
	 * @see #getSuperType()
	 * @generated
	 */
	void setSuperType(BaseEntity value);

	/**
	 * Unsets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getSuperType <em>Super Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isSetSuperType()
	 * @see #getSuperType()
	 * @see #setSuperType(BaseEntity)
	 * @generated
	 */
	void unsetSuperType();

	/**
	 * Returns whether the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getSuperType <em>Super Type</em>}' reference is set.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return whether the value of the '<em>Super Type</em>' reference is set.
	 * @see #unsetSuperType()
	 * @see #getSuperType()
	 * @see #setSuperType(BaseEntity)
	 * @generated
	 */
	boolean isSetSuperType();

	/**
	 * Returns the value of the '<em><b>Attributes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attributes</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getBaseEntity_Attributes()
	 * @model containment="true"
	 * @generated
	 */
	EList<Attribute> getAttributes();

} // BaseEntity
