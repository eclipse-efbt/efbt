/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Foreign Key Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#getEntity <em>Entity</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getForeignKeyAttribute()
 * @model
 * @generated
 */
public interface ForeignKeyAttribute extends Attribute {
	/**
	 * Returns the value of the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity</em>' reference.
	 * @see #setEntity(BasicEntity)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getForeignKeyAttribute_Entity()
	 * @model
	 * @generated
	 */
	BasicEntity getEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#getEntity <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity</em>' reference.
	 * @see #getEntity()
	 * @generated
	 */
	void setEntity(BasicEntity value);

} // ForeignKeyAttribute
