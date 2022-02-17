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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#isContainment <em>Containment</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#getCardinality <em>Cardinality</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#isMandatory <em>Mandatory</em>}</li>
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

	/**
	 * Returns the value of the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Containment</em>' attribute.
	 * @see #setContainment(boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getForeignKeyAttribute_Containment()
	 * @model
	 * @generated
	 */
	boolean isContainment();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#isContainment <em>Containment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Containment</em>' attribute.
	 * @see #isContainment()
	 * @generated
	 */
	void setContainment(boolean value);

	/**
	 * Returns the value of the '<em><b>Cardinality</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cardinality</em>' attribute.
	 * @see #setCardinality(int)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getForeignKeyAttribute_Cardinality()
	 * @model
	 * @generated
	 */
	int getCardinality();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#getCardinality <em>Cardinality</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cardinality</em>' attribute.
	 * @see #getCardinality()
	 * @generated
	 */
	void setCardinality(int value);

	/**
	 * Returns the value of the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Mandatory</em>' attribute.
	 * @see #setMandatory(boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage#getForeignKeyAttribute_Mandatory()
	 * @model
	 * @generated
	 */
	boolean isMandatory();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#isMandatory <em>Mandatory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Mandatory</em>' attribute.
	 * @see #isMandatory()
	 * @generated
	 */
	void setMandatory(boolean value);

} // ForeignKeyAttribute
