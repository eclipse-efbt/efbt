/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Select Column</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectColumn#getAs <em>As</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectColumn#getMemberAsConstant <em>Member As Constant</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getSelectColumn()
 * @model
 * @generated
 */
public interface SelectColumn extends Column {
	/**
	 * Returns the value of the '<em><b>As</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>As</em>' reference.
	 * @see #setAs(Attribute)
	 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getSelectColumn_As()
	 * @model
	 * @generated
	 */
	Attribute getAs();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectColumn#getAs <em>As</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>As</em>' reference.
	 * @see #getAs()
	 * @generated
	 */
	void setAs(Attribute value);

	/**
	 * Returns the value of the '<em><b>Member As Constant</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member As Constant</em>' reference.
	 * @see #setMemberAsConstant(MEMBER)
	 * @see org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage#getSelectColumn_MemberAsConstant()
	 * @model
	 * @generated
	 */
	MEMBER getMemberAsConstant();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.SelectColumn#getMemberAsConstant <em>Member As Constant</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member As Constant</em>' reference.
	 * @see #getMemberAsConstant()
	 * @generated
	 */
	void setMemberAsConstant(MEMBER value);

} // SelectColumn
