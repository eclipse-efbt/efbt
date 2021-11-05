/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core;

import java.util.Date;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>MEMBER HIERARCHY NODE</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getComparator <em>Comparator</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getLevel <em>Level</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_hierarchy_id <em>Member hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getOperator <em>Operator</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getParent_member_id <em>Parent member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE()
 * @model extendedMetaData="name='MEMBER_HIERARCHY_NODE' kind='empty'"
 * @generated
 */
public interface MEMBER_HIERARCHY_NODE extends EObject {
	/**
	 * Returns the value of the '<em><b>Comparator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comparator</em>' attribute.
	 * @see #setComparator(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Comparator()
	 * @model extendedMetaData="kind='attribute' name='comparator'"
	 * @generated
	 */
	String getComparator();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getComparator <em>Comparator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Comparator</em>' attribute.
	 * @see #getComparator()
	 * @generated
	 */
	void setComparator(String value);

	/**
	 * Returns the value of the '<em><b>Level</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Level</em>' attribute.
	 * @see #setLevel(int)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Level()
	 * @model extendedMetaData="kind='attribute' name='level'"
	 * @generated
	 */
	int getLevel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getLevel <em>Level</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Level</em>' attribute.
	 * @see #getLevel()
	 * @generated
	 */
	void setLevel(int value);

	/**
	 * Returns the value of the '<em><b>Member hierarchy id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member hierarchy id</em>' reference.
	 * @see #setMember_hierarchy_id(MEMBER_HIERARCHY)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Member_hierarchy_id()
	 * @model extendedMetaData="kind='attribute' name='member_hierarchy_id'"
	 * @generated
	 */
	MEMBER_HIERARCHY getMember_hierarchy_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_hierarchy_id <em>Member hierarchy id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member hierarchy id</em>' reference.
	 * @see #getMember_hierarchy_id()
	 * @generated
	 */
	void setMember_hierarchy_id(MEMBER_HIERARCHY value);

	/**
	 * Returns the value of the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member id</em>' reference.
	 * @see #setMember_id(MEMBER)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Member_id()
	 * @model extendedMetaData="kind='attribute' name='member_id'"
	 * @generated
	 */
	MEMBER getMember_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getMember_id <em>Member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member id</em>' reference.
	 * @see #getMember_id()
	 * @generated
	 */
	void setMember_id(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Operator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operator</em>' attribute.
	 * @see #setOperator(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Operator()
	 * @model extendedMetaData="kind='attribute' name='operator'"
	 * @generated
	 */
	String getOperator();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getOperator <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operator</em>' attribute.
	 * @see #getOperator()
	 * @generated
	 */
	void setOperator(String value);

	/**
	 * Returns the value of the '<em><b>Parent member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parent member id</em>' reference.
	 * @see #setParent_member_id(MEMBER)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Parent_member_id()
	 * @model extendedMetaData="kind='attribute' name='parent_member_id'"
	 * @generated
	 */
	MEMBER getParent_member_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getParent_member_id <em>Parent member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parent member id</em>' reference.
	 * @see #getParent_member_id()
	 * @generated
	 */
	void setParent_member_id(MEMBER value);

	/**
	 * Returns the value of the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid from</em>' attribute.
	 * @see #setValid_from(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Valid_from()
	 * @model extendedMetaData="kind='attribute' name='valid_from'"
	 * @generated
	 */
	Date getValid_from();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_from <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid from</em>' attribute.
	 * @see #getValid_from()
	 * @generated
	 */
	void setValid_from(Date value);

	/**
	 * Returns the value of the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Valid to</em>' attribute.
	 * @see #setValid_to(Date)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getMEMBER_HIERARCHY_NODE_Valid_to()
	 * @model extendedMetaData="kind='attribute' name='valid_to'"
	 * @generated
	 */
	Date getValid_to();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE#getValid_to <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Valid to</em>' attribute.
	 * @see #getValid_to()
	 * @generated
	 */
	void setValid_to(Date value);

} // MEMBER_HIERARCHY_NODE
