/**
 */
package testmodel;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Reference Test Container</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link testmodel.ReferenceTestContainer#getContainments <em>Containments</em>}</li>
 *   <li>{@link testmodel.ReferenceTestContainer#getReferenceToOne <em>Reference To One</em>}</li>
 *   <li>{@link testmodel.ReferenceTestContainer#getReferenceToMany <em>Reference To Many</em>}</li>
 *   <li>{@link testmodel.ReferenceTestContainer#getName <em>Name</em>}</li>
 *   <li>{@link testmodel.ReferenceTestContainer#getContainment <em>Containment</em>}</li>
 *   <li>{@link testmodel.ReferenceTestContainer#isOneAttribute <em>One Attribute</em>}</li>
 *   <li>{@link testmodel.ReferenceTestContainer#getManyAttributes <em>Many Attributes</em>}</li>
 * </ul>
 *
 * @see testmodel.TestmodelPackage#getReferenceTestContainer()
 * @model
 * @generated
 */
public interface ReferenceTestContainer extends EObject {
	/**
	 * Returns the value of the '<em><b>Containments</b></em>' containment reference list.
	 * The list contents are of type {@link testmodel.ReferenceTarget}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Containments</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Containments</em>' containment reference list.
	 * @see testmodel.TestmodelPackage#getReferenceTestContainer_Containments()
	 * @model containment="true"
	 * @generated
	 */
	EList<ReferenceTarget> getContainments();

	/**
	 * Returns the value of the '<em><b>Reference To One</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Reference To One</em>' reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Reference To One</em>' reference.
	 * @see #setReferenceToOne(ReferenceTarget)
	 * @see testmodel.TestmodelPackage#getReferenceTestContainer_ReferenceToOne()
	 * @model
	 * @generated
	 */
	ReferenceTarget getReferenceToOne();

	/**
	 * Sets the value of the '{@link testmodel.ReferenceTestContainer#getReferenceToOne <em>Reference To One</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Reference To One</em>' reference.
	 * @see #getReferenceToOne()
	 * @generated
	 */
	void setReferenceToOne(ReferenceTarget value);

	/**
	 * Returns the value of the '<em><b>Reference To Many</b></em>' reference list.
	 * The list contents are of type {@link testmodel.ReferenceTarget}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Reference To Many</em>' reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Reference To Many</em>' reference list.
	 * @see testmodel.TestmodelPackage#getReferenceTestContainer_ReferenceToMany()
	 * @model
	 * @generated
	 */
	EList<ReferenceTarget> getReferenceToMany();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Name</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see testmodel.TestmodelPackage#getReferenceTestContainer_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link testmodel.ReferenceTestContainer#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Containment</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Containment</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Containment</em>' containment reference.
	 * @see #setContainment(ReferenceTarget)
	 * @see testmodel.TestmodelPackage#getReferenceTestContainer_Containment()
	 * @model containment="true"
	 * @generated
	 */
	ReferenceTarget getContainment();

	/**
	 * Sets the value of the '{@link testmodel.ReferenceTestContainer#getContainment <em>Containment</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Containment</em>' containment reference.
	 * @see #getContainment()
	 * @generated
	 */
	void setContainment(ReferenceTarget value);

	/**
	 * Returns the value of the '<em><b>One Attribute</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Attribute</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Attribute</em>' attribute.
	 * @see #setOneAttribute(boolean)
	 * @see testmodel.TestmodelPackage#getReferenceTestContainer_OneAttribute()
	 * @model
	 * @generated
	 */
	boolean isOneAttribute();

	/**
	 * Sets the value of the '{@link testmodel.ReferenceTestContainer#isOneAttribute <em>One Attribute</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Attribute</em>' attribute.
	 * @see #isOneAttribute()
	 * @generated
	 */
	void setOneAttribute(boolean value);

	/**
	 * Returns the value of the '<em><b>Many Attributes</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.Boolean}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Attributes</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Attributes</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getReferenceTestContainer_ManyAttributes()
	 * @model
	 * @generated
	 */
	EList<Boolean> getManyAttributes();

} // ReferenceTestContainer
