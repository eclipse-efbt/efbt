/**
 */
package testmodel;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Abstract</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link testmodel.Abstract#isParentAttribute <em>Parent Attribute</em>}</li>
 *   <li>{@link testmodel.Abstract#getParentReference <em>Parent Reference</em>}</li>
 *   <li>{@link testmodel.Abstract#getParentContainment <em>Parent Containment</em>}</li>
 * </ul>
 *
 * @see testmodel.TestmodelPackage#getAbstract()
 * @model abstract="true"
 * @generated
 */
public interface Abstract extends EObject {
	/**
	 * Returns the value of the '<em><b>Parent Attribute</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Parent Attribute</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parent Attribute</em>' attribute.
	 * @see #setParentAttribute(boolean)
	 * @see testmodel.TestmodelPackage#getAbstract_ParentAttribute()
	 * @model
	 * @generated
	 */
	boolean isParentAttribute();

	/**
	 * Sets the value of the '{@link testmodel.Abstract#isParentAttribute <em>Parent Attribute</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parent Attribute</em>' attribute.
	 * @see #isParentAttribute()
	 * @generated
	 */
	void setParentAttribute(boolean value);

	/**
	 * Returns the value of the '<em><b>Parent Reference</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Parent Reference</em>' reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parent Reference</em>' reference.
	 * @see #setParentReference(TestModel)
	 * @see testmodel.TestmodelPackage#getAbstract_ParentReference()
	 * @model
	 * @generated
	 */
	TestModel getParentReference();

	/**
	 * Sets the value of the '{@link testmodel.Abstract#getParentReference <em>Parent Reference</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parent Reference</em>' reference.
	 * @see #getParentReference()
	 * @generated
	 */
	void setParentReference(TestModel value);

	/**
	 * Returns the value of the '<em><b>Parent Containment</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Parent Containment</em>' reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parent Containment</em>' reference.
	 * @see #setParentContainment(Interface)
	 * @see testmodel.TestmodelPackage#getAbstract_ParentContainment()
	 * @model
	 * @generated
	 */
	Interface getParentContainment();

	/**
	 * Sets the value of the '{@link testmodel.Abstract#getParentContainment <em>Parent Containment</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parent Containment</em>' reference.
	 * @see #getParentContainment()
	 * @generated
	 */
	void setParentContainment(Interface value);

} // Abstract
