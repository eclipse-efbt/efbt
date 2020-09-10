/**
 */
package testmodel;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Name Attribute Container</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link testmodel.NameAttributeContainer#getDefaultNameTest <em>Default Name Test</em>}</li>
 *   <li>{@link testmodel.NameAttributeContainer#getCustomNameTest <em>Custom Name Test</em>}</li>
 *   <li>{@link testmodel.NameAttributeContainer#getName <em>Name</em>}</li>
 *   <li>{@link testmodel.NameAttributeContainer#getNoNameTest <em>No Name Test</em>}</li>
 * </ul>
 *
 * @see testmodel.TestmodelPackage#getNameAttributeContainer()
 * @model
 * @generated
 */
public interface NameAttributeContainer extends EObject {
	/**
	 * Returns the value of the '<em><b>Default Name Test</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Default Name Test</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Default Name Test</em>' containment reference.
	 * @see #setDefaultNameTest(DefaultName)
	 * @see testmodel.TestmodelPackage#getNameAttributeContainer_DefaultNameTest()
	 * @model containment="true"
	 * @generated
	 */
	DefaultName getDefaultNameTest();

	/**
	 * Sets the value of the '{@link testmodel.NameAttributeContainer#getDefaultNameTest <em>Default Name Test</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Default Name Test</em>' containment reference.
	 * @see #getDefaultNameTest()
	 * @generated
	 */
	void setDefaultNameTest(DefaultName value);

	/**
	 * Returns the value of the '<em><b>Custom Name Test</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Custom Name Test</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Custom Name Test</em>' containment reference.
	 * @see #setCustomNameTest(CustomName)
	 * @see testmodel.TestmodelPackage#getNameAttributeContainer_CustomNameTest()
	 * @model containment="true"
	 * @generated
	 */
	CustomName getCustomNameTest();

	/**
	 * Sets the value of the '{@link testmodel.NameAttributeContainer#getCustomNameTest <em>Custom Name Test</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Custom Name Test</em>' containment reference.
	 * @see #getCustomNameTest()
	 * @generated
	 */
	void setCustomNameTest(CustomName value);

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
	 * @see testmodel.TestmodelPackage#getNameAttributeContainer_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link testmodel.NameAttributeContainer#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>No Name Test</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>No Name Test</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>No Name Test</em>' containment reference.
	 * @see #setNoNameTest(NoName)
	 * @see testmodel.TestmodelPackage#getNameAttributeContainer_NoNameTest()
	 * @model containment="true"
	 * @generated
	 */
	NoName getNoNameTest();

	/**
	 * Sets the value of the '{@link testmodel.NameAttributeContainer#getNoNameTest <em>No Name Test</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>No Name Test</em>' containment reference.
	 * @see #getNoNameTest()
	 * @generated
	 */
	void setNoNameTest(NoName value);

} // NameAttributeContainer
