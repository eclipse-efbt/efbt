/**
 */
package testModelWithXtextReference;

import org.eclipse.emf.ecore.EObject;

import org.eclipse.emf.eson.xtextintegration.myDsl.Model;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Model With Xtext Reference</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * <ul>
 *   <li>{@link testModelWithXtextReference.TestModelWithXtextReference#getModel <em>Model</em>}</li>
 *   <li>{@link testModelWithXtextReference.TestModelWithXtextReference#getName <em>Name</em>}</li>
 * </ul>
 * </p>
 *
 * @see testModelWithXtextReference.TestModelWithXtextReferencePackage#getTestModelWithXtextReference()
 * @model
 * @generated
 */
public interface TestModelWithXtextReference extends EObject {
	/**
	 * Returns the value of the '<em><b>Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Model</em>' reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Model</em>' reference.
	 * @see #setModel(Model)
	 * @see testModelWithXtextReference.TestModelWithXtextReferencePackage#getTestModelWithXtextReference_Model()
	 * @model
	 * @generated
	 */
	Model getModel();

	/**
	 * Sets the value of the '{@link testModelWithXtextReference.TestModelWithXtextReference#getModel <em>Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Model</em>' reference.
	 * @see #getModel()
	 * @generated
	 */
	void setModel(Model value);

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
	 * @see testModelWithXtextReference.TestModelWithXtextReferencePackage#getTestModelWithXtextReference_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link testModelWithXtextReference.TestModelWithXtextReference#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // TestModelWithXtextReference
