/**
 */
package testmodel;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Child</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link testmodel.Child#getReferenceList <em>Reference List</em>}</li>
 * </ul>
 *
 * @see testmodel.TestmodelPackage#getChild()
 * @model
 * @generated
 */
public interface Child extends EObject {
	/**
	 * Returns the value of the '<em><b>Reference List</b></em>' reference list.
	 * The list contents are of type {@link testmodel.SingleRequired}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Reference List</em>' reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Reference List</em>' reference list.
	 * @see testmodel.TestmodelPackage#getChild_ReferenceList()
	 * @model
	 * @generated
	 */
	EList<SingleRequired> getReferenceList();

} // Child
