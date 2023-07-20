/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Public Operation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPublicOperation#getCalledPrivateOperations <em>Called Private Operations</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELPublicOperation()
 * @model
 * @generated
 */
public interface ELPublicOperation extends ELOperation {
	/**
	 * Returns the value of the '<em><b>Called Private Operations</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPrivateOperation}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Called Private Operations</em>' reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getELPublicOperation_CalledPrivateOperations()
	 * @model
	 * @generated
	 */
	EList<ELPrivateOperation> getCalledPrivateOperations();

} // ELPublicOperation
