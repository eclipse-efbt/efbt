/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>EL Private Operation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.ELPrivateOperation#getEParameters <em>EParameters</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELPrivateOperation()
 * @model
 * @generated
 */
public interface ELPrivateOperation extends ELOperation {
	/**
	 * Returns the value of the '<em><b>EParameters</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ELParameter}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>EParameters</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getELPrivateOperation_EParameters()
	 * @model containment="true"
	 * @generated
	 */
	EList<ELParameter> getEParameters();

} // ELPrivateOperation
