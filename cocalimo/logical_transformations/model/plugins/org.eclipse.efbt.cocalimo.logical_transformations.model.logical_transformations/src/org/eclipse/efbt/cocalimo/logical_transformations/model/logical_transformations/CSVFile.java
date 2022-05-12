/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CSV File</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.CSVFile#getFileName <em>File Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getCSVFile()
 * @model
 * @generated
 */
public interface CSVFile extends EObject {
	/**
	 * Returns the value of the '<em><b>File Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>File Name</em>' attribute.
	 * @see #setFileName(String)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getCSVFile_FileName()
	 * @model
	 * @generated
	 */
	String getFileName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.CSVFile#getFileName <em>File Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>File Name</em>' attribute.
	 * @see #getFileName()
	 * @generated
	 */
	void setFileName(String value);

} // CSVFile
