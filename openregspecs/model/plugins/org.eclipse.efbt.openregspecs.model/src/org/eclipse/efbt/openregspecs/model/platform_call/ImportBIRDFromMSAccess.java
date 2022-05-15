/**
 */
package org.eclipse.efbt.openregspecs.model.platform_call;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Import BIRD From MS Access</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.platform_call.ImportBIRDFromMSAccess#getInputDirectory <em>Input Directory</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.platform_call.ImportBIRDFromMSAccess#getOutputDirectory <em>Output Directory</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getImportBIRDFromMSAccess()
 * @model
 * @generated
 */
public interface ImportBIRDFromMSAccess extends PlatformCall {
	/**
	 * Returns the value of the '<em><b>Input Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Input Directory</em>' attribute.
	 * @see #setInputDirectory(String)
	 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getImportBIRDFromMSAccess_InputDirectory()
	 * @model
	 * @generated
	 */
	String getInputDirectory();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.platform_call.ImportBIRDFromMSAccess#getInputDirectory <em>Input Directory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Input Directory</em>' attribute.
	 * @see #getInputDirectory()
	 * @generated
	 */
	void setInputDirectory(String value);

	/**
	 * Returns the value of the '<em><b>Output Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Output Directory</em>' attribute.
	 * @see #setOutputDirectory(String)
	 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getImportBIRDFromMSAccess_OutputDirectory()
	 * @model
	 * @generated
	 */
	String getOutputDirectory();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.platform_call.ImportBIRDFromMSAccess#getOutputDirectory <em>Output Directory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Output Directory</em>' attribute.
	 * @see #getOutputDirectory()
	 * @generated
	 */
	void setOutputDirectory(String value);

} // ImportBIRDFromMSAccess
