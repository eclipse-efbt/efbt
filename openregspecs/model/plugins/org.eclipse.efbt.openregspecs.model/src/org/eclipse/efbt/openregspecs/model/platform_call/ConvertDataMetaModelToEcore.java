/**
 */
package org.eclipse.efbt.openregspecs.model.platform_call;

import org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Convert Data Meta Model To Ecore</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.platform_call.ConvertDataMetaModelToEcore#getOutputDirectory <em>Output Directory</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.platform_call.ConvertDataMetaModelToEcore#getEntityModule <em>Entity Module</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getConvertDataMetaModelToEcore()
 * @model
 * @generated
 */
public interface ConvertDataMetaModelToEcore extends PlatformCall {
	/**
	 * Returns the value of the '<em><b>Output Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Output Directory</em>' attribute.
	 * @see #setOutputDirectory(String)
	 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getConvertDataMetaModelToEcore_OutputDirectory()
	 * @model
	 * @generated
	 */
	String getOutputDirectory();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.platform_call.ConvertDataMetaModelToEcore#getOutputDirectory <em>Output Directory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Output Directory</em>' attribute.
	 * @see #getOutputDirectory()
	 * @generated
	 */
	void setOutputDirectory(String value);

	/**
	 * Returns the value of the '<em><b>Entity Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity Module</em>' reference.
	 * @see #setEntityModule(EntityModule)
	 * @see org.eclipse.efbt.openregspecs.model.platform_call.Platform_callPackage#getConvertDataMetaModelToEcore_EntityModule()
	 * @model
	 * @generated
	 */
	EntityModule getEntityModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.platform_call.ConvertDataMetaModelToEcore#getEntityModule <em>Entity Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity Module</em>' reference.
	 * @see #getEntityModule()
	 * @generated
	 */
	void setEntityModule(EntityModule value);

} // ConvertDataMetaModelToEcore
