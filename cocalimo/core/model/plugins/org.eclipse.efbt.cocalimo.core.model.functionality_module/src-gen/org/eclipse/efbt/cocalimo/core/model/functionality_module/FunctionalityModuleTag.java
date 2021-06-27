/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Functionality Module Tag</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Tag which tags a FunctionalityModule in order to link it with requirements.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag#getFunctionalityModule <em>Functionality Module</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleTag()
 * @model
 * @generated
 */
public interface FunctionalityModuleTag extends Tag {
	/**
	 * Returns the value of the '<em><b>Functionality Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The tagged FunctionalityModule.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functionality Module</em>' reference.
	 * @see #setFunctionalityModule(FunctionalityModule)
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleTag_FunctionalityModule()
	 * @model
	 * @generated
	 */
	FunctionalityModule getFunctionalityModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag#getFunctionalityModule <em>Functionality Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Functionality Module</em>' reference.
	 * @see #getFunctionalityModule()
	 * @generated
	 */
	void setFunctionalityModule(FunctionalityModule value);

} // FunctionalityModuleTag
