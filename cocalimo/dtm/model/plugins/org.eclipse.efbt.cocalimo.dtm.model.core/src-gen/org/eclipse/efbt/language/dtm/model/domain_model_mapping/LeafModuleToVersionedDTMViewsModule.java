/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.LeafFunctionalityModule;

import org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Leaf Module To Versioned DTM Views Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedDTMViewsModule#getScheme <em>Scheme</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedDTMViewsModule#getModule <em>Module</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getLeafModuleToVersionedDTMViewsModule()
 * @model
 * @generated
 */
public interface LeafModuleToVersionedDTMViewsModule extends EObject {
	/**
	 * Returns the value of the '<em><b>Scheme</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scheme</em>' reference.
	 * @see #setScheme(LeafFunctionalityModule)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getLeafModuleToVersionedDTMViewsModule_Scheme()
	 * @model
	 * @generated
	 */
	LeafFunctionalityModule getScheme();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedDTMViewsModule#getScheme <em>Scheme</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scheme</em>' reference.
	 * @see #getScheme()
	 * @generated
	 */
	void setScheme(LeafFunctionalityModule value);

	/**
	 * Returns the value of the '<em><b>Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Module</em>' reference.
	 * @see #setModule(VersionedDTMViewsModule)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getLeafModuleToVersionedDTMViewsModule_Module()
	 * @model
	 * @generated
	 */
	VersionedDTMViewsModule getModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedDTMViewsModule#getModule <em>Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Module</em>' reference.
	 * @see #getModule()
	 * @generated
	 */
	void setModule(VersionedDTMViewsModule value);

} // LeafModuleToVersionedDTMViewsModule
