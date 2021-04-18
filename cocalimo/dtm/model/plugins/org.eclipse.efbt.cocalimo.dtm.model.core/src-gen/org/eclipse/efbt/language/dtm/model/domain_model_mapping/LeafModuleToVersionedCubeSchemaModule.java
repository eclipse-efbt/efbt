/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.LeafFunctionalityModule;

import org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Leaf Module To Versioned Cube Schema Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule#getScheme <em>Scheme</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule#getModule <em>Module</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getLeafModuleToVersionedCubeSchemaModule()
 * @model
 * @generated
 */
public interface LeafModuleToVersionedCubeSchemaModule extends EObject
{
	/**
	 * Returns the value of the '<em><b>Scheme</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scheme</em>' reference.
	 * @see #setScheme(LeafFunctionalityModule)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getLeafModuleToVersionedCubeSchemaModule_Scheme()
	 * @model
	 * @generated
	 */
	LeafFunctionalityModule getScheme();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule#getScheme <em>Scheme</em>}' reference.
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
	 * @see #setModule(VersionedCubeSchemaModule)
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage#getLeafModuleToVersionedCubeSchemaModule_Module()
	 * @model
	 * @generated
	 */
	VersionedCubeSchemaModule getModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule#getModule <em>Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Module</em>' reference.
	 * @see #getModule()
	 * @generated
	 */
	void setModule(VersionedCubeSchemaModule value);

} // LeafModuleToVersionedCubeSchemaModule
