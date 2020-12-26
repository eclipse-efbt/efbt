/**
 */
package org.eclipse.efbt.cocamo.core.model.functionality_module;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Functionality Module Set</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A set of FunctionalityModules.
 * Note that FunctionalityModulesSet is also a subclass of FunctionalityModule, so the set can also include FunctionalityModulesSets
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.core.model.functionality_module.FunctionalityModuleSet#getSubTransformationsAndSchemes <em>Sub Transformations And Schemes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleSet()
 * @model extendedMetaData="name='FunctionalityModuleSet' kind='elementOnly'"
 * @generated
 */
public interface FunctionalityModuleSet extends FunctionalityModule
{
	/**
	 * Returns the value of the '<em><b>Sub Transformations And Schemes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocamo.core.model.functionality_module.FunctionalityModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  contained set FunctionalityModules 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sub Transformations And Schemes</em>' containment reference list.
	 * @see org.eclipse.efbt.cocamo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModuleSet_SubTransformationsAndSchemes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='subTransformationsAndSchemes'"
	 * @generated
	 */
	EList<FunctionalityModule> getSubTransformationsAndSchemes();

} // FunctionalityModuleSet
