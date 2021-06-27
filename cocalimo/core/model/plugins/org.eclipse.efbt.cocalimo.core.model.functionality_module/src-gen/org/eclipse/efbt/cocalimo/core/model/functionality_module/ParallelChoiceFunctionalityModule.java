/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Parallel Choice Functionality Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A FunctionalityModuleSet, where the sub FunctionalityModules can be run in parallel , or in any order, since there is no dependency between them whereby one needs to run before another. ParallelChoiceFunctionalityModule represents the case where one or more of the contained moduels are run, unlike ParallelFunctionalityModule where all should be run.
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getParallelChoiceFunctionalityModule()
 * @model extendedMetaData="name='ParallelChoiceFunctionalityModule' kind='elementOnly'"
 * @generated
 */
public interface ParallelChoiceFunctionalityModule extends FunctionalityModuleSet {
} // ParallelChoiceFunctionalityModule
