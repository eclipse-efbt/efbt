/**
 */
package org.eclipse.efbt.openregspecs.model.module_management;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Module Dependencies</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * a  list of module dependencies
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.module_management.ModuleDependencies#getTheModules <em>The Modules</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage#getModuleDependencies()
 * @model extendedMetaData="name='ModuleDependencies' kind='elementOnly'"
 * @generated
 */
public interface ModuleDependencies extends EObject {
	/**
	 * Returns the value of the '<em><b>The Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.module_management.ModuleDependency}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The dependent Modules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>The Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage#getModuleDependencies_TheModules()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='theModules'"
	 * @generated
	 */
	EList<ModuleDependency> getTheModules();

} // ModuleDependencies
