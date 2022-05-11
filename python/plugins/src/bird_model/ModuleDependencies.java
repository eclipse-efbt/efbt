/**
 */
package bird_model;

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
 *   <li>{@link bird_model.ModuleDependencies#getTheModules <em>The Modules</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getModuleDependencies()
 * @model extendedMetaData="name='ModuleDependencies' kind='elementOnly'"
 * @generated
 */
public interface ModuleDependencies extends EObject {
	/**
	 * Returns the value of the '<em><b>The Modules</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.ModuleDependency}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The dependent Modules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>The Modules</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getModuleDependencies_TheModules()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='theModules'"
	 * @generated
	 */
	EList<ModuleDependency> getTheModules();

} // ModuleDependencies
