/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Variable Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a list of variables
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.VariableModule#getVariables <em>Variables</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getVariableModule()
 * @model extendedMetaData="name='VariableModule' kind='elementOnly'"
 * @generated
 */
public interface VariableModule extends bird_model.Module {
	/**
	 * Returns the value of the '<em><b>Variables</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.VARIABLE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of Variables  contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Variables</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getVariableModule_Variables()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='variables'"
	 * @generated
	 */
	EList<VARIABLE> getVariables();

} // VariableModule
