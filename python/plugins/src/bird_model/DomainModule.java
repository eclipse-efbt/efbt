/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Domain Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module storing a list of Domains
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.DomainModule#getDomains <em>Domains</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getDomainModule()
 * @model extendedMetaData="name='DomainModule' kind='elementOnly'"
 * @generated
 */
public interface DomainModule extends bird_model.Module {
	/**
	 * Returns the value of the '<em><b>Domains</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.DOMAIN}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of domains contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Domains</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getDomainModule_Domains()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='domains'"
	 * @generated
	 */
	EList<DOMAIN> getDomains();

} // DomainModule
