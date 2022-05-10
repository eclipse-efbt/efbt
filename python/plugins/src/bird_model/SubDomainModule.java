/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Sub Domain Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.SubDomainModule#getSubdomains <em>Subdomains</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getSubDomainModule()
 * @model
 * @generated
 */
public interface SubDomainModule extends bird_model.Module {
	/**
	 * Returns the value of the '<em><b>Subdomains</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.SUBDOMAIN}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomains</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getSubDomainModule_Subdomains()
	 * @model containment="true"
	 * @generated
	 */
	EList<SUBDOMAIN> getSubdomains();

} // SubDomainModule
