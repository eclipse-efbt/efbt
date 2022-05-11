/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SUBDOMAIN</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.SUBDOMAIN#getCode <em>Code</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#getDescription <em>Description</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#getDomain_id <em>Domain id</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#isIs_listed <em>Is listed</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#isIs_natural <em>Is natural</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link bird_model.SUBDOMAIN#getItems <em>Items</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getSUBDOMAIN()
 * @model extendedMetaData="name='SUBDOMAIN' kind='empty'"
 * @generated
 */
public interface SUBDOMAIN extends Classifier {
	/**
	 * Returns the value of the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code</em>' attribute.
	 * @see #setCode(String)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Code()
	 * @model extendedMetaData="kind='attribute' name='code'"
	 * @generated
	 */
	String getCode();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#getCode <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Code</em>' attribute.
	 * @see #getCode()
	 * @generated
	 */
	void setCode(String value);

	/**
	 * Returns the value of the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Description</em>' attribute.
	 * @see #setDescription(String)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Description()
	 * @model extendedMetaData="kind='attribute' name='description'"
	 * @generated
	 */
	String getDescription();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#getDescription <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Description</em>' attribute.
	 * @see #getDescription()
	 * @generated
	 */
	void setDescription(String value);

	/**
	 * Returns the value of the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Domain id</em>' reference.
	 * @see #setDomain_id(DOMAIN)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Domain_id()
	 * @model extendedMetaData="kind='attribute' name='domain_id'"
	 * @generated
	 */
	DOMAIN getDomain_id();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#getDomain_id <em>Domain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Domain id</em>' reference.
	 * @see #getDomain_id()
	 * @generated
	 */
	void setDomain_id(DOMAIN value);

	/**
	 * Returns the value of the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Facet id</em>' reference.
	 * @see #setFacet_id(FACET_COLLECTION)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Facet_id()
	 * @model extendedMetaData="kind='attribute' name='facet_id'"
	 * @generated
	 */
	FACET_COLLECTION getFacet_id();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#getFacet_id <em>Facet id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Facet id</em>' reference.
	 * @see #getFacet_id()
	 * @generated
	 */
	void setFacet_id(FACET_COLLECTION value);

	/**
	 * Returns the value of the '<em><b>Is listed</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is listed</em>' attribute.
	 * @see #setIs_listed(boolean)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Is_listed()
	 * @model extendedMetaData="kind='attribute' name='IS_LISTED'"
	 * @generated
	 */
	boolean isIs_listed();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#isIs_listed <em>Is listed</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is listed</em>' attribute.
	 * @see #isIs_listed()
	 * @generated
	 */
	void setIs_listed(boolean value);

	/**
	 * Returns the value of the '<em><b>Is natural</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is natural</em>' attribute.
	 * @see #setIs_natural(boolean)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Is_natural()
	 * @model extendedMetaData="kind='attribute' name='is_natural'"
	 * @generated
	 */
	boolean isIs_natural();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#isIs_natural <em>Is natural</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is natural</em>' attribute.
	 * @see #isIs_natural()
	 * @generated
	 */
	void setIs_natural(boolean value);

	/**
	 * Returns the value of the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Maintenance agency id</em>' reference.
	 * @see #setMaintenance_agency_id(MAINTENANCE_AGENCY)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Maintenance_agency_id()
	 * @model extendedMetaData="kind='attribute' name='maintenance_agency_id'"
	 * @generated
	 */
	MAINTENANCE_AGENCY getMaintenance_agency_id();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Maintenance agency id</em>' reference.
	 * @see #getMaintenance_agency_id()
	 * @generated
	 */
	void setMaintenance_agency_id(MAINTENANCE_AGENCY value);

	/**
	 * Returns the value of the '<em><b>Subdomain id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Subdomain id</em>' attribute.
	 * @see #setSubdomain_id(String)
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Subdomain_id()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='subdomain_id'"
	 * @generated
	 */
	String getSubdomain_id();

	/**
	 * Sets the value of the '{@link bird_model.SUBDOMAIN#getSubdomain_id <em>Subdomain id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Subdomain id</em>' attribute.
	 * @see #getSubdomain_id()
	 * @generated
	 */
	void setSubdomain_id(String value);

	/**
	 * Returns the value of the '<em><b>Items</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.SUBDOMAIN_ENUMERATION}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Items</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getSUBDOMAIN_Items()
	 * @model containment="true"
	 * @generated
	 */
	EList<SUBDOMAIN_ENUMERATION> getItems();

} // SUBDOMAIN
