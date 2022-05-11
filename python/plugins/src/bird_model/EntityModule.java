/**
 */
package bird_model;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Entity Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.EntityModule#getNsURI <em>Ns URI</em>}</li>
 *   <li>{@link bird_model.EntityModule#getNsPrefix <em>Ns Prefix</em>}</li>
 *   <li>{@link bird_model.EntityModule#getEntities <em>Entities</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getEntityModule()
 * @model
 * @generated
 */
public interface EntityModule extends bird_model.Module {
	/**
	 * Returns the value of the '<em><b>Ns URI</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ns URI</em>' attribute.
	 * @see #setNsURI(String)
	 * @see bird_model.Bird_modelPackage#getEntityModule_NsURI()
	 * @model
	 * @generated
	 */
	String getNsURI();

	/**
	 * Sets the value of the '{@link bird_model.EntityModule#getNsURI <em>Ns URI</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ns URI</em>' attribute.
	 * @see #getNsURI()
	 * @generated
	 */
	void setNsURI(String value);

	/**
	 * Returns the value of the '<em><b>Ns Prefix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ns Prefix</em>' attribute.
	 * @see #setNsPrefix(String)
	 * @see bird_model.Bird_modelPackage#getEntityModule_NsPrefix()
	 * @model
	 * @generated
	 */
	String getNsPrefix();

	/**
	 * Sets the value of the '{@link bird_model.EntityModule#getNsPrefix <em>Ns Prefix</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ns Prefix</em>' attribute.
	 * @see #getNsPrefix()
	 * @generated
	 */
	void setNsPrefix(String value);

	/**
	 * Returns the value of the '<em><b>Entities</b></em>' containment reference list.
	 * The list contents are of type {@link bird_model.Entity}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entities</em>' containment reference list.
	 * @see bird_model.Bird_modelPackage#getEntityModule_Entities()
	 * @model containment="true"
	 * @generated
	 */
	EList<Entity> getEntities();

} // EntityModule
