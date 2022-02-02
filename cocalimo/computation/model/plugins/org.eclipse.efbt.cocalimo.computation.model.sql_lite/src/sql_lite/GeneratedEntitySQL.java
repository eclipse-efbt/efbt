/**
 */
package sql_lite;

import entities.GeneratedEntity;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Generated Entity SQL</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link sql_lite.GeneratedEntitySQL#getGeneratedEntity <em>Generated Entity</em>}</li>
 *   <li>{@link sql_lite.GeneratedEntitySQL#getView <em>View</em>}</li>
 * </ul>
 *
 * @see sql_lite.Sql_litePackage#getGeneratedEntitySQL()
 * @model
 * @generated
 */
public interface GeneratedEntitySQL extends EObject {
	/**
	 * Returns the value of the '<em><b>Generated Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Generated Entity</em>' reference.
	 * @see #setGeneratedEntity(GeneratedEntity)
	 * @see sql_lite.Sql_litePackage#getGeneratedEntitySQL_GeneratedEntity()
	 * @model
	 * @generated
	 */
	GeneratedEntity getGeneratedEntity();

	/**
	 * Sets the value of the '{@link sql_lite.GeneratedEntitySQL#getGeneratedEntity <em>Generated Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Generated Entity</em>' reference.
	 * @see #getGeneratedEntity()
	 * @generated
	 */
	void setGeneratedEntity(GeneratedEntity value);

	/**
	 * Returns the value of the '<em><b>View</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>View</em>' reference.
	 * @see #setView(View)
	 * @see sql_lite.Sql_litePackage#getGeneratedEntitySQL_View()
	 * @model
	 * @generated
	 */
	View getView();

	/**
	 * Sets the value of the '{@link sql_lite.GeneratedEntitySQL#getView <em>View</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>View</em>' reference.
	 * @see #getView()
	 * @generated
	 */
	void setView(View value);

} // GeneratedEntitySQL
