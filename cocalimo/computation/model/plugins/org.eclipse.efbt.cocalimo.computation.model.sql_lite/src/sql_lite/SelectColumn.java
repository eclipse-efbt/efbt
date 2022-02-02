/**
 */
package sql_lite;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Select Column</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link sql_lite.SelectColumn#getAs <em>As</em>}</li>
 * </ul>
 *
 * @see sql_lite.Sql_litePackage#getSelectColumn()
 * @model abstract="true"
 * @generated
 */
public interface SelectColumn extends EObject {
	/**
	 * Returns the value of the '<em><b>As</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>As</em>' reference.
	 * @see #setAs(EAttribute)
	 * @see sql_lite.Sql_litePackage#getSelectColumn_As()
	 * @model
	 * @generated
	 */
	EAttribute getAs();

	/**
	 * Sets the value of the '{@link sql_lite.SelectColumn#getAs <em>As</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>As</em>' reference.
	 * @see #getAs()
	 * @generated
	 */
	void setAs(EAttribute value);

} // SelectColumn
