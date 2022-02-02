/**
 */
package sql_lite;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link sql_lite.View#getSelectClause <em>Select Clause</em>}</li>
 *   <li>{@link sql_lite.View#getFromClause <em>From Clause</em>}</li>
 *   <li>{@link sql_lite.View#getWhereClause <em>Where Clause</em>}</li>
 * </ul>
 *
 * @see sql_lite.Sql_litePackage#getView()
 * @model
 * @generated
 */
public interface View extends SQLEntity {
	/**
	 * Returns the value of the '<em><b>Select Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Select Clause</em>' containment reference.
	 * @see #setSelectClause(SelectClause)
	 * @see sql_lite.Sql_litePackage#getView_SelectClause()
	 * @model containment="true"
	 * @generated
	 */
	SelectClause getSelectClause();

	/**
	 * Sets the value of the '{@link sql_lite.View#getSelectClause <em>Select Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Select Clause</em>' containment reference.
	 * @see #getSelectClause()
	 * @generated
	 */
	void setSelectClause(SelectClause value);

	/**
	 * Returns the value of the '<em><b>From Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>From Clause</em>' containment reference.
	 * @see #setFromClause(FromClause)
	 * @see sql_lite.Sql_litePackage#getView_FromClause()
	 * @model containment="true"
	 * @generated
	 */
	FromClause getFromClause();

	/**
	 * Sets the value of the '{@link sql_lite.View#getFromClause <em>From Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>From Clause</em>' containment reference.
	 * @see #getFromClause()
	 * @generated
	 */
	void setFromClause(FromClause value);

	/**
	 * Returns the value of the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Where Clause</em>' containment reference.
	 * @see #setWhereClause(WhereClause)
	 * @see sql_lite.Sql_litePackage#getView_WhereClause()
	 * @model containment="true"
	 * @generated
	 */
	WhereClause getWhereClause();

	/**
	 * Sets the value of the '{@link sql_lite.View#getWhereClause <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Where Clause</em>' containment reference.
	 * @see #getWhereClause()
	 * @generated
	 */
	void setWhereClause(WhereClause value);

} // View
