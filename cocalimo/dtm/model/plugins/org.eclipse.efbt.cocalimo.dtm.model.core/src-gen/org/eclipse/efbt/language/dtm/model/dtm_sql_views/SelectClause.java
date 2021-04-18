/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Select Clause</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Set of Functions representing a select clause in SQL
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.SelectClause#getAggregateFunctions <em>Aggregate Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.SelectClause#getBasicFunctions <em>Basic Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.SelectClause#getVaraibles <em>Varaibles</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getSelectClause()
 * @model extendedMetaData="name='SelectClause' kind='empty'"
 * @generated
 */
public interface SelectClause extends EObject
{
	/**
	 * Returns the value of the '<em><b>Aggregate Functions</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Aggregating functions
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Aggregate Functions</em>' reference list.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getSelectClause_AggregateFunctions()
	 * @model extendedMetaData="kind='attribute' name='aggregateFunctions'"
	 * @generated
	 */
	EList<AggregateFunction> getAggregateFunctions();

	/**
	 * Returns the value of the '<em><b>Basic Functions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Non-aggregating functions
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Basic Functions</em>' reference.
	 * @see #setBasicFunctions(BasicFunction)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getSelectClause_BasicFunctions()
	 * @model extendedMetaData="kind='attribute' name='basicFunctions'"
	 * @generated
	 */
	BasicFunction getBasicFunctions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.SelectClause#getBasicFunctions <em>Basic Functions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Basic Functions</em>' reference.
	 * @see #getBasicFunctions()
	 * @generated
	 */
	void setBasicFunctions(BasicFunction value);

	/**
	 * Returns the value of the '<em><b>Varaibles</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Column
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Varaibles</em>' reference list.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getSelectClause_Varaibles()
	 * @model extendedMetaData="kind='attribute' name='varaibles'"
	 * @generated
	 */
	EList<VARIABLE> getVaraibles();

} // SelectClause
