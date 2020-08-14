/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Column Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A ColumnFunction on rows where there is only ever 1 source row per source cube. If we have a function that uses multiple source rows (such as summing a column from a large set of source rows) we use an AggregateColumnFunction) 
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getBasicColumnFunction()
 * @model extendedMetaData="name='BasicColumnFunction' kind='empty'"
 * @generated
 */
public interface BasicColumnFunction extends ColumnFunction
{
} // BasicColumnFunction
