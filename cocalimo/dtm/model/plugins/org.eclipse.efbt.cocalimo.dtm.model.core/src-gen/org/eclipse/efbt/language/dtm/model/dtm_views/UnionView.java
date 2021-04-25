/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Union View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An DTMView which represents a Union  of  multiple source cubes of the associated DerivedCube
 *       .The source cubes must have the same Structure.      This is equivalent to Union of tables in  SQL. 
 *       The resulting cube structure will be the same as the source cubes and include all the rows from all the source cubes.
 *     
 *       
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getUnionView()
 * @model extendedMetaData="name='UnionView' kind='empty'"
 * @generated
 */
public interface UnionView extends DTMView {
} // UnionView
