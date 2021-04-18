/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views;

import org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SQL View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An SQLView represents a DataSetTransformation which transforms   data from one DataSet to another in an immutable side-effect free fashion. They use concepts from the well-known SQL StructuredQueryLanguage.
 *     
 *     The transformed dataset is created just once by the transformation and then never amended (it is immutable)
 *     The only data that can be used in the transformation is the data from the source dataset(s), so it cannot use anything else including
 *     'global variables' (which do not exist in the compitational model, the only data created by the transformation is the data in the resulting transformed dataset,
 *     no other data is created or affected so the transformations are side-effect free.
 * 
 * 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.SQLView#getVTL_Code <em>VTL Code</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getSQLView()
 * @model extendedMetaData="name='SQLView' kind='empty'"
 * @generated
 */
public interface SQLView extends DataSetTransformation
{
	/**
	 * Returns the value of the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL Code</em>' attribute.
	 * @see #setVTL_Code(String)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage#getSQLView_VTL_Code()
	 * @model
	 * @generated
	 */
	String getVTL_Code();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.SQLView#getVTL_Code <em>VTL Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL Code</em>' attribute.
	 * @see #getVTL_Code()
	 * @generated
	 */
	void setVTL_Code(String value);

} // SQLView
