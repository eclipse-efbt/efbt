/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>DTM View</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An DTMView represents a DataSetTransformation which transforms   data from one DataSet to another in an immutable side-effect free fashion. They use concepts from the well-known SQL StructuredQueryLanguage.
 *     
 *     The transformed dataset is created just once by the transformation and then never amended (it is immutable)
 *     The only data that can be used in the transformation is the data from the source dataset(s), so it cannot use anything else including
 *     'global variables' (which do not exist in the compitational model, the only data created by the transformation is the data in the resulting transformed dataset,
 *     no other data is created or affected so the transformations are side-effect free.
 * 
 * 
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getDTMView()
 * @model extendedMetaData="name='DTMView' kind='empty'"
 * @generated
 */
public interface DTMView extends DataSetTransformation {
} // DTMView
