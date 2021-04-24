/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.language.dtm.model.transformation.VersionedFunctionalModuleLogic;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Versioned DTM Views Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module of DTMViews with a version number
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule#getDTMViews <em>DTM Views</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getVersionedDTMViewsModule()
 * @model extendedMetaData="name='VersionedDTMViewsModule' kind='elementOnly'"
 * @generated
 */
public interface VersionedDTMViewsModule extends VersionedFunctionalModuleLogic {
	/**
	 * Returns the value of the '<em><b>DTM Views</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_views.DTMView}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The DTMViews in the module.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>DTM Views</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getVersionedDTMViewsModule_DTMViews()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='DTMViews'"
	 * @generated
	 */
	EList<DTMView> getDTMViews();

} // VersionedDTMViewsModule
