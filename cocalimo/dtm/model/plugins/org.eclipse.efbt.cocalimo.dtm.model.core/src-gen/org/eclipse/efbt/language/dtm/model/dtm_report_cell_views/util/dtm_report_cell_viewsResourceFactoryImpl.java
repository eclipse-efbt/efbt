/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.util;

import org.eclipse.emf.common.util.URI;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.resource.impl.ResourceFactoryImpl;

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.util.dtm_report_cell_viewsResourceImpl
 * @generated NOT
 */
public class dtm_report_cell_viewsResourceFactoryImpl extends ResourceFactoryImpl {
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated NOT
	 */
	public dtm_report_cell_viewsResourceFactoryImpl() {
		super();
	}

	/**
	 * Creates an instance of the resource.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated NOT
	 */
	@Override
	public Resource createResource(URI uri) {
		Resource result = new dtm_report_cell_viewsResourceImpl(uri, getMapper());
		return result;
	}

} //dtm_report_cell_viewsResourceFactoryImpl
