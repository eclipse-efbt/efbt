/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.util;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.emfjson.jackson.resource.JsonResourceFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.dtm_sql_views.util.dtm_sql_viewsResourceImpl
 * @generated NOT
 */
public class dtm_sql_viewsResourceFactoryImpl extends JsonResourceFactory {
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated NOT
	 */
	public dtm_sql_viewsResourceFactoryImpl() {
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
		Resource result = new dtm_sql_viewsResourceImpl(uri, getMapper());
		return result;
	}

} //dtm_sql_viewsResourceFactoryImpl
