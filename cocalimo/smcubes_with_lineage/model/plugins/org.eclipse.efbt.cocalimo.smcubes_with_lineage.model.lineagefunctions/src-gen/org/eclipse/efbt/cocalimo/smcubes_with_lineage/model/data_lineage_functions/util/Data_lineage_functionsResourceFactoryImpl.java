/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.util;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.emfjson.jackson.resource.JsonResourceFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage_functions.util.Data_lineage_functionsResourceImpl
 * @generated NOT
 */
public class Data_lineage_functionsResourceFactoryImpl extends JsonResourceFactory {
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated NOT
	 */
	public Data_lineage_functionsResourceFactoryImpl() {
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
		Resource result = new Data_lineage_functionsResourceImpl(uri, getMapper());
		return result;
	}

} //Data_lineage_functionsResourceFactoryImpl
