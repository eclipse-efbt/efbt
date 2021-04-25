/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.util;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.emfjson.jackson.resource.JsonResourceFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.util.Efbt_mappingResourceImpl
 * @generated NOT
 */
public class Efbt_mappingResourceFactoryImpl extends JsonResourceFactory {
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated NOT
	 */
	public Efbt_mappingResourceFactoryImpl() {
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
		Resource result = new Efbt_mappingResourceImpl(uri, getMapper());
		return result;
	}

} //Efbt_mappingResourceFactoryImpl
