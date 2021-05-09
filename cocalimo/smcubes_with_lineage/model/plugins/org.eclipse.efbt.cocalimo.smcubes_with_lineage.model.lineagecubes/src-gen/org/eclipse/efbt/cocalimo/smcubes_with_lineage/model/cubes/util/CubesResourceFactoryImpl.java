/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.util;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.emfjson.jackson.resource.JsonResourceFactory; 

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.util.CubesResourceImpl
 * @generated NOT
 */
public class CubesResourceFactoryImpl extends JsonResourceFactory
{
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated NOT
	 */
	public CubesResourceFactoryImpl()
	{
		super();
	}

	/**
	 * Creates an instance of the resource.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated NOT
	 */
	@Override
	public Resource createResource(URI uri)
	{
		Resource result = new CubesResourceImpl(uri, getMapper());
		return result;
	}

} //CubesResourceFactoryImpl
