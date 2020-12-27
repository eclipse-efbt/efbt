/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.util;

import org.eclipse.emf.common.util.URI;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.resource.impl.ResourceFactoryImpl;

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.util.Efbt_mappingResourceImpl
 * @generated
 */
public class Efbt_mappingResourceFactoryImpl extends ResourceFactoryImpl
{
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Efbt_mappingResourceFactoryImpl()
	{
		super();
	}

	/**
	 * Creates an instance of the resource.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Resource createResource(URI uri)
	{
		Resource result = new Efbt_mappingResourceImpl(uri);
		return result;
	}

} //Efbt_mappingResourceFactoryImpl
