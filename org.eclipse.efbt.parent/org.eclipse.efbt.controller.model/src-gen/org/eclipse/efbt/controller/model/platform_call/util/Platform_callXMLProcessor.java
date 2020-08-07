/**
 */
package org.eclipse.efbt.controller.model.platform_call.util;

import java.util.Map;

import org.eclipse.efbt.controller.model.platform_call.Platform_callPackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Platform_callXMLProcessor extends XMLProcessor
{

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Platform_callXMLProcessor()
	{
		super((EPackage.Registry.INSTANCE));
		Platform_callPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Platform_callResourceFactoryImpl factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Map<String, Resource.Factory> getRegistrations()
	{
		if (registrations == null)
		{
			super.getRegistrations();
			registrations.put(XML_EXTENSION, new Platform_callResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Platform_callResourceFactoryImpl());
		}
		return registrations;
	}

} //Platform_callXMLProcessor
