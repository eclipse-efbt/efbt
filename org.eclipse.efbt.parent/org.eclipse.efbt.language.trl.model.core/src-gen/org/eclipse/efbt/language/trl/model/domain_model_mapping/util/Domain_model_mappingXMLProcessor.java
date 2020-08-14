/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping.util;

import java.util.Map;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Domain_model_mappingXMLProcessor extends XMLProcessor
{

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Domain_model_mappingXMLProcessor()
	{
		super((EPackage.Registry.INSTANCE));
		Domain_model_mappingPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Domain_model_mappingResourceFactoryImpl factory.
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
			registrations.put(XML_EXTENSION, new Domain_model_mappingResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Domain_model_mappingResourceFactoryImpl());
		}
		return registrations;
	}

} //Domain_model_mappingXMLProcessor
