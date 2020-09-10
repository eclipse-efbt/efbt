/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.util;

import java.util.Map;

import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Base_column_structured_dataXMLProcessor extends XMLProcessor
{

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Base_column_structured_dataXMLProcessor()
	{
		super((EPackage.Registry.INSTANCE));
		Base_column_structured_dataPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Base_column_structured_dataResourceFactoryImpl factory.
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
			registrations.put(XML_EXTENSION, new Base_column_structured_dataResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Base_column_structured_dataResourceFactoryImpl());
		}
		return registrations;
	}

} //Base_column_structured_dataXMLProcessor
