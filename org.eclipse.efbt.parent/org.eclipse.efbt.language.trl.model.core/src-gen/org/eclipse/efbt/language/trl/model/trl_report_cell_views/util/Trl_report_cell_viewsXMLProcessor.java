/**
 */
package org.eclipse.efbt.language.trl.model.trl_report_cell_views.util;

import java.util.Map;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.Trl_report_cell_viewsPackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Trl_report_cell_viewsXMLProcessor extends XMLProcessor
{

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Trl_report_cell_viewsXMLProcessor()
	{
		super((EPackage.Registry.INSTANCE));
		Trl_report_cell_viewsPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Trl_report_cell_viewsResourceFactoryImpl factory.
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
			registrations.put(XML_EXTENSION, new Trl_report_cell_viewsResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Trl_report_cell_viewsResourceFactoryImpl());
		}
		return registrations;
	}

} //Trl_report_cell_viewsXMLProcessor
