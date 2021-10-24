/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.input_data.util;

import java.util.Map;

import org.eclipse.efbt.cocalimo.smcubes.model.input_data.Input_dataPackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Input_dataXMLProcessor extends XMLProcessor {

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Input_dataXMLProcessor() {
		super((EPackage.Registry.INSTANCE));
		Input_dataPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Input_dataResourceFactoryImpl factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Map<String, Resource.Factory> getRegistrations() {
		if (registrations == null) {
			super.getRegistrations();
			registrations.put(XML_EXTENSION, new Input_dataResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Input_dataResourceFactoryImpl());
		}
		return registrations;
	}

} //Input_dataXMLProcessor
