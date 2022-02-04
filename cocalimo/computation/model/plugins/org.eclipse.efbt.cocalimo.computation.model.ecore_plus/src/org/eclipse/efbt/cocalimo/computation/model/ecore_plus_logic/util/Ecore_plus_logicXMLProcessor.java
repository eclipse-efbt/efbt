/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.util;

import java.util.Map;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_logicXMLProcessor extends XMLProcessor {

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicXMLProcessor() {
		super((EPackage.Registry.INSTANCE));
		Ecore_plus_logicPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Ecore_plus_logicResourceFactoryImpl factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Map<String, Resource.Factory> getRegistrations() {
		if (registrations == null) {
			super.getRegistrations();
			registrations.put(XML_EXTENSION, new Ecore_plus_logicResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Ecore_plus_logicResourceFactoryImpl());
		}
		return registrations;
	}

} //Ecore_plus_logicXMLProcessor
