/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.util;

import java.util.Map;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_structureXMLProcessor extends XMLProcessor {

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_structureXMLProcessor() {
		super((EPackage.Registry.INSTANCE));
		Ecore_plus_structurePackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Ecore_plus_structureResourceFactoryImpl factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Map<String, Resource.Factory> getRegistrations() {
		if (registrations == null) {
			super.getRegistrations();
			registrations.put(XML_EXTENSION, new Ecore_plus_structureResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Ecore_plus_structureResourceFactoryImpl());
		}
		return registrations;
	}

} //Ecore_plus_structureXMLProcessor
