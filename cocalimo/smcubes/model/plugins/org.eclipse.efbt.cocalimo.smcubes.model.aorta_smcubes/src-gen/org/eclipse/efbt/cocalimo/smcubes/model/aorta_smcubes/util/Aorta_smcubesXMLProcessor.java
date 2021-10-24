/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.util;

import java.util.Map;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Aorta_smcubesXMLProcessor extends XMLProcessor {

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Aorta_smcubesXMLProcessor() {
		super((EPackage.Registry.INSTANCE));
		Aorta_smcubesPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Aorta_smcubesResourceFactoryImpl factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Map<String, Resource.Factory> getRegistrations() {
		if (registrations == null) {
			super.getRegistrations();
			registrations.put(XML_EXTENSION, new Aorta_smcubesResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Aorta_smcubesResourceFactoryImpl());
		}
		return registrations;
	}

} //Aorta_smcubesXMLProcessor
