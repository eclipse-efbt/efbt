/**
 */
package data_lineage_functions.util;

import data_lineage_functions.Data_lineage_functionsPackage;

import java.util.Map;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.util.XMLProcessor;

/**
 * This class contains helper methods to serialize and deserialize XML documents
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Data_lineage_functionsXMLProcessor extends XMLProcessor {

	/**
	 * Public constructor to instantiate the helper.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_lineage_functionsXMLProcessor() {
		super((EPackage.Registry.INSTANCE));
		Data_lineage_functionsPackage.eINSTANCE.eClass();
	}
	
	/**
	 * Register for "*" and "xml" file extensions the Data_lineage_functionsResourceFactoryImpl factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Map<String, Resource.Factory> getRegistrations() {
		if (registrations == null) {
			super.getRegistrations();
			registrations.put(XML_EXTENSION, new Data_lineage_functionsResourceFactoryImpl());
			registrations.put(STAR_EXTENSION, new Data_lineage_functionsResourceFactoryImpl());
		}
		return registrations;
	}

} //Data_lineage_functionsXMLProcessor
