/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.util;

import org.eclipse.emf.common.util.URI;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.resource.impl.ResourceFactoryImpl;

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.util.Cocalimo_smcubes_extensionResourceImpl
 * @generated
 */
public class Cocalimo_smcubes_extensionResourceFactoryImpl extends ResourceFactoryImpl {
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cocalimo_smcubes_extensionResourceFactoryImpl() {
		super();
	}

	/**
	 * Creates an instance of the resource.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Resource createResource(URI uri) {
		Resource result = new Cocalimo_smcubes_extensionResourceImpl(uri);
		return result;
	}

} //Cocalimo_smcubes_extensionResourceFactoryImpl
