/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.util;

import org.eclipse.emf.common.util.URI;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.resource.impl.ResourceFactoryImpl;

/**
 * <!-- begin-user-doc -->
 * The <b>Resource Factory</b> associated with the package.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.util.openregspecs_smcubes_core_extensionResourceImpl
 * @generated
 */
public class openregspecs_smcubes_core_extensionResourceFactoryImpl extends ResourceFactoryImpl {
	/**
	 * Creates an instance of the resource factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public openregspecs_smcubes_core_extensionResourceFactoryImpl() {
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
		Resource result = new openregspecs_smcubes_core_extensionResourceImpl(uri);
		return result;
	}

} //openregspecs_smcubes_core_extensionResourceFactoryImpl
