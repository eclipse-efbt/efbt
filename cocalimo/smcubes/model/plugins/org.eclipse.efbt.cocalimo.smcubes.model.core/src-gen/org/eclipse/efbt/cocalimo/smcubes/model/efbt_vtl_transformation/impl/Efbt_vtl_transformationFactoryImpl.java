/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Efbt_vtl_transformationFactoryImpl extends EFactoryImpl implements Efbt_vtl_transformationFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Efbt_vtl_transformationFactory init()
	{
		try {
			Efbt_vtl_transformationFactory theEfbt_vtl_transformationFactory = (Efbt_vtl_transformationFactory)EPackage.Registry.INSTANCE.getEFactory(Efbt_vtl_transformationPackage.eNS_URI);
			if (theEfbt_vtl_transformationFactory != null) {
				return theEfbt_vtl_transformationFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Efbt_vtl_transformationFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Efbt_vtl_transformationFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID()) {
			case Efbt_vtl_transformationPackage.TRANSFORMATION_SCHEME_MODULE: return createTransformationSchemeModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TransformationSchemeModule createTransformationSchemeModule()
	{
		TransformationSchemeModuleImpl transformationSchemeModule = new TransformationSchemeModuleImpl();
		return transformationSchemeModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Efbt_vtl_transformationPackage getEfbt_vtl_transformationPackage()
	{
		return (Efbt_vtl_transformationPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Efbt_vtl_transformationPackage getPackage()
	{
		return Efbt_vtl_transformationPackage.eINSTANCE;
	}

} //Efbt_vtl_transformationFactoryImpl
