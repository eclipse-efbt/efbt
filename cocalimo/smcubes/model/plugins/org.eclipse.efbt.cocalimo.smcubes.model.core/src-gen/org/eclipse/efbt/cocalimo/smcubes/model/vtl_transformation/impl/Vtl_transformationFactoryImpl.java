/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.*;

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
public class Vtl_transformationFactoryImpl extends EFactoryImpl implements Vtl_transformationFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Vtl_transformationFactory init()
	{
		try {
			Vtl_transformationFactory theVtl_transformationFactory = (Vtl_transformationFactory)EPackage.Registry.INSTANCE.getEFactory(Vtl_transformationPackage.eNS_URI);
			if (theVtl_transformationFactory != null) {
				return theVtl_transformationFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Vtl_transformationFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Vtl_transformationFactoryImpl()
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
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME: return createTRANSFORMATION_SCHEME();
			case Vtl_transformationPackage.TRANSFORMATION: return createTRANSFORMATION();
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
	public TRANSFORMATION_SCHEME createTRANSFORMATION_SCHEME()
	{
		TRANSFORMATION_SCHEMEImpl transformatioN_SCHEME = new TRANSFORMATION_SCHEMEImpl();
		return transformatioN_SCHEME;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TRANSFORMATION createTRANSFORMATION()
	{
		TRANSFORMATIONImpl transformation = new TRANSFORMATIONImpl();
		return transformation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Vtl_transformationPackage getVtl_transformationPackage()
	{
		return (Vtl_transformationPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Vtl_transformationPackage getPackage()
	{
		return Vtl_transformationPackage.eINSTANCE;
	}

} //Vtl_transformationFactoryImpl
