/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.*;

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
public class Smcubes_modelFactoryImpl extends EFactoryImpl implements Smcubes_modelFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Smcubes_modelFactory init()
	{
		try {
			Smcubes_modelFactory theSmcubes_modelFactory = (Smcubes_modelFactory)EPackage.Registry.INSTANCE.getEFactory(Smcubes_modelPackage.eNS_URI);
			if (theSmcubes_modelFactory != null) {
				return theSmcubes_modelFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Smcubes_modelFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Smcubes_modelFactoryImpl()
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
			case Smcubes_modelPackage.SMCUBES_MODEL: return createSmcubesModel();
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
	public SmcubesModel createSmcubesModel()
	{
		SmcubesModelImpl smcubesModel = new SmcubesModelImpl();
		return smcubesModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Smcubes_modelPackage getSmcubes_modelPackage()
	{
		return (Smcubes_modelPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Smcubes_modelPackage getPackage()
	{
		return Smcubes_modelPackage.eINSTANCE;
	}

} //Smcubes_modelFactoryImpl
