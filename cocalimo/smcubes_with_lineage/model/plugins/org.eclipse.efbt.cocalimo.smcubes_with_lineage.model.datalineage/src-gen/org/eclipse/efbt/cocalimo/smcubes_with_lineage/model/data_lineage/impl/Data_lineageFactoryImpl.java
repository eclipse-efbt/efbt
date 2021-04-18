/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.*;

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
public class Data_lineageFactoryImpl extends EFactoryImpl implements Data_lineageFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Data_lineageFactory init()
	{
		try {
			Data_lineageFactory theData_lineageFactory = (Data_lineageFactory)EPackage.Registry.INSTANCE.getEFactory(Data_lineagePackage.eNS_URI);
			if (theData_lineageFactory != null) {
				return theData_lineageFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Data_lineageFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_lineageFactoryImpl()
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
			case Data_lineagePackage.DATA_LINEAGE_MODEL: return createDataLineageModel();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DataLineageModel createDataLineageModel()
	{
		DataLineageModelImpl dataLineageModel = new DataLineageModelImpl();
		return dataLineageModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_lineagePackage getData_lineagePackage()
	{
		return (Data_lineagePackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Data_lineagePackage getPackage()
	{
		return Data_lineagePackage.eINSTANCE;
	}

} //Data_lineageFactoryImpl
