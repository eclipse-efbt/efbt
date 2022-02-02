/**
 */
package data_lineage_functions.impl;

import data_lineage_functions.*;

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
public class Data_lineage_functionsFactoryImpl extends EFactoryImpl implements Data_lineage_functionsFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Data_lineage_functionsFactory init() {
		try {
			Data_lineage_functionsFactory theData_lineage_functionsFactory = (Data_lineage_functionsFactory)EPackage.Registry.INSTANCE.getEFactory(Data_lineage_functionsPackage.eNS_URI);
			if (theData_lineage_functionsFactory != null) {
				return theData_lineage_functionsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Data_lineage_functionsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_lineage_functionsFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Data_lineage_functionsPackage.CELLS_PARAMETER: return createCellsParameter();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CellsParameter createCellsParameter() {
		CellsParameterImpl cellsParameter = new CellsParameterImpl();
		return cellsParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_lineage_functionsPackage getData_lineage_functionsPackage() {
		return (Data_lineage_functionsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Data_lineage_functionsPackage getPackage() {
		return Data_lineage_functionsPackage.eINSTANCE;
	}

} //Data_lineage_functionsFactoryImpl
