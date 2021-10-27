/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.input_data.*;

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
public class Input_dataFactoryImpl extends EFactoryImpl implements Input_dataFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Input_dataFactory init() {
		try {
			Input_dataFactory theInput_dataFactory = (Input_dataFactory)EPackage.Registry.INSTANCE.getEFactory(Input_dataPackage.eNS_URI);
			if (theInput_dataFactory != null) {
				return theInput_dataFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Input_dataFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Input_dataFactoryImpl() {
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
			case Input_dataPackage.CELL_WITH_ENUMERATED_VALUE: return createCellWithEnumeratedValue();
			case Input_dataPackage.CELL_WITH_VALUE: return createCellWithValue();
			case Input_dataPackage.INPUT_DATA: return createInputData();
			case Input_dataPackage.CELL: return createCell();
			case Input_dataPackage.CUBE_DATA: return createCubeData();
			case Input_dataPackage.ROW_DATA: return createRowData();
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
	public CellWithEnumeratedValue createCellWithEnumeratedValue() {
		CellWithEnumeratedValueImpl cellWithEnumeratedValue = new CellWithEnumeratedValueImpl();
		return cellWithEnumeratedValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CellWithValue createCellWithValue() {
		CellWithValueImpl cellWithValue = new CellWithValueImpl();
		return cellWithValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public InputData createInputData() {
		InputDataImpl inputData = new InputDataImpl();
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Cell createCell() {
		CellImpl cell = new CellImpl();
		return cell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeData createCubeData() {
		CubeDataImpl cubeData = new CubeDataImpl();
		return cubeData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RowData createRowData() {
		RowDataImpl rowData = new RowDataImpl();
		return rowData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Input_dataPackage getInput_dataPackage() {
		return (Input_dataPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Input_dataPackage getPackage() {
		return Input_dataPackage.eINSTANCE;
	}

} //Input_dataFactoryImpl
