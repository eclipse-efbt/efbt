/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.*;

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
public class Cell_transformation_logicFactoryImpl extends EFactoryImpl implements Cell_transformation_logicFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Cell_transformation_logicFactory init()
	{
		try {
			Cell_transformation_logicFactory theCell_transformation_logicFactory = (Cell_transformation_logicFactory)EPackage.Registry.INSTANCE.getEFactory(Cell_transformation_logicPackage.eNS_URI);
			if (theCell_transformation_logicFactory != null) {
				return theCell_transformation_logicFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Cell_transformation_logicFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cell_transformation_logicFactoryImpl()
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
			case Cell_transformation_logicPackage.FORMULA_CELL: return createFormulaCell();
			case Cell_transformation_logicPackage.DERIVED_CUBE_DATA: return createDerivedCubeData();
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA: return createDerivedRowData();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FormulaCell createFormulaCell()
	{
		FormulaCellImpl formulaCell = new FormulaCellImpl();
		return formulaCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedCubeData createDerivedCubeData()
	{
		DerivedCubeDataImpl derivedCubeData = new DerivedCubeDataImpl();
		return derivedCubeData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedRowData createDerivedRowData()
	{
		DerivedRowDataImpl derivedRowData = new DerivedRowDataImpl();
		return derivedRowData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cell_transformation_logicPackage getCell_transformation_logicPackage()
	{
		return (Cell_transformation_logicPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Cell_transformation_logicPackage getPackage()
	{
		return Cell_transformation_logicPackage.eINSTANCE;
	}

} //Cell_transformation_logicFactoryImpl
