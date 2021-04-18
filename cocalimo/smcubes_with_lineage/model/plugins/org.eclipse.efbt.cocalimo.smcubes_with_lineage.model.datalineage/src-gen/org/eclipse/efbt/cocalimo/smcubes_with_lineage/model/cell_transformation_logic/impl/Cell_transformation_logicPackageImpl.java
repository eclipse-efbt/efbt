/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.Data_lineagePackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.Data_lineagePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Cell_transformation_logicPackageImpl extends EPackageImpl implements Cell_transformation_logicPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass formulaCellEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedCubeDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedRowDataEClass = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Cell_transformation_logicPackageImpl()
	{
		super(eNS_URI, Cell_transformation_logicFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link Cell_transformation_logicPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Cell_transformation_logicPackage init()
	{
		if (isInited) return (Cell_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Cell_transformation_logicPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredCell_transformation_logicPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Cell_transformation_logicPackageImpl theCell_transformation_logicPackage = registeredCell_transformation_logicPackage instanceof Cell_transformation_logicPackageImpl ? (Cell_transformation_logicPackageImpl)registeredCell_transformation_logicPackage : new Cell_transformation_logicPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		FunctionsPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Base_column_structured_dataPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_lineagePackage.eNS_URI);
		Data_lineagePackageImpl theData_lineagePackage = (Data_lineagePackageImpl)(registeredPackage instanceof Data_lineagePackageImpl ? registeredPackage : Data_lineagePackage.eINSTANCE);

		// Create package meta-data objects
		theCell_transformation_logicPackage.createPackageContents();
		theData_lineagePackage.createPackageContents();

		// Initialize created meta-data
		theCell_transformation_logicPackage.initializePackageContents();
		theData_lineagePackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theCell_transformation_logicPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Cell_transformation_logicPackage.eNS_URI, theCell_transformation_logicPackage);
		return theCell_transformation_logicPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFormulaCell()
	{
		return formulaCellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFormulaCell_Function()
	{
		return (EReference)formulaCellEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFormulaCell_Value()
	{
		return (EAttribute)formulaCellEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedCubeData()
	{
		return derivedCubeDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedCubeData_Rows()
	{
		return (EReference)derivedCubeDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedCubeData_Cube()
	{
		return (EReference)derivedCubeDataEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedCubeData_SourceCubeData()
	{
		return (EReference)derivedCubeDataEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getDerivedCubeData_CubeCreationDescription()
	{
		return (EAttribute)derivedCubeDataEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedRowData()
	{
		return derivedRowDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedRowData_FormulaCells()
	{
		return (EReference)derivedRowDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedRowData_SpecialDerivedCells()
	{
		return (EReference)derivedRowDataEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedRowData_SourceRows()
	{
		return (EReference)derivedRowDataEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cell_transformation_logicFactory getCell_transformation_logicFactory()
	{
		return (Cell_transformation_logicFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents()
	{
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		formulaCellEClass = createEClass(FORMULA_CELL);
		createEReference(formulaCellEClass, FORMULA_CELL__FUNCTION);
		createEAttribute(formulaCellEClass, FORMULA_CELL__VALUE);

		derivedCubeDataEClass = createEClass(DERIVED_CUBE_DATA);
		createEReference(derivedCubeDataEClass, DERIVED_CUBE_DATA__ROWS);
		createEReference(derivedCubeDataEClass, DERIVED_CUBE_DATA__CUBE);
		createEReference(derivedCubeDataEClass, DERIVED_CUBE_DATA__SOURCE_CUBE_DATA);
		createEAttribute(derivedCubeDataEClass, DERIVED_CUBE_DATA__CUBE_CREATION_DESCRIPTION);

		derivedRowDataEClass = createEClass(DERIVED_ROW_DATA);
		createEReference(derivedRowDataEClass, DERIVED_ROW_DATA__FORMULA_CELLS);
		createEReference(derivedRowDataEClass, DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS);
		createEReference(derivedRowDataEClass, DERIVED_ROW_DATA__SOURCE_ROWS);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents()
	{
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Column_structured_dataPackage theColumn_structured_dataPackage = (Column_structured_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Column_structured_dataPackage.eNS_URI);
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		formulaCellEClass.getESuperTypes().add(theColumn_structured_dataPackage.getCell());
		derivedCubeDataEClass.getESuperTypes().add(theColumn_structured_dataPackage.getColumnStructuredData());
		derivedRowDataEClass.getESuperTypes().add(theColumn_structured_dataPackage.getRowData());

		// Initialize classes, features, and operations; add parameters
		initEClass(formulaCellEClass, FormulaCell.class, "FormulaCell", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFormulaCell_Function(), theFunctionsPackage.getFunction(), null, "function", null, 0, 1, FormulaCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFormulaCell_Value(), ecorePackage.getEString(), "value", null, 0, 1, FormulaCell.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(derivedCubeDataEClass, DerivedCubeData.class, "DerivedCubeData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedCubeData_Rows(), this.getDerivedRowData(), null, "rows", null, 0, -1, DerivedCubeData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedCubeData_Cube(), theCubesPackage.getDerivedCube(), null, "cube", null, 0, 1, DerivedCubeData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedCubeData_SourceCubeData(), theColumn_structured_dataPackage.getColumnStructuredData(), null, "sourceCubeData", null, 0, -1, DerivedCubeData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDerivedCubeData_CubeCreationDescription(), ecorePackage.getEString(), "cubeCreationDescription", null, 0, 1, DerivedCubeData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(derivedRowDataEClass, DerivedRowData.class, "DerivedRowData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedRowData_FormulaCells(), this.getFormulaCell(), null, "formulaCells", null, 0, -1, DerivedRowData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedRowData_SpecialDerivedCells(), this.getFormulaCell(), null, "specialDerivedCells", null, 0, 1, DerivedRowData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedRowData_SourceRows(), theColumn_structured_dataPackage.getRowData(), null, "sourceRows", null, 0, -1, DerivedRowData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
	}

	/**
	 * Initializes the annotations for <b>license</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createLicenseAnnotations()
	{
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations()
	{
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (formulaCellEClass,
		   source,
		   new String[] {
			   "name", "FormulaCell",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFormulaCell_Function(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "function"
		   });
		addAnnotation
		  (getFormulaCell_Value(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "value"
		   });
		addAnnotation
		  (derivedCubeDataEClass,
		   source,
		   new String[] {
			   "name", "DerivedCubeData",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getDerivedCubeData_Rows(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "rows"
		   });
		addAnnotation
		  (getDerivedCubeData_Cube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getDerivedCubeData_SourceCubeData(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "sourceCubeData"
		   });
		addAnnotation
		  (derivedRowDataEClass,
		   source,
		   new String[] {
			   "name", "DerivedRowData",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getDerivedRowData_FormulaCells(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "formulaCells"
		   });
		addAnnotation
		  (getDerivedRowData_SpecialDerivedCells(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "specialDerivedCells"
		   });
		addAnnotation
		  (getDerivedRowData_SourceRows(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "sourceRows"
		   });
	}

} //Cell_transformation_logicPackageImpl
