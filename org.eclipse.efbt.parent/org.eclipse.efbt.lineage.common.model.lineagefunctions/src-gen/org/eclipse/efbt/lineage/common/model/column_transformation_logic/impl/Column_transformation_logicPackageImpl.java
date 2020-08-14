/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl;

import org.eclipse.efbt.common.model.functions.FunctionsPackage;

import org.eclipse.efbt.common.model.module_management.Module_managementPackage;

import org.eclipse.efbt.data_structures.smcubes.model.core.CorePackage;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunctionGroup;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicFactory;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.StandardBasicColumnFunction;

import org.eclipse.efbt.lineage.common.model.cubes.CubesPackage;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl;

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
public class Column_transformation_logicPackageImpl extends EPackageImpl implements Column_transformation_logicPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateColumnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicColumnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass standardBasicColumnFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnFunctionGroupEClass = null;

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
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Column_transformation_logicPackageImpl()
	{
		super(eNS_URI, Column_transformation_logicFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Column_transformation_logicPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Column_transformation_logicPackage init()
	{
		if (isInited) return (Column_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredColumn_transformation_logicPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Column_transformation_logicPackageImpl theColumn_transformation_logicPackage = registeredColumn_transformation_logicPackage instanceof Column_transformation_logicPackageImpl ? (Column_transformation_logicPackageImpl)registeredColumn_transformation_logicPackage : new Column_transformation_logicPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		FunctionsPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(LineagefunctionsPackage.eNS_URI);
		LineagefunctionsPackageImpl theLineagefunctionsPackage = (LineagefunctionsPackageImpl)(registeredPackage instanceof LineagefunctionsPackageImpl ? registeredPackage : LineagefunctionsPackage.eINSTANCE);

		// Create package meta-data objects
		theColumn_transformation_logicPackage.createPackageContents();
		theLineagefunctionsPackage.createPackageContents();

		// Initialize created meta-data
		theColumn_transformation_logicPackage.initializePackageContents();
		theLineagefunctionsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theColumn_transformation_logicPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Column_transformation_logicPackage.eNS_URI, theColumn_transformation_logicPackage);
		return theColumn_transformation_logicPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAggregateColumnFunction()
	{
		return aggregateColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAggregateColumnFunction_AggregateFunction()
	{
		return (EReference)aggregateColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBasicColumnFunction()
	{
		return basicColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getColumnFunction()
	{
		return columnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getStandardBasicColumnFunction()
	{
		return standardBasicColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getStandardBasicColumnFunction_BasicFunction()
	{
		return (EReference)standardBasicColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getColumnFunctionGroup()
	{
		return columnFunctionGroupEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnFunctionGroup_ColumnFunctions()
	{
		return (EReference)columnFunctionGroupEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Column_transformation_logicFactory getColumn_transformation_logicFactory()
	{
		return (Column_transformation_logicFactory)getEFactoryInstance();
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
		aggregateColumnFunctionEClass = createEClass(AGGREGATE_COLUMN_FUNCTION);
		createEReference(aggregateColumnFunctionEClass, AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION);

		basicColumnFunctionEClass = createEClass(BASIC_COLUMN_FUNCTION);

		columnFunctionEClass = createEClass(COLUMN_FUNCTION);

		standardBasicColumnFunctionEClass = createEClass(STANDARD_BASIC_COLUMN_FUNCTION);
		createEReference(standardBasicColumnFunctionEClass, STANDARD_BASIC_COLUMN_FUNCTION__BASIC_FUNCTION);

		columnFunctionGroupEClass = createEClass(COLUMN_FUNCTION_GROUP);
		createEReference(columnFunctionGroupEClass, COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS);
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
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		LineagefunctionsPackage theLineagefunctionsPackage = (LineagefunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(LineagefunctionsPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		aggregateColumnFunctionEClass.getESuperTypes().add(this.getColumnFunction());
		basicColumnFunctionEClass.getESuperTypes().add(this.getColumnFunction());
		columnFunctionEClass.getESuperTypes().add(theLineagefunctionsPackage.getCubeColumn());
		standardBasicColumnFunctionEClass.getESuperTypes().add(this.getBasicColumnFunction());

		// Initialize classes, features, and operations; add parameters
		initEClass(aggregateColumnFunctionEClass, AggregateColumnFunction.class, "AggregateColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateColumnFunction_AggregateFunction(), theFunctionsPackage.getAggregateFunction(), null, "aggregateFunction", null, 0, 1, AggregateColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicColumnFunctionEClass, BasicColumnFunction.class, "BasicColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(columnFunctionEClass, ColumnFunction.class, "ColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(standardBasicColumnFunctionEClass, StandardBasicColumnFunction.class, "StandardBasicColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getStandardBasicColumnFunction_BasicFunction(), theFunctionsPackage.getBasicFunction(), null, "basicFunction", null, 0, 1, StandardBasicColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(columnFunctionGroupEClass, ColumnFunctionGroup.class, "ColumnFunctionGroup", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getColumnFunctionGroup_ColumnFunctions(), this.getColumnFunction(), null, "columnFunctions", null, 0, -1, ColumnFunctionGroup.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		   new String[]
		   {
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
		  (aggregateColumnFunctionEClass,
		   source,
		   new String[]
		   {
			   "name", "AggregateColumnFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateColumnFunction_AggregateFunction(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "aggregateFunction"
		   });
		addAnnotation
		  (basicColumnFunctionEClass,
		   source,
		   new String[]
		   {
			   "name", "BasicColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (columnFunctionEClass,
		   source,
		   new String[]
		   {
			   "name", "ColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (standardBasicColumnFunctionEClass,
		   source,
		   new String[]
		   {
			   "name", "StandardBasicColumnFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getStandardBasicColumnFunction_BasicFunction(),
		   source,
		   new String[]
		   {
			   "kind", "element",
			   "name", "basicFunction"
		   });
	}

} //Column_transformation_logicPackageImpl
