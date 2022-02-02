/**
 */
package column_transformation_logic.impl;

import advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;

import advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl;

import column_transformation_logic.AggregateColumnFunction;
import column_transformation_logic.BasicColumnFunction;
import column_transformation_logic.ColumnFunction;
import column_transformation_logic.ColumnFunctionGroup;
import column_transformation_logic.Column_transformation_logicFactory;
import column_transformation_logic.Column_transformation_logicPackage;
import column_transformation_logic.ResolvedCubeColumnParameter;
import column_transformation_logic.SpeculativeCubeColumnParameter;

import data_lineage_functions.Data_lineage_functionsPackage;

import data_lineage_functions.impl.Data_lineage_functionsPackageImpl;

import entities.EntitiesPackage;

import entity_functions.Entity_functionsPackage;

import entity_functions.impl.Entity_functionsPackageImpl;

import functions.FunctionsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Column_transformation_logicPackageImpl extends EPackageImpl implements Column_transformation_logicPackage {
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
	private EClass columnFunctionGroupEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass resolvedCubeColumnParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass speculativeCubeColumnParameterEClass = null;

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
	 * @see column_transformation_logic.Column_transformation_logicPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Column_transformation_logicPackageImpl() {
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
	public static Column_transformation_logicPackage init() {
		if (isInited) return (Column_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredColumn_transformation_logicPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Column_transformation_logicPackageImpl theColumn_transformation_logicPackage = registeredColumn_transformation_logicPackage instanceof Column_transformation_logicPackageImpl ? (Column_transformation_logicPackageImpl)registeredColumn_transformation_logicPackage : new Column_transformation_logicPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		EntitiesPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Advanced_variable_lineagefunctionsPackage.eNS_URI);
		Advanced_variable_lineagefunctionsPackageImpl theAdvanced_variable_lineagefunctionsPackage = (Advanced_variable_lineagefunctionsPackageImpl)(registeredPackage instanceof Advanced_variable_lineagefunctionsPackageImpl ? registeredPackage : Advanced_variable_lineagefunctionsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_lineage_functionsPackage.eNS_URI);
		Data_lineage_functionsPackageImpl theData_lineage_functionsPackage = (Data_lineage_functionsPackageImpl)(registeredPackage instanceof Data_lineage_functionsPackageImpl ? registeredPackage : Data_lineage_functionsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Entity_functionsPackage.eNS_URI);
		Entity_functionsPackageImpl theEntity_functionsPackage = (Entity_functionsPackageImpl)(registeredPackage instanceof Entity_functionsPackageImpl ? registeredPackage : Entity_functionsPackage.eINSTANCE);

		// Create package meta-data objects
		theColumn_transformation_logicPackage.createPackageContents();
		theAdvanced_variable_lineagefunctionsPackage.createPackageContents();
		theData_lineage_functionsPackage.createPackageContents();
		theEntity_functionsPackage.createPackageContents();

		// Initialize created meta-data
		theColumn_transformation_logicPackage.initializePackageContents();
		theAdvanced_variable_lineagefunctionsPackage.initializePackageContents();
		theData_lineage_functionsPackage.initializePackageContents();
		theEntity_functionsPackage.initializePackageContents();

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
	public EClass getAggregateColumnFunction() {
		return aggregateColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAggregateColumnFunction_AggregateFunction() {
		return (EReference)aggregateColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicColumnFunction() {
		return basicColumnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicColumnFunction_BasicFunction() {
		return (EReference)basicColumnFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getColumnFunction() {
		return columnFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getColumnFunctionGroup() {
		return columnFunctionGroupEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getColumnFunctionGroup_ColumnFunctions() {
		return (EReference)columnFunctionGroupEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getResolvedCubeColumnParameter() {
		return resolvedCubeColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getResolvedCubeColumnParameter_CubeColumn() {
		return (EReference)resolvedCubeColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSpeculativeCubeColumnParameter() {
		return speculativeCubeColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeCubeColumnParameter_Cube() {
		return (EReference)speculativeCubeColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSpeculativeCubeColumnParameter_Attribute() {
		return (EReference)speculativeCubeColumnParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Column_transformation_logicFactory getColumn_transformation_logicFactory() {
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
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		aggregateColumnFunctionEClass = createEClass(AGGREGATE_COLUMN_FUNCTION);
		createEReference(aggregateColumnFunctionEClass, AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION);

		basicColumnFunctionEClass = createEClass(BASIC_COLUMN_FUNCTION);
		createEReference(basicColumnFunctionEClass, BASIC_COLUMN_FUNCTION__BASIC_FUNCTION);

		columnFunctionEClass = createEClass(COLUMN_FUNCTION);

		columnFunctionGroupEClass = createEClass(COLUMN_FUNCTION_GROUP);
		createEReference(columnFunctionGroupEClass, COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS);

		resolvedCubeColumnParameterEClass = createEClass(RESOLVED_CUBE_COLUMN_PARAMETER);
		createEReference(resolvedCubeColumnParameterEClass, RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN);

		speculativeCubeColumnParameterEClass = createEClass(SPECULATIVE_CUBE_COLUMN_PARAMETER);
		createEReference(speculativeCubeColumnParameterEClass, SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE);
		createEReference(speculativeCubeColumnParameterEClass, SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE);
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
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		aggregateColumnFunctionEClass.getESuperTypes().add(this.getColumnFunction());
		basicColumnFunctionEClass.getESuperTypes().add(this.getColumnFunction());
		columnFunctionEClass.getESuperTypes().add(theEcorePackage.getEStructuralFeature());
		resolvedCubeColumnParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());
		speculativeCubeColumnParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());

		// Initialize classes, features, and operations; add parameters
		initEClass(aggregateColumnFunctionEClass, AggregateColumnFunction.class, "AggregateColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateColumnFunction_AggregateFunction(), theFunctionsPackage.getAggregateFunction(), null, "aggregateFunction", null, 0, 1, AggregateColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicColumnFunctionEClass, BasicColumnFunction.class, "BasicColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicColumnFunction_BasicFunction(), theFunctionsPackage.getBasicFunction(), null, "basicFunction", null, 0, 1, BasicColumnFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(columnFunctionEClass, ColumnFunction.class, "ColumnFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(columnFunctionGroupEClass, ColumnFunctionGroup.class, "ColumnFunctionGroup", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getColumnFunctionGroup_ColumnFunctions(), this.getColumnFunction(), null, "columnFunctions", null, 0, -1, ColumnFunctionGroup.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(resolvedCubeColumnParameterEClass, ResolvedCubeColumnParameter.class, "ResolvedCubeColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getResolvedCubeColumnParameter_CubeColumn(), theEcorePackage.getEStructuralFeature(), null, "cubeColumn", null, 0, 1, ResolvedCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(speculativeCubeColumnParameterEClass, SpeculativeCubeColumnParameter.class, "SpeculativeCubeColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSpeculativeCubeColumnParameter_Cube(), theEcorePackage.getEClass(), null, "cube", null, 0, 1, SpeculativeCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSpeculativeCubeColumnParameter_Attribute(), theEcorePackage.getEAttribute(), null, "attribute", null, 0, 1, SpeculativeCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
	protected void createLicenseAnnotations() {
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
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (aggregateColumnFunctionEClass,
		   source,
		   new String[] {
			   "name", "AggregateColumnFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateColumnFunction_AggregateFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "aggregateFunction"
		   });
		addAnnotation
		  (basicColumnFunctionEClass,
		   source,
		   new String[] {
			   "name", "BasicColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (getBasicColumnFunction_BasicFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "basicFunction"
		   });
		addAnnotation
		  (columnFunctionEClass,
		   source,
		   new String[] {
			   "name", "ColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (resolvedCubeColumnParameterEClass,
		   source,
		   new String[] {
			   "name", "ResolvedCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getResolvedCubeColumnParameter_CubeColumn(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cubeColumn"
		   });
		addAnnotation
		  (speculativeCubeColumnParameterEClass,
		   source,
		   new String[] {
			   "name", "SpeculativeCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getSpeculativeCubeColumnParameter_Cube(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
	}

} //Column_transformation_logicPackageImpl
