/**
 */
package cube_transformation_logic.impl;

import advanced_row_transformation_logic.Advanced_row_transformation_logicPackage;

import advanced_row_transformation_logic.impl.Advanced_row_transformation_logicPackageImpl;

import attribute_lineage.Attribute_lineagePackage;

import attribute_lineage.impl.Attribute_lineagePackageImpl;

import column_transformation_logic.Column_transformation_logicPackage;

import cube_transformation_logic.CubeTransformationLogic;
import cube_transformation_logic.Cube_transformation_logicFactory;
import cube_transformation_logic.Cube_transformation_logicPackage;

import entities.EntitiesPackage;

import functions.FunctionsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

import row_transformation_logic.Row_transformation_logicPackage;

import row_transformation_logic.impl.Row_transformation_logicPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Cube_transformation_logicPackageImpl extends EPackageImpl implements Cube_transformation_logicPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cubeTransformationLogicEClass = null;

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
	 * @see cube_transformation_logic.Cube_transformation_logicPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Cube_transformation_logicPackageImpl() {
		super(eNS_URI, Cube_transformation_logicFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Cube_transformation_logicPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Cube_transformation_logicPackage init() {
		if (isInited) return (Cube_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Cube_transformation_logicPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredCube_transformation_logicPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Cube_transformation_logicPackageImpl theCube_transformation_logicPackage = registeredCube_transformation_logicPackage instanceof Cube_transformation_logicPackageImpl ? (Cube_transformation_logicPackageImpl)registeredCube_transformation_logicPackage : new Cube_transformation_logicPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		EntitiesPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Column_transformation_logicPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Advanced_row_transformation_logicPackage.eNS_URI);
		Advanced_row_transformation_logicPackageImpl theAdvanced_row_transformation_logicPackage = (Advanced_row_transformation_logicPackageImpl)(registeredPackage instanceof Advanced_row_transformation_logicPackageImpl ? registeredPackage : Advanced_row_transformation_logicPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Attribute_lineagePackage.eNS_URI);
		Attribute_lineagePackageImpl theAttribute_lineagePackage = (Attribute_lineagePackageImpl)(registeredPackage instanceof Attribute_lineagePackageImpl ? registeredPackage : Attribute_lineagePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Row_transformation_logicPackage.eNS_URI);
		Row_transformation_logicPackageImpl theRow_transformation_logicPackage = (Row_transformation_logicPackageImpl)(registeredPackage instanceof Row_transformation_logicPackageImpl ? registeredPackage : Row_transformation_logicPackage.eINSTANCE);

		// Create package meta-data objects
		theCube_transformation_logicPackage.createPackageContents();
		theAdvanced_row_transformation_logicPackage.createPackageContents();
		theAttribute_lineagePackage.createPackageContents();
		theRow_transformation_logicPackage.createPackageContents();

		// Initialize created meta-data
		theCube_transformation_logicPackage.initializePackageContents();
		theAdvanced_row_transformation_logicPackage.initializePackageContents();
		theAttribute_lineagePackage.initializePackageContents();
		theRow_transformation_logicPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theCube_transformation_logicPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Cube_transformation_logicPackage.eNS_URI, theCube_transformation_logicPackage);
		return theCube_transformation_logicPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCubeTransformationLogic() {
		return cubeTransformationLogicEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCubeTransformationLogic_ColumnFunctionGroup() {
		return (EReference)cubeTransformationLogicEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCubeTransformationLogic_RowCreationApproachForentity() {
		return (EReference)cubeTransformationLogicEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getCubeTransformationLogic_UsedInSubsets() {
		return (EAttribute)cubeTransformationLogicEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cube_transformation_logicFactory getCube_transformation_logicFactory() {
		return (Cube_transformation_logicFactory)getEFactoryInstance();
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
		cubeTransformationLogicEClass = createEClass(CUBE_TRANSFORMATION_LOGIC);
		createEReference(cubeTransformationLogicEClass, CUBE_TRANSFORMATION_LOGIC__COLUMN_FUNCTION_GROUP);
		createEReference(cubeTransformationLogicEClass, CUBE_TRANSFORMATION_LOGIC__ROW_CREATION_APPROACH_FORENTITY);
		createEAttribute(cubeTransformationLogicEClass, CUBE_TRANSFORMATION_LOGIC__USED_IN_SUBSETS);
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
		Column_transformation_logicPackage theColumn_transformation_logicPackage = (Column_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);
		Row_transformation_logicPackage theRow_transformation_logicPackage = (Row_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Row_transformation_logicPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes

		// Initialize classes, features, and operations; add parameters
		initEClass(cubeTransformationLogicEClass, CubeTransformationLogic.class, "CubeTransformationLogic", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCubeTransformationLogic_ColumnFunctionGroup(), theColumn_transformation_logicPackage.getColumnFunctionGroup(), null, "columnFunctionGroup", null, 0, 1, CubeTransformationLogic.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeTransformationLogic_RowCreationApproachForentity(), theRow_transformation_logicPackage.getRowCreationApproachForEntity(), null, "rowCreationApproachForentity", null, 0, 1, CubeTransformationLogic.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCubeTransformationLogic_UsedInSubsets(), ecorePackage.getEBoolean(), "usedInSubsets", null, 0, 1, CubeTransformationLogic.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
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

} //Cube_transformation_logicPackageImpl
