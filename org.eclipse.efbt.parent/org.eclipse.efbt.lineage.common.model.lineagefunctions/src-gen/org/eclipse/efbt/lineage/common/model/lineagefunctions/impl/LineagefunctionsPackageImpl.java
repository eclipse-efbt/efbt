/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions.impl;

import org.eclipse.efbt.common.model.functions.FunctionsPackage;

import org.eclipse.efbt.common.model.module_management.Module_managementPackage;

import org.eclipse.efbt.data_structures.smcubes.model.core.CorePackage;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl;

import org.eclipse.efbt.lineage.common.model.cubes.CubesPackage;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsFactory;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.MemberParameter;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter;

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
public class LineagefunctionsPackageImpl extends EPackageImpl implements LineagefunctionsPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberParameterEClass = null;

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
	private EClass cubeColumnEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass speculativeCubeColumnParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass valueParameterEClass = null;

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
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private LineagefunctionsPackageImpl()
	{
		super(eNS_URI, LineagefunctionsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link LineagefunctionsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static LineagefunctionsPackage init()
	{
		if (isInited) return (LineagefunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(LineagefunctionsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredLineagefunctionsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		LineagefunctionsPackageImpl theLineagefunctionsPackage = registeredLineagefunctionsPackage instanceof LineagefunctionsPackageImpl ? (LineagefunctionsPackageImpl)registeredLineagefunctionsPackage : new LineagefunctionsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		FunctionsPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);
		Column_transformation_logicPackageImpl theColumn_transformation_logicPackage = (Column_transformation_logicPackageImpl)(registeredPackage instanceof Column_transformation_logicPackageImpl ? registeredPackage : Column_transformation_logicPackage.eINSTANCE);

		// Create package meta-data objects
		theLineagefunctionsPackage.createPackageContents();
		theColumn_transformation_logicPackage.createPackageContents();

		// Initialize created meta-data
		theLineagefunctionsPackage.initializePackageContents();
		theColumn_transformation_logicPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theLineagefunctionsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(LineagefunctionsPackage.eNS_URI, theLineagefunctionsPackage);
		return theLineagefunctionsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMemberParameter()
	{
		return memberParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMemberParameter_Param()
	{
		return (EReference)memberParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getResolvedCubeColumnParameter()
	{
		return resolvedCubeColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getResolvedCubeColumnParameter_CubeColumn()
	{
		return (EReference)resolvedCubeColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCubeColumn()
	{
		return cubeColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCubeColumn_ColumnID()
	{
		return (EAttribute)cubeColumnEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeColumn_Cube()
	{
		return (EReference)cubeColumnEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getCubeColumn_UsedInSubsets()
	{
		return (EAttribute)cubeColumnEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCubeColumn_Variable()
	{
		return (EReference)cubeColumnEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSpeculativeCubeColumnParameter()
	{
		return speculativeCubeColumnParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSpeculativeCubeColumnParameter_Column()
	{
		return (EReference)speculativeCubeColumnParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSpeculativeCubeColumnParameter_Cube()
	{
		return (EReference)speculativeCubeColumnParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getValueParameter()
	{
		return valueParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getValueParameter_DataType()
	{
		return (EAttribute)valueParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getValueParameter_Value()
	{
		return (EAttribute)valueParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LineagefunctionsFactory getLineagefunctionsFactory()
	{
		return (LineagefunctionsFactory)getEFactoryInstance();
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
		memberParameterEClass = createEClass(MEMBER_PARAMETER);
		createEReference(memberParameterEClass, MEMBER_PARAMETER__PARAM);

		resolvedCubeColumnParameterEClass = createEClass(RESOLVED_CUBE_COLUMN_PARAMETER);
		createEReference(resolvedCubeColumnParameterEClass, RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN);

		cubeColumnEClass = createEClass(CUBE_COLUMN);
		createEAttribute(cubeColumnEClass, CUBE_COLUMN__COLUMN_ID);
		createEReference(cubeColumnEClass, CUBE_COLUMN__CUBE);
		createEAttribute(cubeColumnEClass, CUBE_COLUMN__USED_IN_SUBSETS);
		createEReference(cubeColumnEClass, CUBE_COLUMN__VARIABLE);

		speculativeCubeColumnParameterEClass = createEClass(SPECULATIVE_CUBE_COLUMN_PARAMETER);
		createEReference(speculativeCubeColumnParameterEClass, SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN);
		createEReference(speculativeCubeColumnParameterEClass, SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE);

		valueParameterEClass = createEClass(VALUE_PARAMETER);
		createEAttribute(valueParameterEClass, VALUE_PARAMETER__DATA_TYPE);
		createEAttribute(valueParameterEClass, VALUE_PARAMETER__VALUE);
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
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);
		CubesPackage theCubesPackage = (CubesPackage)EPackage.Registry.INSTANCE.getEPackage(CubesPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		memberParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());
		resolvedCubeColumnParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());
		speculativeCubeColumnParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());
		valueParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());

		// Initialize classes, features, and operations; add parameters
		initEClass(memberParameterEClass, MemberParameter.class, "MemberParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberParameter_Param(), theCorePackage.getMEMBER(), null, "param", null, 0, 1, MemberParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(resolvedCubeColumnParameterEClass, ResolvedCubeColumnParameter.class, "ResolvedCubeColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getResolvedCubeColumnParameter_CubeColumn(), this.getCubeColumn(), null, "cubeColumn", null, 0, 1, ResolvedCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cubeColumnEClass, CubeColumn.class, "CubeColumn", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCubeColumn_ColumnID(), ecorePackage.getEString(), "columnID", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeColumn_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getCubeColumn_UsedInSubsets(), ecorePackage.getEBoolean(), "usedInSubsets", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCubeColumn_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, CubeColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(speculativeCubeColumnParameterEClass, SpeculativeCubeColumnParameter.class, "SpeculativeCubeColumnParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSpeculativeCubeColumnParameter_Column(), theCorePackage.getVARIABLE(), null, "column", null, 0, 1, SpeculativeCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSpeculativeCubeColumnParameter_Cube(), theCubesPackage.getFreeBirdToolsCube(), null, "cube", null, 0, 1, SpeculativeCubeColumnParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(valueParameterEClass, ValueParameter.class, "ValueParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getValueParameter_DataType(), theCorePackage.getFACET_VALUE_TYPE(), "dataType", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getValueParameter_Value(), ecorePackage.getEString(), "value", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
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
		  (memberParameterEClass,
		   source,
		   new String[]
		   {
			   "name", "MemberParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMemberParameter_Param(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "param"
		   });
		addAnnotation
		  (resolvedCubeColumnParameterEClass,
		   source,
		   new String[]
		   {
			   "name", "ResolvedCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getResolvedCubeColumnParameter_CubeColumn(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "cubeColumn"
		   });
		addAnnotation
		  (cubeColumnEClass,
		   source,
		   new String[]
		   {
			   "name", "CubeColumn",
			   "kind", "empty"
		   });
		addAnnotation
		  (getCubeColumn_ColumnID(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "columnID"
		   });
		addAnnotation
		  (getCubeColumn_Cube(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getCubeColumn_UsedInSubsets(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "usedInSubsets"
		   });
		addAnnotation
		  (getCubeColumn_Variable(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "variable"
		   });
		addAnnotation
		  (speculativeCubeColumnParameterEClass,
		   source,
		   new String[]
		   {
			   "name", "SpeculativeCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getSpeculativeCubeColumnParameter_Column(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "column"
		   });
		addAnnotation
		  (getSpeculativeCubeColumnParameter_Cube(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (valueParameterEClass,
		   source,
		   new String[]
		   {
			   "name", "ValueParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getValueParameter_DataType(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "dataType"
		   });
		addAnnotation
		  (getValueParameter_Value(),
		   source,
		   new String[]
		   {
			   "kind", "attribute",
			   "name", "value"
		   });
	}

} //LineagefunctionsPackageImpl
