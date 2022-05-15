/**
 */
package org.eclipse.efbt.openregspecs.model.sql_lite.impl;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.Bpmn_litePackageImpl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.efbt.openregspecs.model.module_management.impl.Module_managementPackageImpl;

import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.openregspecs.model.requirements_text.impl.Requirements_textPackageImpl;

import org.eclipse.efbt.openregspecs.model.sql_lite.Column;
import org.eclipse.efbt.openregspecs.model.sql_lite.FromClause;
import org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL;
import org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQLModule;
import org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntity;
import org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule;
import org.eclipse.efbt.openregspecs.model.sql_lite.SelectClause;
import org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn;
import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_liteFactory;
import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage;
import org.eclipse.efbt.openregspecs.model.sql_lite.Table;
import org.eclipse.efbt.openregspecs.model.sql_lite.View;
import org.eclipse.efbt.openregspecs.model.sql_lite.WhereClause;

import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;

import org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl;

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
public class Sql_litePackageImpl extends EPackageImpl implements Sql_litePackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass viewEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectClauseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectColumnEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass whereClauseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass sqlEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass fromClauseEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass sqlEntityModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass generatedEntitySQLEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass generatedEntitySQLModuleEClass = null;

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
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Sql_litePackageImpl() {
		super(eNS_URI, Sql_liteFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Sql_litePackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Sql_litePackage init() {
		if (isInited) return (Sql_litePackage)EPackage.Registry.INSTANCE.getEPackage(Sql_litePackage.eNS_URI);

		// Obtain or create and register package
		Object registeredSql_litePackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Sql_litePackageImpl theSql_litePackage = registeredSql_litePackage instanceof Sql_litePackageImpl ? (Sql_litePackageImpl)registeredSql_litePackage : new Sql_litePackageImpl();

		isInited = true;

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Bpmn_litePackageImpl theBpmn_litePackage = (Bpmn_litePackageImpl)(registeredPackage instanceof Bpmn_litePackageImpl ? registeredPackage : Bpmn_litePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Data_meta_modelPackageImpl theData_meta_modelPackage = (Data_meta_modelPackageImpl)(registeredPackage instanceof Data_meta_modelPackageImpl ? registeredPackage : Data_meta_modelPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Module_managementPackageImpl theModule_managementPackage = (Module_managementPackageImpl)(registeredPackage instanceof Module_managementPackageImpl ? registeredPackage : Module_managementPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		Requirements_textPackageImpl theRequirements_textPackage = (Requirements_textPackageImpl)(registeredPackage instanceof Requirements_textPackageImpl ? registeredPackage : Requirements_textPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TestingPackage.eNS_URI);
		TestingPackageImpl theTestingPackage = (TestingPackageImpl)(registeredPackage instanceof TestingPackageImpl ? registeredPackage : TestingPackage.eINSTANCE);

		// Create package meta-data objects
		theSql_litePackage.createPackageContents();
		theBpmn_litePackage.createPackageContents();
		theData_meta_modelPackage.createPackageContents();
		theModule_managementPackage.createPackageContents();
		theRequirements_textPackage.createPackageContents();
		theTestingPackage.createPackageContents();

		// Initialize created meta-data
		theSql_litePackage.initializePackageContents();
		theBpmn_litePackage.initializePackageContents();
		theData_meta_modelPackage.initializePackageContents();
		theModule_managementPackage.initializePackageContents();
		theRequirements_textPackage.initializePackageContents();
		theTestingPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theSql_litePackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Sql_litePackage.eNS_URI, theSql_litePackage);
		return theSql_litePackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getView() {
		return viewEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getView_SelectClause() {
		return (EReference)viewEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getView_FromClause() {
		return (EReference)viewEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getView_WhereClause() {
		return (EReference)viewEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectClause() {
		return selectClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectClause_Columns() {
		return (EReference)selectClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectColumn() {
		return selectColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectColumn_As() {
		return (EReference)selectColumnEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectColumn_MemberAsConstant() {
		return (EReference)selectColumnEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getColumn() {
		return columnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getColumn_Attribute() {
		return (EReference)columnEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getWhereClause() {
		return whereClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTable() {
		return tableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTable_Entity() {
		return (EReference)tableEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSQLEntity() {
		return sqlEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getSQLEntity_Name() {
		return (EAttribute)sqlEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFromClause() {
		return fromClauseEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFromClause_SqlEntities() {
		return (EReference)fromClauseEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSQLEntityModule() {
		return sqlEntityModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSQLEntityModule_SqlEntities() {
		return (EReference)sqlEntityModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGeneratedEntitySQL() {
		return generatedEntitySQLEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGeneratedEntitySQL_GeneratedEntity() {
		return (EReference)generatedEntitySQLEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGeneratedEntitySQL_View() {
		return (EReference)generatedEntitySQLEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGeneratedEntitySQLModule() {
		return generatedEntitySQLModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGeneratedEntitySQLModule_GeneratedEntitySQLs() {
		return (EReference)generatedEntitySQLModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Sql_liteFactory getSql_liteFactory() {
		return (Sql_liteFactory)getEFactoryInstance();
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
		viewEClass = createEClass(VIEW);
		createEReference(viewEClass, VIEW__SELECT_CLAUSE);
		createEReference(viewEClass, VIEW__FROM_CLAUSE);
		createEReference(viewEClass, VIEW__WHERE_CLAUSE);

		selectClauseEClass = createEClass(SELECT_CLAUSE);
		createEReference(selectClauseEClass, SELECT_CLAUSE__COLUMNS);

		selectColumnEClass = createEClass(SELECT_COLUMN);
		createEReference(selectColumnEClass, SELECT_COLUMN__AS);
		createEReference(selectColumnEClass, SELECT_COLUMN__MEMBER_AS_CONSTANT);

		columnEClass = createEClass(COLUMN);
		createEReference(columnEClass, COLUMN__ATTRIBUTE);

		whereClauseEClass = createEClass(WHERE_CLAUSE);

		tableEClass = createEClass(TABLE);
		createEReference(tableEClass, TABLE__ENTITY);

		sqlEntityEClass = createEClass(SQL_ENTITY);
		createEAttribute(sqlEntityEClass, SQL_ENTITY__NAME);

		fromClauseEClass = createEClass(FROM_CLAUSE);
		createEReference(fromClauseEClass, FROM_CLAUSE__SQL_ENTITIES);

		sqlEntityModuleEClass = createEClass(SQL_ENTITY_MODULE);
		createEReference(sqlEntityModuleEClass, SQL_ENTITY_MODULE__SQL_ENTITIES);

		generatedEntitySQLEClass = createEClass(GENERATED_ENTITY_SQL);
		createEReference(generatedEntitySQLEClass, GENERATED_ENTITY_SQL__GENERATED_ENTITY);
		createEReference(generatedEntitySQLEClass, GENERATED_ENTITY_SQL__VIEW);

		generatedEntitySQLModuleEClass = createEClass(GENERATED_ENTITY_SQL_MODULE);
		createEReference(generatedEntitySQLModuleEClass, GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS);
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
		Data_meta_modelPackage theData_meta_modelPackage = (Data_meta_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		viewEClass.getESuperTypes().add(this.getSQLEntity());
		selectColumnEClass.getESuperTypes().add(this.getColumn());
		tableEClass.getESuperTypes().add(this.getSQLEntity());
		sqlEntityModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		generatedEntitySQLModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(viewEClass, View.class, "View", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getView_SelectClause(), this.getSelectClause(), null, "selectClause", null, 0, 1, View.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getView_FromClause(), this.getFromClause(), null, "fromClause", null, 0, 1, View.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getView_WhereClause(), this.getWhereClause(), null, "whereClause", null, 0, 1, View.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(selectClauseEClass, SelectClause.class, "SelectClause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSelectClause_Columns(), this.getSelectColumn(), null, "columns", null, 0, -1, SelectClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(selectColumnEClass, SelectColumn.class, "SelectColumn", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSelectColumn_As(), theData_meta_modelPackage.getAttribute(), null, "as", null, 0, 1, SelectColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSelectColumn_MemberAsConstant(), theData_meta_modelPackage.getEnumMember(), null, "memberAsConstant", null, 0, 1, SelectColumn.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(columnEClass, Column.class, "Column", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getColumn_Attribute(), theData_meta_modelPackage.getAttribute(), null, "attribute", null, 0, 1, Column.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(whereClauseEClass, WhereClause.class, "WhereClause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(tableEClass, Table.class, "Table", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTable_Entity(), theData_meta_modelPackage.getEntity(), null, "entity", null, 0, 1, Table.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(sqlEntityEClass, SQLEntity.class, "SQLEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getSQLEntity_Name(), ecorePackage.getEString(), "name", null, 0, 1, SQLEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(fromClauseEClass, FromClause.class, "FromClause", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFromClause_SqlEntities(), this.getSQLEntity(), null, "sqlEntities", null, 0, 1, FromClause.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(sqlEntityModuleEClass, SQLEntityModule.class, "SQLEntityModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getSQLEntityModule_SqlEntities(), this.getSQLEntity(), null, "sqlEntities", null, 0, -1, SQLEntityModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(generatedEntitySQLEClass, GeneratedEntitySQL.class, "GeneratedEntitySQL", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGeneratedEntitySQL_GeneratedEntity(), theData_meta_modelPackage.getGeneratedEntity(), null, "generatedEntity", null, 0, 1, GeneratedEntitySQL.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGeneratedEntitySQL_View(), this.getView(), null, "view", null, 0, -1, GeneratedEntitySQL.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(generatedEntitySQLModuleEClass, GeneratedEntitySQLModule.class, "GeneratedEntitySQLModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGeneratedEntitySQLModule_GeneratedEntitySQLs(), this.getGeneratedEntitySQL(), null, "GeneratedEntitySQLs", null, 0, -1, GeneratedEntitySQLModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);
	}

} //Sql_litePackageImpl
