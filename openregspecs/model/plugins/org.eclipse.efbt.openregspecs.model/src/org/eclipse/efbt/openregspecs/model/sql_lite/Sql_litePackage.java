/**
 */
package org.eclipse.efbt.openregspecs.model.sql_lite;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Sql_liteFactory
 * @model kind="package"
 * @generated
 */
public interface Sql_litePackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "sql_lite";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.example.org/sql_lite";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "sql_lite";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Sql_litePackage eINSTANCE = org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityImpl <em>SQL Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSQLEntity()
	 * @generated
	 */
	int SQL_ENTITY = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY__NAME = 0;

	/**
	 * The number of structural features of the '<em>SQL Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>SQL Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.ViewImpl <em>View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.ViewImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getView()
	 * @generated
	 */
	int VIEW = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VIEW__NAME = SQL_ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Select Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VIEW__SELECT_CLAUSE = SQL_ENTITY_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>From Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VIEW__FROM_CLAUSE = SQL_ENTITY_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VIEW__WHERE_CLAUSE = SQL_ENTITY_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VIEW_FEATURE_COUNT = SQL_ENTITY_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VIEW_OPERATION_COUNT = SQL_ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectClauseImpl <em>Select Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectClauseImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSelectClause()
	 * @generated
	 */
	int SELECT_CLAUSE = 1;

	/**
	 * The feature id for the '<em><b>Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE__COLUMNS = 0;

	/**
	 * The number of structural features of the '<em>Select Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Select Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.ColumnImpl <em>Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.ColumnImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getColumn()
	 * @generated
	 */
	int COLUMN = 3;

	/**
	 * The feature id for the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN__ATTRIBUTE = 0;

	/**
	 * The number of structural features of the '<em>Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectColumnImpl <em>Select Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectColumnImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSelectColumn()
	 * @generated
	 */
	int SELECT_COLUMN = 2;

	/**
	 * The feature id for the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN__ATTRIBUTE = COLUMN__ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>As</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN__AS = COLUMN_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Member As Constant</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN__MEMBER_AS_CONSTANT = COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Select Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_FEATURE_COUNT = COLUMN_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Select Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_OPERATION_COUNT = COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.WhereClauseImpl <em>Where Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.WhereClauseImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getWhereClause()
	 * @generated
	 */
	int WHERE_CLAUSE = 4;

	/**
	 * The number of structural features of the '<em>Where Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHERE_CLAUSE_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Where Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHERE_CLAUSE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.TableImpl <em>Table</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.TableImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getTable()
	 * @generated
	 */
	int TABLE = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__NAME = SQL_ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__ENTITY = SQL_ENTITY_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FEATURE_COUNT = SQL_ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_OPERATION_COUNT = SQL_ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.FromClauseImpl <em>From Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.FromClauseImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getFromClause()
	 * @generated
	 */
	int FROM_CLAUSE = 7;

	/**
	 * The feature id for the '<em><b>Sql Entities</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FROM_CLAUSE__SQL_ENTITIES = 0;

	/**
	 * The number of structural features of the '<em>From Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FROM_CLAUSE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>From Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FROM_CLAUSE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityModuleImpl <em>SQL Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSQLEntityModule()
	 * @generated
	 */
	int SQL_ENTITY_MODULE = 8;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Sql Entities</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE__SQL_ENTITIES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>SQL Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>SQL Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_ENTITY_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLImpl <em>Generated Entity SQL</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getGeneratedEntitySQL()
	 * @generated
	 */
	int GENERATED_ENTITY_SQL = 9;

	/**
	 * The feature id for the '<em><b>Generated Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL__GENERATED_ENTITY = 0;

	/**
	 * The feature id for the '<em><b>View</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL__VIEW = 1;

	/**
	 * The number of structural features of the '<em>Generated Entity SQL</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Generated Entity SQL</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLModuleImpl <em>Generated Entity SQL Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getGeneratedEntitySQLModule()
	 * @generated
	 */
	int GENERATED_ENTITY_SQL_MODULE = 10;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Generated Entity SQ Ls</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Generated Entity SQL Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Generated Entity SQL Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_SQL_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.View <em>View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>View</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.View
	 * @generated
	 */
	EClass getView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.View#getSelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Select Clause</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.View#getSelectClause()
	 * @see #getView()
	 * @generated
	 */
	EReference getView_SelectClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.View#getFromClause <em>From Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>From Clause</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.View#getFromClause()
	 * @see #getView()
	 * @generated
	 */
	EReference getView_FromClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.View#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.View#getWhereClause()
	 * @see #getView()
	 * @generated
	 */
	EReference getView_WhereClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Clause</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SelectClause
	 * @generated
	 */
	EClass getSelectClause();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SelectClause#getColumns <em>Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Columns</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SelectClause#getColumns()
	 * @see #getSelectClause()
	 * @generated
	 */
	EReference getSelectClause_Columns();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn <em>Select Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Column</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn
	 * @generated
	 */
	EClass getSelectColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn#getAs <em>As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>As</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn#getAs()
	 * @see #getSelectColumn()
	 * @generated
	 */
	EReference getSelectColumn_As();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn#getMemberAsConstant <em>Member As Constant</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member As Constant</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SelectColumn#getMemberAsConstant()
	 * @see #getSelectColumn()
	 * @generated
	 */
	EReference getSelectColumn_MemberAsConstant();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.Column <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Column
	 * @generated
	 */
	EClass getColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.Column#getAttribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Column#getAttribute()
	 * @see #getColumn()
	 * @generated
	 */
	EReference getColumn_Attribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.WhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.WhereClause
	 * @generated
	 */
	EClass getWhereClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.Table <em>Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Table</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Table
	 * @generated
	 */
	EClass getTable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.Table#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.Table#getEntity()
	 * @see #getTable()
	 * @generated
	 */
	EReference getTable_Entity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntity <em>SQL Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SQL Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntity
	 * @generated
	 */
	EClass getSQLEntity();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntity#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntity#getName()
	 * @see #getSQLEntity()
	 * @generated
	 */
	EAttribute getSQLEntity_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.FromClause <em>From Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>From Clause</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.FromClause
	 * @generated
	 */
	EClass getFromClause();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.FromClause#getSqlEntities <em>Sql Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Sql Entities</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.FromClause#getSqlEntities()
	 * @see #getFromClause()
	 * @generated
	 */
	EReference getFromClause_SqlEntities();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule <em>SQL Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SQL Entity Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule
	 * @generated
	 */
	EClass getSQLEntityModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule#getSqlEntities <em>Sql Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sql Entities</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule#getSqlEntities()
	 * @see #getSQLEntityModule()
	 * @generated
	 */
	EReference getSQLEntityModule_SqlEntities();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL <em>Generated Entity SQL</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generated Entity SQL</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL
	 * @generated
	 */
	EClass getGeneratedEntitySQL();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL#getGeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Generated Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL#getGeneratedEntity()
	 * @see #getGeneratedEntitySQL()
	 * @generated
	 */
	EReference getGeneratedEntitySQL_GeneratedEntity();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL#getView <em>View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>View</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQL#getView()
	 * @see #getGeneratedEntitySQL()
	 * @generated
	 */
	EReference getGeneratedEntitySQL_View();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQLModule <em>Generated Entity SQL Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generated Entity SQL Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQLModule
	 * @generated
	 */
	EClass getGeneratedEntitySQLModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQLModule#getGeneratedEntitySQLs <em>Generated Entity SQ Ls</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Generated Entity SQ Ls</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.sql_lite.GeneratedEntitySQLModule#getGeneratedEntitySQLs()
	 * @see #getGeneratedEntitySQLModule()
	 * @generated
	 */
	EReference getGeneratedEntitySQLModule_GeneratedEntitySQLs();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Sql_liteFactory getSql_liteFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals {
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.ViewImpl <em>View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.ViewImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getView()
		 * @generated
		 */
		EClass VIEW = eINSTANCE.getView();

		/**
		 * The meta object literal for the '<em><b>Select Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VIEW__SELECT_CLAUSE = eINSTANCE.getView_SelectClause();

		/**
		 * The meta object literal for the '<em><b>From Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VIEW__FROM_CLAUSE = eINSTANCE.getView_FromClause();

		/**
		 * The meta object literal for the '<em><b>Where Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VIEW__WHERE_CLAUSE = eINSTANCE.getView_WhereClause();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectClauseImpl <em>Select Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectClauseImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSelectClause()
		 * @generated
		 */
		EClass SELECT_CLAUSE = eINSTANCE.getSelectClause();

		/**
		 * The meta object literal for the '<em><b>Columns</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SELECT_CLAUSE__COLUMNS = eINSTANCE.getSelectClause_Columns();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectColumnImpl <em>Select Column</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SelectColumnImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSelectColumn()
		 * @generated
		 */
		EClass SELECT_COLUMN = eINSTANCE.getSelectColumn();

		/**
		 * The meta object literal for the '<em><b>As</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SELECT_COLUMN__AS = eINSTANCE.getSelectColumn_As();

		/**
		 * The meta object literal for the '<em><b>Member As Constant</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SELECT_COLUMN__MEMBER_AS_CONSTANT = eINSTANCE.getSelectColumn_MemberAsConstant();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.ColumnImpl <em>Column</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.ColumnImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getColumn()
		 * @generated
		 */
		EClass COLUMN = eINSTANCE.getColumn();

		/**
		 * The meta object literal for the '<em><b>Attribute</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COLUMN__ATTRIBUTE = eINSTANCE.getColumn_Attribute();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.WhereClauseImpl <em>Where Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.WhereClauseImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getWhereClause()
		 * @generated
		 */
		EClass WHERE_CLAUSE = eINSTANCE.getWhereClause();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.TableImpl <em>Table</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.TableImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getTable()
		 * @generated
		 */
		EClass TABLE = eINSTANCE.getTable();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TABLE__ENTITY = eINSTANCE.getTable_Entity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityImpl <em>SQL Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSQLEntity()
		 * @generated
		 */
		EClass SQL_ENTITY = eINSTANCE.getSQLEntity();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SQL_ENTITY__NAME = eINSTANCE.getSQLEntity_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.FromClauseImpl <em>From Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.FromClauseImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getFromClause()
		 * @generated
		 */
		EClass FROM_CLAUSE = eINSTANCE.getFromClause();

		/**
		 * The meta object literal for the '<em><b>Sql Entities</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FROM_CLAUSE__SQL_ENTITIES = eINSTANCE.getFromClause_SqlEntities();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityModuleImpl <em>SQL Entity Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.SQLEntityModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getSQLEntityModule()
		 * @generated
		 */
		EClass SQL_ENTITY_MODULE = eINSTANCE.getSQLEntityModule();

		/**
		 * The meta object literal for the '<em><b>Sql Entities</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SQL_ENTITY_MODULE__SQL_ENTITIES = eINSTANCE.getSQLEntityModule_SqlEntities();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLImpl <em>Generated Entity SQL</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getGeneratedEntitySQL()
		 * @generated
		 */
		EClass GENERATED_ENTITY_SQL = eINSTANCE.getGeneratedEntitySQL();

		/**
		 * The meta object literal for the '<em><b>Generated Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERATED_ENTITY_SQL__GENERATED_ENTITY = eINSTANCE.getGeneratedEntitySQL_GeneratedEntity();

		/**
		 * The meta object literal for the '<em><b>View</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERATED_ENTITY_SQL__VIEW = eINSTANCE.getGeneratedEntitySQL_View();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLModuleImpl <em>Generated Entity SQL Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.GeneratedEntitySQLModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl#getGeneratedEntitySQLModule()
		 * @generated
		 */
		EClass GENERATED_ENTITY_SQL_MODULE = eINSTANCE.getGeneratedEntitySQLModule();

		/**
		 * The meta object literal for the '<em><b>Generated Entity SQ Ls</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERATED_ENTITY_SQL_MODULE__GENERATED_ENTITY_SQ_LS = eINSTANCE.getGeneratedEntitySQLModule_GeneratedEntitySQLs();

	}

} //Sql_litePackage
