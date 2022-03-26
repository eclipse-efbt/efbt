/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.*;

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
public class Sql_liteFactoryImpl extends EFactoryImpl implements Sql_liteFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Sql_liteFactory init() {
		try {
			Sql_liteFactory theSql_liteFactory = (Sql_liteFactory)EPackage.Registry.INSTANCE.getEFactory(Sql_litePackage.eNS_URI);
			if (theSql_liteFactory != null) {
				return theSql_liteFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Sql_liteFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Sql_liteFactoryImpl() {
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
			case Sql_litePackage.VIEW: return createView();
			case Sql_litePackage.SELECT_CLAUSE: return createSelectClause();
			case Sql_litePackage.SELECT_COLUMN: return createSelectColumn();
			case Sql_litePackage.COLUMN: return createColumn();
			case Sql_litePackage.WHERE_CLAUSE: return createWhereClause();
			case Sql_litePackage.TABLE: return createTable();
			case Sql_litePackage.SQL_ENTITY: return createSQLEntity();
			case Sql_litePackage.FROM_CLAUSE: return createFromClause();
			case Sql_litePackage.SQL_ENTITY_MODULE: return createSQLEntityModule();
			case Sql_litePackage.GENERATED_ENTITY_SQL: return createGeneratedEntitySQL();
			case Sql_litePackage.GENERATED_ENTITY_SQL_MODULE: return createGeneratedEntitySQLModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public View createView() {
		ViewImpl view = new ViewImpl();
		return view;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectClause createSelectClause() {
		SelectClauseImpl selectClause = new SelectClauseImpl();
		return selectClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectColumn createSelectColumn() {
		SelectColumnImpl selectColumn = new SelectColumnImpl();
		return selectColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Column createColumn() {
		ColumnImpl column = new ColumnImpl();
		return column;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public WhereClause createWhereClause() {
		WhereClauseImpl whereClause = new WhereClauseImpl();
		return whereClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Table createTable() {
		TableImpl table = new TableImpl();
		return table;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SQLEntity createSQLEntity() {
		SQLEntityImpl sqlEntity = new SQLEntityImpl();
		return sqlEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FromClause createFromClause() {
		FromClauseImpl fromClause = new FromClauseImpl();
		return fromClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SQLEntityModule createSQLEntityModule() {
		SQLEntityModuleImpl sqlEntityModule = new SQLEntityModuleImpl();
		return sqlEntityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntitySQL createGeneratedEntitySQL() {
		GeneratedEntitySQLImpl generatedEntitySQL = new GeneratedEntitySQLImpl();
		return generatedEntitySQL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntitySQLModule createGeneratedEntitySQLModule() {
		GeneratedEntitySQLModuleImpl generatedEntitySQLModule = new GeneratedEntitySQLModuleImpl();
		return generatedEntitySQLModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Sql_litePackage getSql_litePackage() {
		return (Sql_litePackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Sql_litePackage getPackage() {
		return Sql_litePackage.eINSTANCE;
	}

} //Sql_liteFactoryImpl
