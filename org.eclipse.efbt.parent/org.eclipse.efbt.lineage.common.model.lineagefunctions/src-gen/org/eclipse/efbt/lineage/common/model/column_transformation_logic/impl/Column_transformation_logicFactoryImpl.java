/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.*;

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
public class Column_transformation_logicFactoryImpl extends EFactoryImpl implements Column_transformation_logicFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Column_transformation_logicFactory init()
	{
		try
		{
			Column_transformation_logicFactory theColumn_transformation_logicFactory = (Column_transformation_logicFactory)EPackage.Registry.INSTANCE.getEFactory(Column_transformation_logicPackage.eNS_URI);
			if (theColumn_transformation_logicFactory != null)
			{
				return theColumn_transformation_logicFactory;
			}
		}
		catch (Exception exception)
		{
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Column_transformation_logicFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Column_transformation_logicFactoryImpl()
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
		switch (eClass.getClassifierID())
		{
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION: return createAggregateColumnFunction();
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION: return createBasicColumnFunction();
			case Column_transformation_logicPackage.COLUMN_FUNCTION: return createColumnFunction();
			case Column_transformation_logicPackage.STANDARD_BASIC_COLUMN_FUNCTION: return createStandardBasicColumnFunction();
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP: return createColumnFunctionGroup();
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
	public AggregateColumnFunction createAggregateColumnFunction()
	{
		AggregateColumnFunctionImpl aggregateColumnFunction = new AggregateColumnFunctionImpl();
		return aggregateColumnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BasicColumnFunction createBasicColumnFunction()
	{
		BasicColumnFunctionImpl basicColumnFunction = new BasicColumnFunctionImpl();
		return basicColumnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ColumnFunction createColumnFunction()
	{
		ColumnFunctionImpl columnFunction = new ColumnFunctionImpl();
		return columnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StandardBasicColumnFunction createStandardBasicColumnFunction()
	{
		StandardBasicColumnFunctionImpl standardBasicColumnFunction = new StandardBasicColumnFunctionImpl();
		return standardBasicColumnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ColumnFunctionGroup createColumnFunctionGroup()
	{
		ColumnFunctionGroupImpl columnFunctionGroup = new ColumnFunctionGroupImpl();
		return columnFunctionGroup;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Column_transformation_logicPackage getColumn_transformation_logicPackage()
	{
		return (Column_transformation_logicPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Column_transformation_logicPackage getPackage()
	{
		return Column_transformation_logicPackage.eINSTANCE;
	}

} //Column_transformation_logicFactoryImpl
