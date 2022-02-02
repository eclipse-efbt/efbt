/**
 */
package column_transformation_logic.impl;

import column_transformation_logic.*;

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
public class Column_transformation_logicFactoryImpl extends EFactoryImpl implements Column_transformation_logicFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Column_transformation_logicFactory init() {
		try {
			Column_transformation_logicFactory theColumn_transformation_logicFactory = (Column_transformation_logicFactory)EPackage.Registry.INSTANCE.getEFactory(Column_transformation_logicPackage.eNS_URI);
			if (theColumn_transformation_logicFactory != null) {
				return theColumn_transformation_logicFactory;
			}
		}
		catch (Exception exception) {
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
	public Column_transformation_logicFactoryImpl() {
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
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION: return createAggregateColumnFunction();
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION: return createBasicColumnFunction();
			case Column_transformation_logicPackage.COLUMN_FUNCTION: return createColumnFunction();
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP: return createColumnFunctionGroup();
			case Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER: return createResolvedCubeColumnParameter();
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER: return createSpeculativeCubeColumnParameter();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateColumnFunction createAggregateColumnFunction() {
		AggregateColumnFunctionImpl aggregateColumnFunction = new AggregateColumnFunctionImpl();
		return aggregateColumnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicColumnFunction createBasicColumnFunction() {
		BasicColumnFunctionImpl basicColumnFunction = new BasicColumnFunctionImpl();
		return basicColumnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ColumnFunction createColumnFunction() {
		ColumnFunctionImpl columnFunction = new ColumnFunctionImpl();
		return columnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ColumnFunctionGroup createColumnFunctionGroup() {
		ColumnFunctionGroupImpl columnFunctionGroup = new ColumnFunctionGroupImpl();
		return columnFunctionGroup;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ResolvedCubeColumnParameter createResolvedCubeColumnParameter() {
		ResolvedCubeColumnParameterImpl resolvedCubeColumnParameter = new ResolvedCubeColumnParameterImpl();
		return resolvedCubeColumnParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SpeculativeCubeColumnParameter createSpeculativeCubeColumnParameter() {
		SpeculativeCubeColumnParameterImpl speculativeCubeColumnParameter = new SpeculativeCubeColumnParameterImpl();
		return speculativeCubeColumnParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Column_transformation_logicPackage getColumn_transformation_logicPackage() {
		return (Column_transformation_logicPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Column_transformation_logicPackage getPackage() {
		return Column_transformation_logicPackage.eINSTANCE;
	}

} //Column_transformation_logicFactoryImpl
