/**
 */
package row_transformation_logic.impl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

import row_transformation_logic.*;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Row_transformation_logicFactoryImpl extends EFactoryImpl implements Row_transformation_logicFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Row_transformation_logicFactory init() {
		try {
			Row_transformation_logicFactory theRow_transformation_logicFactory = (Row_transformation_logicFactory)EPackage.Registry.INSTANCE.getEFactory(Row_transformation_logicPackage.eNS_URI);
			if (theRow_transformation_logicFactory != null) {
				return theRow_transformation_logicFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Row_transformation_logicFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Row_transformation_logicFactoryImpl() {
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
			case Row_transformation_logicPackage.CUSTOM_ROW_CREATION_APPROACH: return createCustomRowCreationApproach();
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH: return createFilterAndGroupToOneRowCreationApproach();
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY: return createRowCreationApproachForEntity();
			case Row_transformation_logicPackage.ONE_TO_ONE_ROW_CREATION_APPROACH: return createOneToOneRowCreationApproach();
			case Row_transformation_logicPackage.FILTER_ROW_CREATION_APPROACH: return createFilterRowCreationApproach();
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH: return createRowCreationApproach();
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH: return createGroupByRowCreationApproach();
			case Row_transformation_logicPackage.ROW_JOIN_FUNCTION: return createRowJoinFunction();
			case Row_transformation_logicPackage.UNION_ROW_CREATION_APPROACH: return createUnionRowCreationApproach();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CustomRowCreationApproach createCustomRowCreationApproach() {
		CustomRowCreationApproachImpl customRowCreationApproach = new CustomRowCreationApproachImpl();
		return customRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FilterAndGroupToOneRowCreationApproach createFilterAndGroupToOneRowCreationApproach() {
		FilterAndGroupToOneRowCreationApproachImpl filterAndGroupToOneRowCreationApproach = new FilterAndGroupToOneRowCreationApproachImpl();
		return filterAndGroupToOneRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RowCreationApproachForEntity createRowCreationApproachForEntity() {
		RowCreationApproachForEntityImpl rowCreationApproachForEntity = new RowCreationApproachForEntityImpl();
		return rowCreationApproachForEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OneToOneRowCreationApproach createOneToOneRowCreationApproach() {
		OneToOneRowCreationApproachImpl oneToOneRowCreationApproach = new OneToOneRowCreationApproachImpl();
		return oneToOneRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FilterRowCreationApproach createFilterRowCreationApproach() {
		FilterRowCreationApproachImpl filterRowCreationApproach = new FilterRowCreationApproachImpl();
		return filterRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RowCreationApproach createRowCreationApproach() {
		RowCreationApproachImpl rowCreationApproach = new RowCreationApproachImpl();
		return rowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GroupByRowCreationApproach createGroupByRowCreationApproach() {
		GroupByRowCreationApproachImpl groupByRowCreationApproach = new GroupByRowCreationApproachImpl();
		return groupByRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RowJoinFunction createRowJoinFunction() {
		RowJoinFunctionImpl rowJoinFunction = new RowJoinFunctionImpl();
		return rowJoinFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnionRowCreationApproach createUnionRowCreationApproach() {
		UnionRowCreationApproachImpl unionRowCreationApproach = new UnionRowCreationApproachImpl();
		return unionRowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Row_transformation_logicPackage getRow_transformation_logicPackage() {
		return (Row_transformation_logicPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Row_transformation_logicPackage getPackage() {
		return Row_transformation_logicPackage.eINSTANCE;
	}

} //Row_transformation_logicFactoryImpl
