/**
 */
package row_transformation_logic.util;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

import row_transformation_logic.*;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see row_transformation_logic.Row_transformation_logicPackage
 * @generated
 */
public class Row_transformation_logicAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Row_transformation_logicPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Row_transformation_logicAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Row_transformation_logicPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Row_transformation_logicSwitch<Adapter> modelSwitch =
		new Row_transformation_logicSwitch<Adapter>() {
			@Override
			public Adapter caseCustomRowCreationApproach(CustomRowCreationApproach object) {
				return createCustomRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseFilterAndGroupToOneRowCreationApproach(FilterAndGroupToOneRowCreationApproach object) {
				return createFilterAndGroupToOneRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseRowCreationApproachForEntity(RowCreationApproachForEntity object) {
				return createRowCreationApproachForEntityAdapter();
			}
			@Override
			public Adapter caseOneToOneRowCreationApproach(OneToOneRowCreationApproach object) {
				return createOneToOneRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseFilterRowCreationApproach(FilterRowCreationApproach object) {
				return createFilterRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseRowCreationApproach(RowCreationApproach object) {
				return createRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseGroupByRowCreationApproach(GroupByRowCreationApproach object) {
				return createGroupByRowCreationApproachAdapter();
			}
			@Override
			public Adapter caseRowJoinFunction(RowJoinFunction object) {
				return createRowJoinFunctionAdapter();
			}
			@Override
			public Adapter caseUnionRowCreationApproach(UnionRowCreationApproach object) {
				return createUnionRowCreationApproachAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.CustomRowCreationApproach <em>Custom Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.CustomRowCreationApproach
	 * @generated
	 */
	public Adapter createCustomRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.FilterAndGroupToOneRowCreationApproach <em>Filter And Group To One Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.FilterAndGroupToOneRowCreationApproach
	 * @generated
	 */
	public Adapter createFilterAndGroupToOneRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.RowCreationApproachForEntity <em>Row Creation Approach For Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.RowCreationApproachForEntity
	 * @generated
	 */
	public Adapter createRowCreationApproachForEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.OneToOneRowCreationApproach <em>One To One Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.OneToOneRowCreationApproach
	 * @generated
	 */
	public Adapter createOneToOneRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.FilterRowCreationApproach <em>Filter Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.FilterRowCreationApproach
	 * @generated
	 */
	public Adapter createFilterRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.RowCreationApproach <em>Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.RowCreationApproach
	 * @generated
	 */
	public Adapter createRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.GroupByRowCreationApproach <em>Group By Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.GroupByRowCreationApproach
	 * @generated
	 */
	public Adapter createGroupByRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.RowJoinFunction <em>Row Join Function</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.RowJoinFunction
	 * @generated
	 */
	public Adapter createRowJoinFunctionAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link row_transformation_logic.UnionRowCreationApproach <em>Union Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see row_transformation_logic.UnionRowCreationApproach
	 * @generated
	 */
	public Adapter createUnionRowCreationApproachAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //Row_transformation_logicAdapterFactory
