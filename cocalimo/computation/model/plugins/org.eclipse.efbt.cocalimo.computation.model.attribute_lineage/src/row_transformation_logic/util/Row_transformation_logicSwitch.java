/**
 */
package row_transformation_logic.util;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

import row_transformation_logic.*;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see row_transformation_logic.Row_transformation_logicPackage
 * @generated
 */
public class Row_transformation_logicSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Row_transformation_logicPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Row_transformation_logicSwitch() {
		if (modelPackage == null) {
			modelPackage = Row_transformation_logicPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case Row_transformation_logicPackage.CUSTOM_ROW_CREATION_APPROACH: {
				CustomRowCreationApproach customRowCreationApproach = (CustomRowCreationApproach)theEObject;
				T result = caseCustomRowCreationApproach(customRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(customRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH: {
				FilterAndGroupToOneRowCreationApproach filterAndGroupToOneRowCreationApproach = (FilterAndGroupToOneRowCreationApproach)theEObject;
				T result = caseFilterAndGroupToOneRowCreationApproach(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(filterAndGroupToOneRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY: {
				RowCreationApproachForEntity rowCreationApproachForEntity = (RowCreationApproachForEntity)theEObject;
				T result = caseRowCreationApproachForEntity(rowCreationApproachForEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.ONE_TO_ONE_ROW_CREATION_APPROACH: {
				OneToOneRowCreationApproach oneToOneRowCreationApproach = (OneToOneRowCreationApproach)theEObject;
				T result = caseOneToOneRowCreationApproach(oneToOneRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(oneToOneRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.FILTER_ROW_CREATION_APPROACH: {
				FilterRowCreationApproach filterRowCreationApproach = (FilterRowCreationApproach)theEObject;
				T result = caseFilterRowCreationApproach(filterRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(filterRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH: {
				RowCreationApproach rowCreationApproach = (RowCreationApproach)theEObject;
				T result = caseRowCreationApproach(rowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH: {
				GroupByRowCreationApproach groupByRowCreationApproach = (GroupByRowCreationApproach)theEObject;
				T result = caseGroupByRowCreationApproach(groupByRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(groupByRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.ROW_JOIN_FUNCTION: {
				RowJoinFunction rowJoinFunction = (RowJoinFunction)theEObject;
				T result = caseRowJoinFunction(rowJoinFunction);
				if (result == null) result = caseRowCreationApproach(rowJoinFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Row_transformation_logicPackage.UNION_ROW_CREATION_APPROACH: {
				UnionRowCreationApproach unionRowCreationApproach = (UnionRowCreationApproach)theEObject;
				T result = caseUnionRowCreationApproach(unionRowCreationApproach);
				if (result == null) result = caseRowCreationApproach(unionRowCreationApproach);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Custom Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Custom Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCustomRowCreationApproach(CustomRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Filter And Group To One Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Filter And Group To One Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFilterAndGroupToOneRowCreationApproach(FilterAndGroupToOneRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Row Creation Approach For Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Row Creation Approach For Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRowCreationApproachForEntity(RowCreationApproachForEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>One To One Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>One To One Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOneToOneRowCreationApproach(OneToOneRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Filter Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Filter Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFilterRowCreationApproach(FilterRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRowCreationApproach(RowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Group By Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Group By Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGroupByRowCreationApproach(GroupByRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Row Join Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Row Join Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRowJoinFunction(RowJoinFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Union Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Union Row Creation Approach</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseUnionRowCreationApproach(UnionRowCreationApproach object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //Row_transformation_logicSwitch
