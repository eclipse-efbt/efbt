/**
 */
package column_transformation_logic.util;

import column_transformation_logic.*;

import functions.Parameter;

import org.eclipse.emf.ecore.EModelElement;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.ETypedElement;

import org.eclipse.emf.ecore.util.Switch;

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
 * @see column_transformation_logic.Column_transformation_logicPackage
 * @generated
 */
public class Column_transformation_logicSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Column_transformation_logicPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Column_transformation_logicSwitch() {
		if (modelPackage == null) {
			modelPackage = Column_transformation_logicPackage.eINSTANCE;
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
			case Column_transformation_logicPackage.AGGREGATE_COLUMN_FUNCTION: {
				AggregateColumnFunction aggregateColumnFunction = (AggregateColumnFunction)theEObject;
				T result = caseAggregateColumnFunction(aggregateColumnFunction);
				if (result == null) result = caseColumnFunction(aggregateColumnFunction);
				if (result == null) result = caseEStructuralFeature(aggregateColumnFunction);
				if (result == null) result = caseETypedElement(aggregateColumnFunction);
				if (result == null) result = caseENamedElement(aggregateColumnFunction);
				if (result == null) result = caseEModelElement(aggregateColumnFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Column_transformation_logicPackage.BASIC_COLUMN_FUNCTION: {
				BasicColumnFunction basicColumnFunction = (BasicColumnFunction)theEObject;
				T result = caseBasicColumnFunction(basicColumnFunction);
				if (result == null) result = caseColumnFunction(basicColumnFunction);
				if (result == null) result = caseEStructuralFeature(basicColumnFunction);
				if (result == null) result = caseETypedElement(basicColumnFunction);
				if (result == null) result = caseENamedElement(basicColumnFunction);
				if (result == null) result = caseEModelElement(basicColumnFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Column_transformation_logicPackage.COLUMN_FUNCTION: {
				ColumnFunction columnFunction = (ColumnFunction)theEObject;
				T result = caseColumnFunction(columnFunction);
				if (result == null) result = caseEStructuralFeature(columnFunction);
				if (result == null) result = caseETypedElement(columnFunction);
				if (result == null) result = caseENamedElement(columnFunction);
				if (result == null) result = caseEModelElement(columnFunction);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Column_transformation_logicPackage.COLUMN_FUNCTION_GROUP: {
				ColumnFunctionGroup columnFunctionGroup = (ColumnFunctionGroup)theEObject;
				T result = caseColumnFunctionGroup(columnFunctionGroup);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Column_transformation_logicPackage.RESOLVED_CUBE_COLUMN_PARAMETER: {
				ResolvedCubeColumnParameter resolvedCubeColumnParameter = (ResolvedCubeColumnParameter)theEObject;
				T result = caseResolvedCubeColumnParameter(resolvedCubeColumnParameter);
				if (result == null) result = caseParameter(resolvedCubeColumnParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER: {
				SpeculativeCubeColumnParameter speculativeCubeColumnParameter = (SpeculativeCubeColumnParameter)theEObject;
				T result = caseSpeculativeCubeColumnParameter(speculativeCubeColumnParameter);
				if (result == null) result = caseParameter(speculativeCubeColumnParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Aggregate Column Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Aggregate Column Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAggregateColumnFunction(AggregateColumnFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Column Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Column Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicColumnFunction(BasicColumnFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Column Function</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Column Function</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseColumnFunction(ColumnFunction object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Column Function Group</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Column Function Group</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseColumnFunctionGroup(ColumnFunctionGroup object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Resolved Cube Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Resolved Cube Column Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseResolvedCubeColumnParameter(ResolvedCubeColumnParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Speculative Cube Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Speculative Cube Column Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSpeculativeCubeColumnParameter(SpeculativeCubeColumnParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EModel Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EModel Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEModelElement(EModelElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>ENamed Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>ENamed Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseENamedElement(ENamedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>ETyped Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>ETyped Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseETypedElement(ETypedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EStructural Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EStructural Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEStructuralFeature(EStructuralFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParameter(Parameter object) {
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

} //Column_transformation_logicSwitch
