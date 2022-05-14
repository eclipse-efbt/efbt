/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.util;

import org.eclipse.efbt.openregspecs.model.rendering.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

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
 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage
 * @generated
 */
public class RenderingSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static RenderingPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RenderingSwitch() {
		if (modelPackage == null) {
			modelPackage = RenderingPackage.eINSTANCE;
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
			case RenderingPackage.AXIS: {
				AXIS axis = (AXIS)theEObject;
				T result = caseAXIS(axis);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case RenderingPackage.AXIS_ORDINATE: {
				AXIS_ORDINATE axiS_ORDINATE = (AXIS_ORDINATE)theEObject;
				T result = caseAXIS_ORDINATE(axiS_ORDINATE);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case RenderingPackage.CELL_POSITION: {
				CELL_POSITION celL_POSITION = (CELL_POSITION)theEObject;
				T result = caseCELL_POSITION(celL_POSITION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case RenderingPackage.ORDINATE_ITEM: {
				ORDINATE_ITEM ordinatE_ITEM = (ORDINATE_ITEM)theEObject;
				T result = caseORDINATE_ITEM(ordinatE_ITEM);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case RenderingPackage.TABLE: {
				TABLE table = (TABLE)theEObject;
				T result = caseTABLE(table);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case RenderingPackage.TABLE_CELL: {
				TABLE_CELL tablE_CELL = (TABLE_CELL)theEObject;
				T result = caseTABLE_CELL(tablE_CELL);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>AXIS</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>AXIS</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAXIS(AXIS object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>AXIS ORDINATE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>AXIS ORDINATE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAXIS_ORDINATE(AXIS_ORDINATE object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CELL POSITION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CELL POSITION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCELL_POSITION(CELL_POSITION object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>ORDINATE ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>ORDINATE ITEM</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseORDINATE_ITEM(ORDINATE_ITEM object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>TABLE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>TABLE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTABLE(TABLE object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>TABLE CELL</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>TABLE CELL</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTABLE_CELL(TABLE_CELL object) {
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

} //RenderingSwitch
