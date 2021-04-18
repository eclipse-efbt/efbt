/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Coverage Grid Point</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * One Point of the Coverage grid
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint#getCount <em>Count</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint#getObjects <em>Objects</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getCoverageGridPoint()
 * @model
 * @generated
 */
public interface CoverageGridPoint extends EObject
{
	/**
	 * Returns the value of the '<em><b>Count</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Count</em>' attribute.
	 * @see #setCount(int)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getCoverageGridPoint_Count()
	 * @model
	 * @generated
	 */
	int getCount();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint#getCount <em>Count</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Count</em>' attribute.
	 * @see #getCount()
	 * @generated
	 */
	void setCount(int value);

	/**
	 * Returns the value of the '<em><b>Objects</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EObject}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of parameter values that identify that grid point.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Objects</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getCoverageGridPoint_Objects()
	 * @model
	 * @generated
	 */
	EList<EObject> getObjects();

} // CoverageGridPoint
