/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SM Cubes Test Input Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * InputData for that represents instances of SMCubesModels, they are set of rws of data meeting the cube structure of an associated cube
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesTestInputData#getSmcubes_inputdata <em>Smcubes inputdata</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesTestInputData()
 * @model
 * @generated
 */
public interface SMCubesTestInputData extends TestInputData
{
	/**
	 * Returns the value of the '<em><b>Smcubes inputdata</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The input data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Smcubes inputdata</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesTestInputData_Smcubes_inputdata()
	 * @model containment="true"
	 * @generated
	 */
	EList<BaseColumnStructuredData> getSmcubes_inputdata();

} // SMCubesTestInputData
