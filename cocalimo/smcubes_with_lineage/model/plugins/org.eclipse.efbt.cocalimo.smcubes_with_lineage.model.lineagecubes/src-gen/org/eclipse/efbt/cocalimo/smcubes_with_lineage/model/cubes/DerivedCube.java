/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Cube</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This is a cube which can contain data which is derived from BaseCubes, and other DerivedCubes. DeriveCubes are different from BaseCubes which can contain actual base data 
 * The DerivedCube class is a lightweight class which effectively just has a name, and the set of source cubes which it is derived from, this allows us to define a very high level outline of the relationships between cubes.
 * 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube#getSourceCubes <em>Source Cubes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage#getDerivedCube()
 * @model extendedMetaData="name='DerivedCube' kind='empty'"
 * @generated
 */
public interface DerivedCube extends FreeBirdToolsCube
{
	/**
	 * Returns the value of the '<em><b>Source Cubes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the sourceCubes from which this cube is derived
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Source Cubes</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage#getDerivedCube_SourceCubes()
	 * @model extendedMetaData="kind='attribute' name='sourceCubes'"
	 * @generated
	 */
	EList<FreeBirdToolsCube> getSourceCubes();

} // DerivedCube
