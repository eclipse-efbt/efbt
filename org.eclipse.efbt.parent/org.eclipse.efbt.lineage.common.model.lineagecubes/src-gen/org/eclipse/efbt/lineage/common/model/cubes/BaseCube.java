/**
 */
package org.eclipse.efbt.lineage.common.model.cubes;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Cube</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This is a cube which can contain actual base data, as opposed to DerivedCubes which contain data which is derived from BaseCubes, and other DerivedCubes. 
 * The BaseCube class is a lightweight class which effectively just has a name, which allows us to define a very high level outline of the relationships between cubes.
 * The CubeSchema class in the cube_schema package links the BaseCube to a CUBE from the data_definition package which has a structure.
 * The BaseColumnStructuredData class in the base_column_structured_data package associates the  BaseCube with actual data to describe a populated cube.
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.lineage.common.model.cubes.CubesPackage#getBaseCube()
 * @model extendedMetaData="name='BaseCube' kind='empty'"
 * @generated
 */
public interface BaseCube extends FreeBirdToolsCube
{
} // BaseCube
