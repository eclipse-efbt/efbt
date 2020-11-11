/**
 */
package cubes;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Cube</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This is a cube which can contain data which is derived from BaseCubes, and other DerivedCubes. DeriveCubes are different from BaseCubes which can contain actual base data 
 * The DerivedCube class is a lightweight class which effectively just has a name, and the set of source cubes which it is derived from, this allows us to define a very high level outline of the relationships between cubes.
 * The DataSetTransformation class in the tranformation package links the DerivedCube to the logic which is used to derive the cubes structure from the structure of its source cubes, and to derive its data from populated source cubes.
 * Note that the DataSetTransformations has many subsclasses, currently they represent structures from the TraceableReportingLanguage (TRL), and soon these will be replaced by subclasses which represent structures from VTL.
 * When we create AttributeLineageModels, The ColumnFunctionGroup from the row_transformation_logic package shows the structure of the DerivedCube as a set of Columns each with a formula deriving data from columns of source cubes.
 * The SpreadsheetLineageModel class from the data_lineage package shows tha actual derived data for a derived cube, supplemented with the formula used to calculate each piece of data from data in populated source cubes.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link cubes.DerivedCube#getSourceCubes <em>Source Cubes</em>}</li>
 * </ul>
 *
 * @see cubes.CubesPackage#getDerivedCube()
 * @model extendedMetaData="name='DerivedCube' kind='empty'"
 * @generated
 */
public interface DerivedCube extends FreeBirdToolsCube
{
	/**
	 * Returns the value of the '<em><b>Source Cubes</b></em>' reference list.
	 * The list contents are of type {@link cubes.FreeBirdToolsCube}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the sourceCubes from which this cube is derived
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Source Cubes</em>' reference list.
	 * @see cubes.CubesPackage#getDerivedCube_SourceCubes()
	 * @model extendedMetaData="kind='attribute' name='sourceCubes'"
	 * @generated
	 */
	EList<FreeBirdToolsCube> getSourceCubes();

} // DerivedCube
