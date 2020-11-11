/**
 */
package cube_schema;

import cubes.BaseCube;

import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cube Schema</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * This class  describes the schemas of FreeBirdToolsCubes by providing  means to access the relevent the data_definition:CUBE_STRUCTURE via an associated data_definition:CUBE  
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link cube_schema.CubeSchema#getCube <em>Cube</em>}</li>
 *   <li>{@link cube_schema.CubeSchema#getName <em>Name</em>}</li>
 *   <li>{@link cube_schema.CubeSchema#getBaseCube <em>Base Cube</em>}</li>
 * </ul>
 *
 * @see cube_schema.Cube_schemaPackage#getCubeSchema()
 * @model extendedMetaData="name='CubeSchema' kind='empty'"
 * @generated
 */
public interface CubeSchema extends EObject
{
	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the associated data_definition:CUBE
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(CUBE)
	 * @see cube_schema.Cube_schemaPackage#getCubeSchema_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	CUBE getCube();

	/**
	 * Sets the value of the '{@link cube_schema.CubeSchema#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(CUBE value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the schema name
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see cube_schema.Cube_schemaPackage#getCubeSchema_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link cube_schema.CubeSchema#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The FreeBIRDToolsCube
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Base Cube</em>' reference.
	 * @see #setBaseCube(BaseCube)
	 * @see cube_schema.Cube_schemaPackage#getCubeSchema_BaseCube()
	 * @model extendedMetaData="kind='attribute' name='baseCube'"
	 * @generated
	 */
	BaseCube getBaseCube();

	/**
	 * Sets the value of the '{@link cube_schema.CubeSchema#getBaseCube <em>Base Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Base Cube</em>' reference.
	 * @see #getBaseCube()
	 * @generated
	 */
	void setBaseCube(BaseCube value);

} // CubeSchema
