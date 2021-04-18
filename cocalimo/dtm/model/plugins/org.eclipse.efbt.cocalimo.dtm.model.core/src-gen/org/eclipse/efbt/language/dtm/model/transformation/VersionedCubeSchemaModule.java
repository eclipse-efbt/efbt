/**
 */
package org.eclipse.efbt.language.dtm.model.transformation;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Versioned Cube Schema Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module of VersionedCubeSchemas
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule#getSchemas <em>Schemas</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedCubeSchemaModule()
 * @model extendedMetaData="name='VersionedCubeSchemaModule' kind='elementOnly'"
 * @generated
 */
public interface VersionedCubeSchemaModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Schemas</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The schemas in the module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Schemas</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedCubeSchemaModule_Schemas()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='schemas'"
	 * @generated
	 */
	EList<CubeSchema> getSchemas();

} // VersionedCubeSchemaModule
