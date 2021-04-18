/**
 */
package org.eclipse.efbt.language.dtm.model.transformation;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage
 * @generated
 */
public interface TransformationFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TransformationFactory eINSTANCE = org.eclipse.efbt.language.dtm.model.transformation.impl.TransformationFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Data Set Transformation</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Data Set Transformation</em>'.
	 * @generated
	 */
	DataSetTransformation createDataSetTransformation();

	/**
	 * Returns a new object of class '<em>Release</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Release</em>'.
	 * @generated
	 */
	Release createRelease();

	/**
	 * Returns a new object of class '<em>Versioned Components Set</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Versioned Components Set</em>'.
	 * @generated
	 */
	VersionedComponentsSet createVersionedComponentsSet();

	/**
	 * Returns a new object of class '<em>Versioned Cube Schema Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Versioned Cube Schema Module</em>'.
	 * @generated
	 */
	VersionedCubeSchemaModule createVersionedCubeSchemaModule();

	/**
	 * Returns a new object of class '<em>Versioned Functional Module Logic</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Versioned Functional Module Logic</em>'.
	 * @generated
	 */
	VersionedFunctionalModuleLogic createVersionedFunctionalModuleLogic();

	/**
	 * Returns a new object of class '<em>dtm Executable Logic</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>dtm Executable Logic</em>'.
	 * @generated
	 */
	dtmExecutableLogic createdtmExecutableLogic();

	/**
	 * Returns a new object of class '<em>dtm Program</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>dtm Program</em>'.
	 * @generated
	 */
	dtmProgram createdtmProgram();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	TransformationPackage getTransformationPackage();

} //TransformationFactory
