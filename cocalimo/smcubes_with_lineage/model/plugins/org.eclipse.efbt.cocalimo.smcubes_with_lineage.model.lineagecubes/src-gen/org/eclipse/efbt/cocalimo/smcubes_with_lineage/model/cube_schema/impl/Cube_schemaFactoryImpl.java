/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Cube_schemaFactoryImpl extends EFactoryImpl implements Cube_schemaFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Cube_schemaFactory init()
	{
		try {
			Cube_schemaFactory theCube_schemaFactory = (Cube_schemaFactory)EPackage.Registry.INSTANCE.getEFactory(Cube_schemaPackage.eNS_URI);
			if (theCube_schemaFactory != null) {
				return theCube_schemaFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Cube_schemaFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cube_schemaFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID()) {
			case Cube_schemaPackage.CUBE_SCHEMA: return createCubeSchema();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeSchema createCubeSchema()
	{
		CubeSchemaImpl cubeSchema = new CubeSchemaImpl();
		return cubeSchema;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Cube_schemaPackage getCube_schemaPackage()
	{
		return (Cube_schemaPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Cube_schemaPackage getPackage()
	{
		return Cube_schemaPackage.eINSTANCE;
	}

} //Cube_schemaFactoryImpl
