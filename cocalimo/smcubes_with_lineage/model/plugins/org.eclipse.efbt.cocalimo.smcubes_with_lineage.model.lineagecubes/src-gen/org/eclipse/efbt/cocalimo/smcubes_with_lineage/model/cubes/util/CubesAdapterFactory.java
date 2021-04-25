/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.util;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage
 * @generated
 */
public class CubesAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static CubesPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubesAdapterFactory()
	{
		if (modelPackage == null) {
			modelPackage = CubesPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object)
	{
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CubesSwitch<Adapter> modelSwitch =
		new CubesSwitch<Adapter>() {
			@Override
			public Adapter caseBaseCube(BaseCube object) {
				return createBaseCubeAdapter();
			}
			@Override
			public Adapter caseDerivedCube(DerivedCube object) {
				return createDerivedCubeAdapter();
			}
			@Override
			public Adapter caseFreeBirdToolsCube(FreeBirdToolsCube object) {
				return createFreeBirdToolsCubeAdapter();
			}
			@Override
			public Adapter caseFreeBirdToolsCubeModule(FreeBirdToolsCubeModule object) {
				return createFreeBirdToolsCubeModuleAdapter();
			}
			@Override
			public Adapter caseTargetCube(TargetCube object) {
				return createTargetCubeAdapter();
			}
			@Override
			public Adapter caseFreeBIRDToolsCubeHierarchyRelationshipModule(FreeBIRDToolsCubeHierarchyRelationshipModule object) {
				return createFreeBIRDToolsCubeHierarchyRelationshipModuleAdapter();
			}
			@Override
			public Adapter caseFreeBIRDToolsCubeHierarchyRelationship(FreeBIRDToolsCubeHierarchyRelationship object) {
				return createFreeBIRDToolsCubeHierarchyRelationshipAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target)
	{
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube <em>Base Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube
	 * @generated
	 */
	public Adapter createBaseCubeAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube <em>Derived Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube
	 * @generated
	 */
	public Adapter createDerivedCubeAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube <em>Free Bird Tools Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube
	 * @generated
	 */
	public Adapter createFreeBirdToolsCubeAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule <em>Free Bird Tools Cube Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule
	 * @generated
	 */
	public Adapter createFreeBirdToolsCubeModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.TargetCube <em>Target Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.TargetCube
	 * @generated
	 */
	public Adapter createTargetCubeAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule <em>Free BIRD Tools Cube Hierarchy Relationship Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule
	 * @generated
	 */
	public Adapter createFreeBIRDToolsCubeHierarchyRelationshipModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship <em>Free BIRD Tools Cube Hierarchy Relationship</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship
	 * @generated
	 */
	public Adapter createFreeBIRDToolsCubeHierarchyRelationshipAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter()
	{
		return null;
	}

} //CubesAdapterFactory
