/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions.impl;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.*;

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
public class LineagefunctionsFactoryImpl extends EFactoryImpl implements LineagefunctionsFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static LineagefunctionsFactory init()
	{
		try
		{
			LineagefunctionsFactory theLineagefunctionsFactory = (LineagefunctionsFactory)EPackage.Registry.INSTANCE.getEFactory(LineagefunctionsPackage.eNS_URI);
			if (theLineagefunctionsFactory != null)
			{
				return theLineagefunctionsFactory;
			}
		}
		catch (Exception exception)
		{
			EcorePlugin.INSTANCE.log(exception);
		}
		return new LineagefunctionsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LineagefunctionsFactoryImpl()
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
		switch (eClass.getClassifierID())
		{
			case LineagefunctionsPackage.MEMBER_PARAMETER: return createMemberParameter();
			case LineagefunctionsPackage.RESOLVED_CUBE_COLUMN_PARAMETER: return createResolvedCubeColumnParameter();
			case LineagefunctionsPackage.CUBE_COLUMN: return createCubeColumn();
			case LineagefunctionsPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER: return createSpeculativeCubeColumnParameter();
			case LineagefunctionsPackage.VALUE_PARAMETER: return createValueParameter();
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
	public MemberParameter createMemberParameter()
	{
		MemberParameterImpl memberParameter = new MemberParameterImpl();
		return memberParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResolvedCubeColumnParameter createResolvedCubeColumnParameter()
	{
		ResolvedCubeColumnParameterImpl resolvedCubeColumnParameter = new ResolvedCubeColumnParameterImpl();
		return resolvedCubeColumnParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeColumn createCubeColumn()
	{
		CubeColumnImpl cubeColumn = new CubeColumnImpl();
		return cubeColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SpeculativeCubeColumnParameter createSpeculativeCubeColumnParameter()
	{
		SpeculativeCubeColumnParameterImpl speculativeCubeColumnParameter = new SpeculativeCubeColumnParameterImpl();
		return speculativeCubeColumnParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ValueParameter createValueParameter()
	{
		ValueParameterImpl valueParameter = new ValueParameterImpl();
		return valueParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LineagefunctionsPackage getLineagefunctionsPackage()
	{
		return (LineagefunctionsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static LineagefunctionsPackage getPackage()
	{
		return LineagefunctionsPackage.eINSTANCE;
	}

} //LineagefunctionsFactoryImpl
