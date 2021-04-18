/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.*;

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
public class Advanced_variable_lineagefunctionsFactoryImpl extends EFactoryImpl implements Advanced_variable_lineagefunctionsFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Advanced_variable_lineagefunctionsFactory init()
	{
		try {
			Advanced_variable_lineagefunctionsFactory theAdvanced_variable_lineagefunctionsFactory = (Advanced_variable_lineagefunctionsFactory)EPackage.Registry.INSTANCE.getEFactory(Advanced_variable_lineagefunctionsPackage.eNS_URI);
			if (theAdvanced_variable_lineagefunctionsFactory != null) {
				return theAdvanced_variable_lineagefunctionsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Advanced_variable_lineagefunctionsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Advanced_variable_lineagefunctionsFactoryImpl()
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
			case Advanced_variable_lineagefunctionsPackage.GET_STRUCT_COLUMN_FUNCTION: return createGetStructColumnFunction();
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER: return createResolvedStructColumnParameter();
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER: return createSpeculativeStructColumnParameter();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GetStructColumnFunction createGetStructColumnFunction()
	{
		GetStructColumnFunctionImpl getStructColumnFunction = new GetStructColumnFunctionImpl();
		return getStructColumnFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ResolvedStructColumnParameter createResolvedStructColumnParameter()
	{
		ResolvedStructColumnParameterImpl resolvedStructColumnParameter = new ResolvedStructColumnParameterImpl();
		return resolvedStructColumnParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SpeculativeStructColumnParameter createSpeculativeStructColumnParameter()
	{
		SpeculativeStructColumnParameterImpl speculativeStructColumnParameter = new SpeculativeStructColumnParameterImpl();
		return speculativeStructColumnParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Advanced_variable_lineagefunctionsPackage getAdvanced_variable_lineagefunctionsPackage()
	{
		return (Advanced_variable_lineagefunctionsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Advanced_variable_lineagefunctionsPackage getPackage()
	{
		return Advanced_variable_lineagefunctionsPackage.eINSTANCE;
	}

} //Advanced_variable_lineagefunctionsFactoryImpl
