/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.*;

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
public class Smcubes_functionsFactoryImpl extends EFactoryImpl implements Smcubes_functionsFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Smcubes_functionsFactory init()
	{
		try {
			Smcubes_functionsFactory theSmcubes_functionsFactory = (Smcubes_functionsFactory)EPackage.Registry.INSTANCE.getEFactory(Smcubes_functionsPackage.eNS_URI);
			if (theSmcubes_functionsFactory != null) {
				return theSmcubes_functionsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Smcubes_functionsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Smcubes_functionsFactoryImpl()
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
			case Smcubes_functionsPackage.MEMBER_PARAMETER: return createMemberParameter();
			case Smcubes_functionsPackage.VALUE_PARAMETER: return createValueParameter();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
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
	public Smcubes_functionsPackage getSmcubes_functionsPackage()
	{
		return (Smcubes_functionsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Smcubes_functionsPackage getPackage()
	{
		return Smcubes_functionsPackage.eINSTANCE;
	}

} //Smcubes_functionsFactoryImpl
