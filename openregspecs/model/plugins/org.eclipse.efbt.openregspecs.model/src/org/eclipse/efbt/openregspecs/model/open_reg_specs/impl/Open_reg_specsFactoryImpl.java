/**
 */
package org.eclipse.efbt.openregspecs.model.open_reg_specs.impl;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.*;

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
public class Open_reg_specsFactoryImpl extends EFactoryImpl implements Open_reg_specsFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Open_reg_specsFactory init() {
		try {
			Open_reg_specsFactory theOpen_reg_specsFactory = (Open_reg_specsFactory)EPackage.Registry.INSTANCE.getEFactory(Open_reg_specsPackage.eNS_URI);
			if (theOpen_reg_specsFactory != null) {
				return theOpen_reg_specsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Open_reg_specsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Open_reg_specsFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Open_reg_specsPackage.OPEN_REG_SPECS: return createOpenRegSpecs();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OpenRegSpecs createOpenRegSpecs() {
		OpenRegSpecsImpl openRegSpecs = new OpenRegSpecsImpl();
		return openRegSpecs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Open_reg_specsPackage getOpen_reg_specsPackage() {
		return (Open_reg_specsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Open_reg_specsPackage getPackage() {
		return Open_reg_specsPackage.eINSTANCE;
	}

} //Open_reg_specsFactoryImpl
