/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.*;

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
public class MappingFactoryImpl extends EFactoryImpl implements MappingFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static MappingFactory init() {
		try {
			MappingFactory theMappingFactory = (MappingFactory)EPackage.Registry.INSTANCE.getEFactory(MappingPackage.eNS_URI);
			if (theMappingFactory != null) {
				return theMappingFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new MappingFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MappingFactoryImpl() {
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
			case MappingPackage.CUBE_MAPPING: return createCUBE_MAPPING();
			case MappingPackage.MEMBER_MAPPING: return createMEMBER_MAPPING();
			case MappingPackage.MEMBER_MAPPING_ITEM: return createMEMBER_MAPPING_ITEM();
			case MappingPackage.VARIABLE_MAPPING_ITEM: return createVARIABLE_MAPPING_ITEM();
			case MappingPackage.VARIABLE_MAPPING: return createVARIABLE_MAPPING();
			case MappingPackage.MAPPING_TO_CUBE: return createMAPPING_TO_CUBE();
			case MappingPackage.VARIABLE_SET_MAPPING: return createVARIABLE_SET_MAPPING();
			case MappingPackage.MAPPING_DEFINITION: return createMAPPING_DEFINITION();
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
	public CUBE_MAPPING createCUBE_MAPPING() {
		CUBE_MAPPINGImpl cubE_MAPPING = new CUBE_MAPPINGImpl();
		return cubE_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER_MAPPING createMEMBER_MAPPING() {
		MEMBER_MAPPINGImpl membeR_MAPPING = new MEMBER_MAPPINGImpl();
		return membeR_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER_MAPPING_ITEM createMEMBER_MAPPING_ITEM() {
		MEMBER_MAPPING_ITEMImpl membeR_MAPPING_ITEM = new MEMBER_MAPPING_ITEMImpl();
		return membeR_MAPPING_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_MAPPING_ITEM createVARIABLE_MAPPING_ITEM() {
		VARIABLE_MAPPING_ITEMImpl variablE_MAPPING_ITEM = new VARIABLE_MAPPING_ITEMImpl();
		return variablE_MAPPING_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_MAPPING createVARIABLE_MAPPING() {
		VARIABLE_MAPPINGImpl variablE_MAPPING = new VARIABLE_MAPPINGImpl();
		return variablE_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MAPPING_TO_CUBE createMAPPING_TO_CUBE() {
		MAPPING_TO_CUBEImpl mappinG_TO_CUBE = new MAPPING_TO_CUBEImpl();
		return mappinG_TO_CUBE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_SET_MAPPING createVARIABLE_SET_MAPPING() {
		VARIABLE_SET_MAPPINGImpl variablE_SET_MAPPING = new VARIABLE_SET_MAPPINGImpl();
		return variablE_SET_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MAPPING_DEFINITION createMAPPING_DEFINITION() {
		MAPPING_DEFINITIONImpl mappinG_DEFINITION = new MAPPING_DEFINITIONImpl();
		return mappinG_DEFINITION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MappingPackage getMappingPackage() {
		return (MappingPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static MappingPackage getPackage() {
		return MappingPackage.eINSTANCE;
	}

} //MappingFactoryImpl
