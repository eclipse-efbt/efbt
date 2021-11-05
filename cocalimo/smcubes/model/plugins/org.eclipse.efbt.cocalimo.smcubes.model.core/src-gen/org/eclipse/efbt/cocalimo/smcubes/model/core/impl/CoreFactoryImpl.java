/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
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
public class CoreFactoryImpl extends EFactoryImpl implements CoreFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static CoreFactory init() {
		try {
			CoreFactory theCoreFactory = (CoreFactory)EPackage.Registry.INSTANCE.getEFactory(CorePackage.eNS_URI);
			if (theCoreFactory != null) {
				return theCoreFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new CoreFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CoreFactoryImpl() {
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
			case CorePackage.DOMAIN: return createDOMAIN();
			case CorePackage.FACET_COLLECTION: return createFACET_COLLECTION();
			case CorePackage.FACET_ENUMERATION: return createFACET_ENUMERATION();
			case CorePackage.FACET_TYPE: return createfacet_type();
			case CorePackage.MAINTENANCE_AGENCY: return createMAINTENANCE_AGENCY();
			case CorePackage.MEMBER: return createMEMBER();
			case CorePackage.MEMBER_HIERARCHY: return createMEMBER_HIERARCHY();
			case CorePackage.MEMBER_HIERARCHY_NODE: return createMEMBER_HIERARCHY_NODE();
			case CorePackage.SUBDOMAIN: return createSUBDOMAIN();
			case CorePackage.SUBDOMAIN_ENUMERATION: return createSUBDOMAIN_ENUMERATION();
			case CorePackage.VARIABLE: return createVARIABLE();
			case CorePackage.VARIABLE_SET: return createVARIABLE_SET();
			case CorePackage.VARIABLE_SET_ENUMERATION: return createVARIABLE_SET_ENUMERATION();
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
	public Object createFromString(EDataType eDataType, String initialValue) {
		switch (eDataType.getClassifierID()) {
			case CorePackage.FACET_VALUE_TYPE:
				return createFACET_VALUE_TYPEFromString(eDataType, initialValue);
			case CorePackage.FACET_VALUE_TYPE_OBJECT:
				return createFACET_VALUE_TYPEObjectFromString(eDataType, initialValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String convertToString(EDataType eDataType, Object instanceValue) {
		switch (eDataType.getClassifierID()) {
			case CorePackage.FACET_VALUE_TYPE:
				return convertFACET_VALUE_TYPEToString(eDataType, instanceValue);
			case CorePackage.FACET_VALUE_TYPE_OBJECT:
				return convertFACET_VALUE_TYPEObjectToString(eDataType, instanceValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DOMAIN createDOMAIN() {
		DOMAINImpl domain = new DOMAINImpl();
		return domain;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FACET_COLLECTION createFACET_COLLECTION() {
		FACET_COLLECTIONImpl faceT_COLLECTION = new FACET_COLLECTIONImpl();
		return faceT_COLLECTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FACET_ENUMERATION createFACET_ENUMERATION() {
		FACET_ENUMERATIONImpl faceT_ENUMERATION = new FACET_ENUMERATIONImpl();
		return faceT_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public facet_type createfacet_type() {
		facet_typeImpl facet_type = new facet_typeImpl();
		return facet_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MAINTENANCE_AGENCY createMAINTENANCE_AGENCY() {
		MAINTENANCE_AGENCYImpl maintenancE_AGENCY = new MAINTENANCE_AGENCYImpl();
		return maintenancE_AGENCY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER createMEMBER() {
		MEMBERImpl member = new MEMBERImpl();
		return member;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER_HIERARCHY createMEMBER_HIERARCHY() {
		MEMBER_HIERARCHYImpl membeR_HIERARCHY = new MEMBER_HIERARCHYImpl();
		return membeR_HIERARCHY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER_HIERARCHY_NODE createMEMBER_HIERARCHY_NODE() {
		MEMBER_HIERARCHY_NODEImpl membeR_HIERARCHY_NODE = new MEMBER_HIERARCHY_NODEImpl();
		return membeR_HIERARCHY_NODE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SUBDOMAIN createSUBDOMAIN() {
		SUBDOMAINImpl subdomain = new SUBDOMAINImpl();
		return subdomain;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SUBDOMAIN_ENUMERATION createSUBDOMAIN_ENUMERATION() {
		SUBDOMAIN_ENUMERATIONImpl subdomaiN_ENUMERATION = new SUBDOMAIN_ENUMERATIONImpl();
		return subdomaiN_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE createVARIABLE() {
		VARIABLEImpl variable = new VARIABLEImpl();
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_SET createVARIABLE_SET() {
		VARIABLE_SETImpl variablE_SET = new VARIABLE_SETImpl();
		return variablE_SET;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_SET_ENUMERATION createVARIABLE_SET_ENUMERATION() {
		VARIABLE_SET_ENUMERATIONImpl variablE_SET_ENUMERATION = new VARIABLE_SET_ENUMERATIONImpl();
		return variablE_SET_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_VALUE_TYPE createFACET_VALUE_TYPEFromString(EDataType eDataType, String initialValue) {
		FACET_VALUE_TYPE result = FACET_VALUE_TYPE.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertFACET_VALUE_TYPEToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_VALUE_TYPE createFACET_VALUE_TYPEObjectFromString(EDataType eDataType, String initialValue) {
		return createFACET_VALUE_TYPEFromString(CorePackage.Literals.FACET_VALUE_TYPE, initialValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertFACET_VALUE_TYPEObjectToString(EDataType eDataType, Object instanceValue) {
		return convertFACET_VALUE_TYPEToString(CorePackage.Literals.FACET_VALUE_TYPE, instanceValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CorePackage getCorePackage() {
		return (CorePackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static CorePackage getPackage() {
		return CorePackage.eINSTANCE;
	}

} //CoreFactoryImpl
