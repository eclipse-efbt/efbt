/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.impl;

import org.eclipse.efbt.cocalimo.core.model.platform_call.*;

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
public class Platform_callFactoryImpl extends EFactoryImpl implements Platform_callFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Platform_callFactory init() {
		try {
			Platform_callFactory thePlatform_callFactory = (Platform_callFactory)EPackage.Registry.INSTANCE.getEFactory(Platform_callPackage.eNS_URI);
			if (thePlatform_callFactory != null) {
				return thePlatform_callFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Platform_callFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Platform_callFactoryImpl() {
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
			case Platform_callPackage.PLATFORM_CALL: return createPlatformCall();
			case Platform_callPackage.PLATFORM_CALL_MODULE: return createPlatformCallModule();
			case Platform_callPackage.TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON: return createTranslateBIRDWithOldTestFormatToCocason();
			case Platform_callPackage.CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE: return createConvertSQLDeveloperModelToEcore();
			case Platform_callPackage.TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON: return createTranslateBIRDWithNewTestFormatToCocason();
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
	public PlatformCall createPlatformCall() {
		PlatformCallImpl platformCall = new PlatformCallImpl();
		return platformCall;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public PlatformCallModule createPlatformCallModule() {
		PlatformCallModuleImpl platformCallModule = new PlatformCallModuleImpl();
		return platformCallModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TranslateBIRDWithOldTestFormatToCocason createTranslateBIRDWithOldTestFormatToCocason() {
		TranslateBIRDWithOldTestFormatToCocasonImpl translateBIRDWithOldTestFormatToCocason = new TranslateBIRDWithOldTestFormatToCocasonImpl();
		return translateBIRDWithOldTestFormatToCocason;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ConvertSQLDeveloperModelToEcore createConvertSQLDeveloperModelToEcore() {
		ConvertSQLDeveloperModelToEcoreImpl convertSQLDeveloperModelToEcore = new ConvertSQLDeveloperModelToEcoreImpl();
		return convertSQLDeveloperModelToEcore;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TranslateBIRDWithNewTestFormatToCocason createTranslateBIRDWithNewTestFormatToCocason() {
		TranslateBIRDWithNewTestFormatToCocasonImpl translateBIRDWithNewTestFormatToCocason = new TranslateBIRDWithNewTestFormatToCocasonImpl();
		return translateBIRDWithNewTestFormatToCocason;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Platform_callPackage getPlatform_callPackage() {
		return (Platform_callPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Platform_callPackage getPackage() {
		return Platform_callPackage.eINSTANCE;
	}

} //Platform_callFactoryImpl
