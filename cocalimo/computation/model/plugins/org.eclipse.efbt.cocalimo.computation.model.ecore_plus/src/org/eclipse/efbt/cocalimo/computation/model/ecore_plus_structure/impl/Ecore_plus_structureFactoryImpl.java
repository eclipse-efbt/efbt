/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.*;

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
public class Ecore_plus_structureFactoryImpl extends EFactoryImpl implements Ecore_plus_structureFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Ecore_plus_structureFactory init() {
		try {
			Ecore_plus_structureFactory theEcore_plus_structureFactory = (Ecore_plus_structureFactory)EPackage.Registry.INSTANCE.getEFactory(Ecore_plus_structurePackage.eNS_URI);
			if (theEcore_plus_structureFactory != null) {
				return theEcore_plus_structureFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Ecore_plus_structureFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_structureFactoryImpl() {
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
			case Ecore_plus_structurePackage.BASE_ENTITY: return createBaseEntity();
			case Ecore_plus_structurePackage.CELL: return createCell();
			case Ecore_plus_structurePackage.ENTITY: return createEntity();
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE: return createBaseEntityFeature();
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE: return createBaseEntityModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntity createBaseEntity() {
		BaseEntityImpl baseEntity = new BaseEntityImpl();
		return baseEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Cell createCell() {
		CellImpl cell = new CellImpl();
		return cell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Entity createEntity() {
		EntityImpl entity = new EntityImpl();
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntityFeature createBaseEntityFeature() {
		BaseEntityFeatureImpl baseEntityFeature = new BaseEntityFeatureImpl();
		return baseEntityFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntityModule createBaseEntityModule() {
		BaseEntityModuleImpl baseEntityModule = new BaseEntityModuleImpl();
		return baseEntityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_structurePackage getEcore_plus_structurePackage() {
		return (Ecore_plus_structurePackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Ecore_plus_structurePackage getPackage() {
		return Ecore_plus_structurePackage.eINSTANCE;
	}

} //Ecore_plus_structureFactoryImpl
