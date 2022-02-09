/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

import org.eclipse.emf.ecore.xml.type.XMLTypeFactory;
import org.eclipse.emf.ecore.xml.type.XMLTypePackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Data_meta_modelFactoryImpl extends EFactoryImpl implements Data_meta_modelFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Data_meta_modelFactory init() {
		try {
			Data_meta_modelFactory theData_meta_modelFactory = (Data_meta_modelFactory)EPackage.Registry.INSTANCE.getEFactory(Data_meta_modelPackage.eNS_URI);
			if (theData_meta_modelFactory != null) {
				return theData_meta_modelFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Data_meta_modelFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_meta_modelFactoryImpl() {
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
			case Data_meta_modelPackage.ATTRIBUTE: return createAttribute();
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE: return createForeignKeyAttribute();
			case Data_meta_modelPackage.ENTITY_MODULE: return createEntityModule();
			case Data_meta_modelPackage.GENERATED_ENTITY: return createGeneratedEntity();
			case Data_meta_modelPackage.DERIVED_ENTITY: return createDerivedEntity();
			case Data_meta_modelPackage.BASIC_ENTITY: return createBasicEntity();
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
			case Data_meta_modelPackage.BOOLEAN:
				return createBooleanFromString(eDataType, initialValue);
			case Data_meta_modelPackage.INT:
				return createIntFromString(eDataType, initialValue);
			case Data_meta_modelPackage.STRING:
				return createStringFromString(eDataType, initialValue);
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
			case Data_meta_modelPackage.BOOLEAN:
				return convertBooleanToString(eDataType, instanceValue);
			case Data_meta_modelPackage.INT:
				return convertIntToString(eDataType, instanceValue);
			case Data_meta_modelPackage.STRING:
				return convertStringToString(eDataType, instanceValue);
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
	public Attribute createAttribute() {
		AttributeImpl attribute = new AttributeImpl();
		return attribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ForeignKeyAttribute createForeignKeyAttribute() {
		ForeignKeyAttributeImpl foreignKeyAttribute = new ForeignKeyAttributeImpl();
		return foreignKeyAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EntityModule createEntityModule() {
		EntityModuleImpl entityModule = new EntityModuleImpl();
		return entityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GeneratedEntity createGeneratedEntity() {
		GeneratedEntityImpl generatedEntity = new GeneratedEntityImpl();
		return generatedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DerivedEntity createDerivedEntity() {
		DerivedEntityImpl derivedEntity = new DerivedEntityImpl();
		return derivedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BasicEntity createBasicEntity() {
		BasicEntityImpl basicEntity = new BasicEntityImpl();
		return basicEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Object createBooleanFromString(EDataType eDataType, String initialValue) {
		return XMLTypeFactory.eINSTANCE.createFromString(XMLTypePackage.Literals.BOOLEAN, initialValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertBooleanToString(EDataType eDataType, Object instanceValue) {
		return XMLTypeFactory.eINSTANCE.convertToString(XMLTypePackage.Literals.BOOLEAN, instanceValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Object createIntFromString(EDataType eDataType, String initialValue) {
		return XMLTypeFactory.eINSTANCE.createFromString(XMLTypePackage.Literals.INT, initialValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertIntToString(EDataType eDataType, Object instanceValue) {
		return XMLTypeFactory.eINSTANCE.convertToString(XMLTypePackage.Literals.INT, instanceValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Object createStringFromString(EDataType eDataType, String initialValue) {
		return XMLTypeFactory.eINSTANCE.createFromString(XMLTypePackage.Literals.STRING, initialValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertStringToString(EDataType eDataType, Object instanceValue) {
		return XMLTypeFactory.eINSTANCE.convertToString(XMLTypePackage.Literals.STRING, instanceValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Data_meta_modelPackage getData_meta_modelPackage() {
		return (Data_meta_modelPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Data_meta_modelPackage getPackage() {
		return Data_meta_modelPackage.eINSTANCE;
	}

} //Data_meta_modelFactoryImpl
