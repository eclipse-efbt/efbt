/**
 */
package org.eclipse.efbt.openregspecs.model.data_meta_model.impl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.*;

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
			case Data_meta_modelPackage.CONCEPT: return createConcept();
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE: return createRelationshipAttribute();
			case Data_meta_modelPackage.ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE: return createOneToOneRelationshipAttribute();
			case Data_meta_modelPackage.MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE: return createManyToOneRelationshipAttribute();
			case Data_meta_modelPackage.ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE: return createOneToManyRelationshipAttribute();
			case Data_meta_modelPackage.MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE: return createManyToManyRelationshipAttribute();
			case Data_meta_modelPackage.ENTITY_MODULE: return createEntityModule();
			case Data_meta_modelPackage.GENERATED_ENTITY: return createGeneratedEntity();
			case Data_meta_modelPackage.DERIVED_ENTITY: return createDerivedEntity();
			case Data_meta_modelPackage.BASIC_ENTITY: return createBasicEntity();
			case Data_meta_modelPackage.ENUM_MEMBER: return createEnumMember();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute createAttribute() {
		AttributeImpl attribute = new AttributeImpl();
		return attribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Concept createConcept() {
		ConceptImpl concept = new ConceptImpl();
		return concept;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RelationshipAttribute createRelationshipAttribute() {
		RelationshipAttributeImpl relationshipAttribute = new RelationshipAttributeImpl();
		return relationshipAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OneToOneRelationshipAttribute createOneToOneRelationshipAttribute() {
		OneToOneRelationshipAttributeImpl oneToOneRelationshipAttribute = new OneToOneRelationshipAttributeImpl();
		return oneToOneRelationshipAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ManyToOneRelationshipAttribute createManyToOneRelationshipAttribute() {
		ManyToOneRelationshipAttributeImpl manyToOneRelationshipAttribute = new ManyToOneRelationshipAttributeImpl();
		return manyToOneRelationshipAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OneToManyRelationshipAttribute createOneToManyRelationshipAttribute() {
		OneToManyRelationshipAttributeImpl oneToManyRelationshipAttribute = new OneToManyRelationshipAttributeImpl();
		return oneToManyRelationshipAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ManyToManyRelationshipAttribute createManyToManyRelationshipAttribute() {
		ManyToManyRelationshipAttributeImpl manyToManyRelationshipAttribute = new ManyToManyRelationshipAttributeImpl();
		return manyToManyRelationshipAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityModule createEntityModule() {
		EntityModuleImpl entityModule = new EntityModuleImpl();
		return entityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntity createGeneratedEntity() {
		GeneratedEntityImpl generatedEntity = new GeneratedEntityImpl();
		return generatedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntity createDerivedEntity() {
		DerivedEntityImpl derivedEntity = new DerivedEntityImpl();
		return derivedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicEntity createBasicEntity() {
		BasicEntityImpl basicEntity = new BasicEntityImpl();
		return basicEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EnumMember createEnumMember() {
		EnumMemberImpl enumMember = new EnumMemberImpl();
		return enumMember;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
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
