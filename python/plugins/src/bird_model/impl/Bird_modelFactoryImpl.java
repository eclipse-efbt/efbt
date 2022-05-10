/**
 */
package bird_model.impl;

import bird_model.Attribute;
import bird_model.BIRDModel;
import bird_model.BasicEntity;
import bird_model.Bird_modelFactory;
import bird_model.Bird_modelPackage;
import bird_model.DOMAIN;
import bird_model.DerivedEntity;
import bird_model.DomainModule;
import bird_model.EntityModule;
import bird_model.FACET_COLLECTION;
import bird_model.FACET_ENUMERATION;
import bird_model.FACET_VALUE_TYPE;
import bird_model.GeneratedEntity;
import bird_model.MAINTENANCE_AGENCY;
import bird_model.MEMBER;
import bird_model.MEMBER_HIERARCHY;
import bird_model.MEMBER_HIERARCHY_NODE;
import bird_model.ManyToManyRelationshipAttribute;
import bird_model.ManyToOneRelationshipAttribute;
import bird_model.MemberHierarchyModule;
import bird_model.MemberModule;
import bird_model.ModuleDependencies;
import bird_model.ModuleDependency;
import bird_model.ModuleLongName;
import bird_model.OneToManyRelationshipAttribute;
import bird_model.OneToOneRelationshipAttribute;
import bird_model.RelationshipAttribute;
import bird_model.SMCubesCoreModel;
import bird_model.SUBDOMAIN;
import bird_model.SUBDOMAIN_ENUMERATION;
import bird_model.SubDomainModule;
import bird_model.VARIABLE;
import bird_model.VARIABLE_SET;
import bird_model.VARIABLE_SET_ENUMERATION;
import bird_model.VariableModule;
import bird_model.facet_type;

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
public class Bird_modelFactoryImpl extends EFactoryImpl implements Bird_modelFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Bird_modelFactory init() {
		try {
			Bird_modelFactory theBird_modelFactory = (Bird_modelFactory)EPackage.Registry.INSTANCE.getEFactory(Bird_modelPackage.eNS_URI);
			if (theBird_modelFactory != null) {
				return theBird_modelFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Bird_modelFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Bird_modelFactoryImpl() {
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
			case Bird_modelPackage.DOMAIN_MODULE: return createDomainModule();
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE: return createMemberHierarchyModule();
			case Bird_modelPackage.MEMBER_MODULE: return createMemberModule();
			case Bird_modelPackage.VARIABLE_MODULE: return createVariableModule();
			case Bird_modelPackage.SUB_DOMAIN_MODULE: return createSubDomainModule();
			case Bird_modelPackage.SM_CUBES_CORE_MODEL: return createSMCubesCoreModel();
			case Bird_modelPackage.DOMAIN: return createDOMAIN();
			case Bird_modelPackage.FACET_COLLECTION: return createFACET_COLLECTION();
			case Bird_modelPackage.FACET_ENUMERATION: return createFACET_ENUMERATION();
			case Bird_modelPackage.FACET_TYPE: return createfacet_type();
			case Bird_modelPackage.MAINTENANCE_AGENCY: return createMAINTENANCE_AGENCY();
			case Bird_modelPackage.MEMBER: return createMEMBER();
			case Bird_modelPackage.MEMBER_HIERARCHY: return createMEMBER_HIERARCHY();
			case Bird_modelPackage.MEMBER_HIERARCHY_NODE: return createMEMBER_HIERARCHY_NODE();
			case Bird_modelPackage.SUBDOMAIN: return createSUBDOMAIN();
			case Bird_modelPackage.SUBDOMAIN_ENUMERATION: return createSUBDOMAIN_ENUMERATION();
			case Bird_modelPackage.VARIABLE: return createVARIABLE();
			case Bird_modelPackage.VARIABLE_SET: return createVARIABLE_SET();
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION: return createVARIABLE_SET_ENUMERATION();
			case Bird_modelPackage.ATTRIBUTE: return createAttribute();
			case Bird_modelPackage.RELATIONSHIP_ATTRIBUTE: return createRelationshipAttribute();
			case Bird_modelPackage.ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE: return createOneToOneRelationshipAttribute();
			case Bird_modelPackage.MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE: return createManyToOneRelationshipAttribute();
			case Bird_modelPackage.ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE: return createOneToManyRelationshipAttribute();
			case Bird_modelPackage.MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE: return createManyToManyRelationshipAttribute();
			case Bird_modelPackage.ENTITY_MODULE: return createEntityModule();
			case Bird_modelPackage.GENERATED_ENTITY: return createGeneratedEntity();
			case Bird_modelPackage.DERIVED_ENTITY: return createDerivedEntity();
			case Bird_modelPackage.BASIC_ENTITY: return createBasicEntity();
			case Bird_modelPackage.MODULE: return createModule();
			case Bird_modelPackage.MODULE_LONG_NAME: return createModuleLongName();
			case Bird_modelPackage.MODULE_DEPENDENCIES: return createModuleDependencies();
			case Bird_modelPackage.MODULE_DEPENDENCY: return createModuleDependency();
			case Bird_modelPackage.BIRD_MODEL: return createBIRDModel();
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
			case Bird_modelPackage.FACET_VALUE_TYPE:
				return createFACET_VALUE_TYPEFromString(eDataType, initialValue);
			case Bird_modelPackage.FACET_VALUE_TYPE_OBJECT:
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
			case Bird_modelPackage.FACET_VALUE_TYPE:
				return convertFACET_VALUE_TYPEToString(eDataType, instanceValue);
			case Bird_modelPackage.FACET_VALUE_TYPE_OBJECT:
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
	public DomainModule createDomainModule() {
		DomainModuleImpl domainModule = new DomainModuleImpl();
		return domainModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberHierarchyModule createMemberHierarchyModule() {
		MemberHierarchyModuleImpl memberHierarchyModule = new MemberHierarchyModuleImpl();
		return memberHierarchyModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberModule createMemberModule() {
		MemberModuleImpl memberModule = new MemberModuleImpl();
		return memberModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VariableModule createVariableModule() {
		VariableModuleImpl variableModule = new VariableModuleImpl();
		return variableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SubDomainModule createSubDomainModule() {
		SubDomainModuleImpl subDomainModule = new SubDomainModuleImpl();
		return subDomainModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesCoreModel createSMCubesCoreModel() {
		SMCubesCoreModelImpl smCubesCoreModel = new SMCubesCoreModelImpl();
		return smCubesCoreModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DOMAIN createDOMAIN() {
		DOMAINImpl domain = new DOMAINImpl();
		return domain;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_COLLECTION createFACET_COLLECTION() {
		FACET_COLLECTIONImpl faceT_COLLECTION = new FACET_COLLECTIONImpl();
		return faceT_COLLECTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_ENUMERATION createFACET_ENUMERATION() {
		FACET_ENUMERATIONImpl faceT_ENUMERATION = new FACET_ENUMERATIONImpl();
		return faceT_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public facet_type createfacet_type() {
		facet_typeImpl facet_type = new facet_typeImpl();
		return facet_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAINTENANCE_AGENCY createMAINTENANCE_AGENCY() {
		MAINTENANCE_AGENCYImpl maintenancE_AGENCY = new MAINTENANCE_AGENCYImpl();
		return maintenancE_AGENCY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER createMEMBER() {
		MEMBERImpl member = new MEMBERImpl();
		return member;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER_HIERARCHY createMEMBER_HIERARCHY() {
		MEMBER_HIERARCHYImpl membeR_HIERARCHY = new MEMBER_HIERARCHYImpl();
		return membeR_HIERARCHY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER_HIERARCHY_NODE createMEMBER_HIERARCHY_NODE() {
		MEMBER_HIERARCHY_NODEImpl membeR_HIERARCHY_NODE = new MEMBER_HIERARCHY_NODEImpl();
		return membeR_HIERARCHY_NODE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN createSUBDOMAIN() {
		SUBDOMAINImpl subdomain = new SUBDOMAINImpl();
		return subdomain;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN_ENUMERATION createSUBDOMAIN_ENUMERATION() {
		SUBDOMAIN_ENUMERATIONImpl subdomaiN_ENUMERATION = new SUBDOMAIN_ENUMERATIONImpl();
		return subdomaiN_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE createVARIABLE() {
		VARIABLEImpl variable = new VARIABLEImpl();
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE_SET createVARIABLE_SET() {
		VARIABLE_SETImpl variablE_SET = new VARIABLE_SETImpl();
		return variablE_SET;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE_SET_ENUMERATION createVARIABLE_SET_ENUMERATION() {
		VARIABLE_SET_ENUMERATIONImpl variablE_SET_ENUMERATION = new VARIABLE_SET_ENUMERATIONImpl();
		return variablE_SET_ENUMERATION;
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
	public bird_model.Module createModule() {
		ModuleImpl module = new ModuleImpl();
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleLongName createModuleLongName() {
		ModuleLongNameImpl moduleLongName = new ModuleLongNameImpl();
		return moduleLongName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleDependencies createModuleDependencies() {
		ModuleDependenciesImpl moduleDependencies = new ModuleDependenciesImpl();
		return moduleDependencies;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleDependency createModuleDependency() {
		ModuleDependencyImpl moduleDependency = new ModuleDependencyImpl();
		return moduleDependency;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BIRDModel createBIRDModel() {
		BIRDModelImpl birdModel = new BIRDModelImpl();
		return birdModel;
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
		return createFACET_VALUE_TYPEFromString(Bird_modelPackage.Literals.FACET_VALUE_TYPE, initialValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertFACET_VALUE_TYPEObjectToString(EDataType eDataType, Object instanceValue) {
		return convertFACET_VALUE_TYPEToString(Bird_modelPackage.Literals.FACET_VALUE_TYPE, instanceValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Bird_modelPackage getBird_modelPackage() {
		return (Bird_modelPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Bird_modelPackage getPackage() {
		return Bird_modelPackage.eINSTANCE;
	}

} //Bird_modelFactoryImpl
