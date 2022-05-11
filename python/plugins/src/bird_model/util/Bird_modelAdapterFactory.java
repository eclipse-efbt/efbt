/**
 */
package bird_model.util;

import bird_model.Attribute;
import bird_model.BIRDModel;
import bird_model.BasicEntity;
import bird_model.Bird_modelPackage;
import bird_model.Classifier;
import bird_model.DOMAIN;
import bird_model.DerivedEntity;
import bird_model.DomainModule;
import bird_model.Element;
import bird_model.Entity;
import bird_model.EntityModule;
import bird_model.FACET_COLLECTION;
import bird_model.FACET_ENUMERATION;
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
import bird_model.NamedElement;
import bird_model.OneToManyRelationshipAttribute;
import bird_model.OneToOneRelationshipAttribute;
import bird_model.RelationshipAttribute;
import bird_model.SMCubesCoreModel;
import bird_model.SUBDOMAIN;
import bird_model.SUBDOMAIN_ENUMERATION;
import bird_model.StructuralFeature;
import bird_model.SubDomainModule;
import bird_model.TypedElement;
import bird_model.VARIABLE;
import bird_model.VARIABLE_SET;
import bird_model.VARIABLE_SET_ENUMERATION;
import bird_model.VariableModule;
import bird_model.facet_type;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see bird_model.Bird_modelPackage
 * @generated
 */
public class Bird_modelAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Bird_modelPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Bird_modelAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Bird_modelPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Bird_modelSwitch<Adapter> modelSwitch =
		new Bird_modelSwitch<Adapter>() {
			@Override
			public Adapter caseDomainModule(DomainModule object) {
				return createDomainModuleAdapter();
			}
			@Override
			public Adapter caseMemberHierarchyModule(MemberHierarchyModule object) {
				return createMemberHierarchyModuleAdapter();
			}
			@Override
			public Adapter caseMemberModule(MemberModule object) {
				return createMemberModuleAdapter();
			}
			@Override
			public Adapter caseVariableModule(VariableModule object) {
				return createVariableModuleAdapter();
			}
			@Override
			public Adapter caseSubDomainModule(SubDomainModule object) {
				return createSubDomainModuleAdapter();
			}
			@Override
			public Adapter caseSMCubesCoreModel(SMCubesCoreModel object) {
				return createSMCubesCoreModelAdapter();
			}
			@Override
			public Adapter caseDOMAIN(DOMAIN object) {
				return createDOMAINAdapter();
			}
			@Override
			public Adapter caseFACET_COLLECTION(FACET_COLLECTION object) {
				return createFACET_COLLECTIONAdapter();
			}
			@Override
			public Adapter caseFACET_ENUMERATION(FACET_ENUMERATION object) {
				return createFACET_ENUMERATIONAdapter();
			}
			@Override
			public Adapter casefacet_type(facet_type object) {
				return createfacet_typeAdapter();
			}
			@Override
			public Adapter caseMAINTENANCE_AGENCY(MAINTENANCE_AGENCY object) {
				return createMAINTENANCE_AGENCYAdapter();
			}
			@Override
			public Adapter caseMEMBER(MEMBER object) {
				return createMEMBERAdapter();
			}
			@Override
			public Adapter caseMEMBER_HIERARCHY(MEMBER_HIERARCHY object) {
				return createMEMBER_HIERARCHYAdapter();
			}
			@Override
			public Adapter caseMEMBER_HIERARCHY_NODE(MEMBER_HIERARCHY_NODE object) {
				return createMEMBER_HIERARCHY_NODEAdapter();
			}
			@Override
			public Adapter caseSUBDOMAIN(SUBDOMAIN object) {
				return createSUBDOMAINAdapter();
			}
			@Override
			public Adapter caseSUBDOMAIN_ENUMERATION(SUBDOMAIN_ENUMERATION object) {
				return createSUBDOMAIN_ENUMERATIONAdapter();
			}
			@Override
			public Adapter caseVARIABLE(VARIABLE object) {
				return createVARIABLEAdapter();
			}
			@Override
			public Adapter caseVARIABLE_SET(VARIABLE_SET object) {
				return createVARIABLE_SETAdapter();
			}
			@Override
			public Adapter caseVARIABLE_SET_ENUMERATION(VARIABLE_SET_ENUMERATION object) {
				return createVARIABLE_SET_ENUMERATIONAdapter();
			}
			@Override
			public Adapter caseAttribute(Attribute object) {
				return createAttributeAdapter();
			}
			@Override
			public Adapter caseRelationshipAttribute(RelationshipAttribute object) {
				return createRelationshipAttributeAdapter();
			}
			@Override
			public Adapter caseOneToOneRelationshipAttribute(OneToOneRelationshipAttribute object) {
				return createOneToOneRelationshipAttributeAdapter();
			}
			@Override
			public Adapter caseManyToOneRelationshipAttribute(ManyToOneRelationshipAttribute object) {
				return createManyToOneRelationshipAttributeAdapter();
			}
			@Override
			public Adapter caseOneToManyRelationshipAttribute(OneToManyRelationshipAttribute object) {
				return createOneToManyRelationshipAttributeAdapter();
			}
			@Override
			public Adapter caseManyToManyRelationshipAttribute(ManyToManyRelationshipAttribute object) {
				return createManyToManyRelationshipAttributeAdapter();
			}
			@Override
			public Adapter caseClassifier(Classifier object) {
				return createClassifierAdapter();
			}
			@Override
			public Adapter caseElement(Element object) {
				return createElementAdapter();
			}
			@Override
			public Adapter caseNamedElement(NamedElement object) {
				return createNamedElementAdapter();
			}
			@Override
			public Adapter caseEntityModule(EntityModule object) {
				return createEntityModuleAdapter();
			}
			@Override
			public Adapter caseStructuralFeature(StructuralFeature object) {
				return createStructuralFeatureAdapter();
			}
			@Override
			public Adapter caseTypedElement(TypedElement object) {
				return createTypedElementAdapter();
			}
			@Override
			public Adapter caseGeneratedEntity(GeneratedEntity object) {
				return createGeneratedEntityAdapter();
			}
			@Override
			public Adapter caseEntity(Entity object) {
				return createEntityAdapter();
			}
			@Override
			public Adapter caseDerivedEntity(DerivedEntity object) {
				return createDerivedEntityAdapter();
			}
			@Override
			public Adapter caseBasicEntity(BasicEntity object) {
				return createBasicEntityAdapter();
			}
			@Override
			public Adapter caseModule(bird_model.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter caseModuleLongName(ModuleLongName object) {
				return createModuleLongNameAdapter();
			}
			@Override
			public Adapter caseModuleDependencies(ModuleDependencies object) {
				return createModuleDependenciesAdapter();
			}
			@Override
			public Adapter caseModuleDependency(ModuleDependency object) {
				return createModuleDependencyAdapter();
			}
			@Override
			public Adapter caseBIRDModel(BIRDModel object) {
				return createBIRDModelAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link bird_model.DomainModule <em>Domain Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.DomainModule
	 * @generated
	 */
	public Adapter createDomainModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.MemberHierarchyModule <em>Member Hierarchy Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.MemberHierarchyModule
	 * @generated
	 */
	public Adapter createMemberHierarchyModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.MemberModule <em>Member Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.MemberModule
	 * @generated
	 */
	public Adapter createMemberModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.VariableModule <em>Variable Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.VariableModule
	 * @generated
	 */
	public Adapter createVariableModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.SubDomainModule <em>Sub Domain Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.SubDomainModule
	 * @generated
	 */
	public Adapter createSubDomainModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.SMCubesCoreModel <em>SM Cubes Core Model</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.SMCubesCoreModel
	 * @generated
	 */
	public Adapter createSMCubesCoreModelAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.DOMAIN <em>DOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.DOMAIN
	 * @generated
	 */
	public Adapter createDOMAINAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.FACET_COLLECTION <em>FACET COLLECTION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.FACET_COLLECTION
	 * @generated
	 */
	public Adapter createFACET_COLLECTIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.FACET_ENUMERATION <em>FACET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.FACET_ENUMERATION
	 * @generated
	 */
	public Adapter createFACET_ENUMERATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.facet_type <em>facet type</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.facet_type
	 * @generated
	 */
	public Adapter createfacet_typeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.MAINTENANCE_AGENCY <em>MAINTENANCE AGENCY</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.MAINTENANCE_AGENCY
	 * @generated
	 */
	public Adapter createMAINTENANCE_AGENCYAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.MEMBER <em>MEMBER</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.MEMBER
	 * @generated
	 */
	public Adapter createMEMBERAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.MEMBER_HIERARCHY <em>MEMBER HIERARCHY</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.MEMBER_HIERARCHY
	 * @generated
	 */
	public Adapter createMEMBER_HIERARCHYAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.MEMBER_HIERARCHY_NODE <em>MEMBER HIERARCHY NODE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.MEMBER_HIERARCHY_NODE
	 * @generated
	 */
	public Adapter createMEMBER_HIERARCHY_NODEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.SUBDOMAIN <em>SUBDOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.SUBDOMAIN
	 * @generated
	 */
	public Adapter createSUBDOMAINAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.SUBDOMAIN_ENUMERATION <em>SUBDOMAIN ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.SUBDOMAIN_ENUMERATION
	 * @generated
	 */
	public Adapter createSUBDOMAIN_ENUMERATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.VARIABLE <em>VARIABLE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.VARIABLE
	 * @generated
	 */
	public Adapter createVARIABLEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.VARIABLE_SET <em>VARIABLE SET</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.VARIABLE_SET
	 * @generated
	 */
	public Adapter createVARIABLE_SETAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.VARIABLE_SET_ENUMERATION <em>VARIABLE SET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.VARIABLE_SET_ENUMERATION
	 * @generated
	 */
	public Adapter createVARIABLE_SET_ENUMERATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.Attribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.Attribute
	 * @generated
	 */
	public Adapter createAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.RelationshipAttribute <em>Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.RelationshipAttribute
	 * @generated
	 */
	public Adapter createRelationshipAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.OneToOneRelationshipAttribute <em>One To One Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.OneToOneRelationshipAttribute
	 * @generated
	 */
	public Adapter createOneToOneRelationshipAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.ManyToOneRelationshipAttribute <em>Many To One Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.ManyToOneRelationshipAttribute
	 * @generated
	 */
	public Adapter createManyToOneRelationshipAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.OneToManyRelationshipAttribute <em>One To Many Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.OneToManyRelationshipAttribute
	 * @generated
	 */
	public Adapter createOneToManyRelationshipAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.ManyToManyRelationshipAttribute <em>Many To Many Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.ManyToManyRelationshipAttribute
	 * @generated
	 */
	public Adapter createManyToManyRelationshipAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.Classifier <em>Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.Classifier
	 * @generated
	 */
	public Adapter createClassifierAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.Element <em>Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.Element
	 * @generated
	 */
	public Adapter createElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.NamedElement <em>Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.NamedElement
	 * @generated
	 */
	public Adapter createNamedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.EntityModule <em>Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.EntityModule
	 * @generated
	 */
	public Adapter createEntityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.StructuralFeature <em>Structural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.StructuralFeature
	 * @generated
	 */
	public Adapter createStructuralFeatureAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.TypedElement <em>Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.TypedElement
	 * @generated
	 */
	public Adapter createTypedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.GeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.GeneratedEntity
	 * @generated
	 */
	public Adapter createGeneratedEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.Entity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.Entity
	 * @generated
	 */
	public Adapter createEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.DerivedEntity <em>Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.DerivedEntity
	 * @generated
	 */
	public Adapter createDerivedEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.BasicEntity <em>Basic Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.BasicEntity
	 * @generated
	 */
	public Adapter createBasicEntityAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.Module
	 * @generated
	 */
	public Adapter createModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.ModuleLongName <em>Module Long Name</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.ModuleLongName
	 * @generated
	 */
	public Adapter createModuleLongNameAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.ModuleDependencies <em>Module Dependencies</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.ModuleDependencies
	 * @generated
	 */
	public Adapter createModuleDependenciesAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.ModuleDependency <em>Module Dependency</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.ModuleDependency
	 * @generated
	 */
	public Adapter createModuleDependencyAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link bird_model.BIRDModel <em>BIRD Model</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see bird_model.BIRDModel
	 * @generated
	 */
	public Adapter createBIRDModelAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //Bird_modelAdapterFactory
