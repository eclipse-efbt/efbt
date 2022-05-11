/**
 */
package bird_model;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see bird_model.Bird_modelPackage
 * @generated
 */
public interface Bird_modelFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Bird_modelFactory eINSTANCE = bird_model.impl.Bird_modelFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Domain Module</em>'.
	 * @generated
	 */
	DomainModule createDomainModule();

	/**
	 * Returns a new object of class '<em>Member Hierarchy Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Member Hierarchy Module</em>'.
	 * @generated
	 */
	MemberHierarchyModule createMemberHierarchyModule();

	/**
	 * Returns a new object of class '<em>Member Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Member Module</em>'.
	 * @generated
	 */
	MemberModule createMemberModule();

	/**
	 * Returns a new object of class '<em>Variable Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Variable Module</em>'.
	 * @generated
	 */
	VariableModule createVariableModule();

	/**
	 * Returns a new object of class '<em>Sub Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Sub Domain Module</em>'.
	 * @generated
	 */
	SubDomainModule createSubDomainModule();

	/**
	 * Returns a new object of class '<em>SM Cubes Core Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SM Cubes Core Model</em>'.
	 * @generated
	 */
	SMCubesCoreModel createSMCubesCoreModel();

	/**
	 * Returns a new object of class '<em>DOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>DOMAIN</em>'.
	 * @generated
	 */
	DOMAIN createDOMAIN();

	/**
	 * Returns a new object of class '<em>FACET COLLECTION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>FACET COLLECTION</em>'.
	 * @generated
	 */
	FACET_COLLECTION createFACET_COLLECTION();

	/**
	 * Returns a new object of class '<em>FACET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>FACET ENUMERATION</em>'.
	 * @generated
	 */
	FACET_ENUMERATION createFACET_ENUMERATION();

	/**
	 * Returns a new object of class '<em>facet type</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>facet type</em>'.
	 * @generated
	 */
	facet_type createfacet_type();

	/**
	 * Returns a new object of class '<em>MAINTENANCE AGENCY</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MAINTENANCE AGENCY</em>'.
	 * @generated
	 */
	MAINTENANCE_AGENCY createMAINTENANCE_AGENCY();

	/**
	 * Returns a new object of class '<em>MEMBER</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER</em>'.
	 * @generated
	 */
	MEMBER createMEMBER();

	/**
	 * Returns a new object of class '<em>MEMBER HIERARCHY</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER HIERARCHY</em>'.
	 * @generated
	 */
	MEMBER_HIERARCHY createMEMBER_HIERARCHY();

	/**
	 * Returns a new object of class '<em>MEMBER HIERARCHY NODE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>MEMBER HIERARCHY NODE</em>'.
	 * @generated
	 */
	MEMBER_HIERARCHY_NODE createMEMBER_HIERARCHY_NODE();

	/**
	 * Returns a new object of class '<em>SUBDOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SUBDOMAIN</em>'.
	 * @generated
	 */
	SUBDOMAIN createSUBDOMAIN();

	/**
	 * Returns a new object of class '<em>SUBDOMAIN ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>SUBDOMAIN ENUMERATION</em>'.
	 * @generated
	 */
	SUBDOMAIN_ENUMERATION createSUBDOMAIN_ENUMERATION();

	/**
	 * Returns a new object of class '<em>VARIABLE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE</em>'.
	 * @generated
	 */
	VARIABLE createVARIABLE();

	/**
	 * Returns a new object of class '<em>VARIABLE SET</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE SET</em>'.
	 * @generated
	 */
	VARIABLE_SET createVARIABLE_SET();

	/**
	 * Returns a new object of class '<em>VARIABLE SET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>VARIABLE SET ENUMERATION</em>'.
	 * @generated
	 */
	VARIABLE_SET_ENUMERATION createVARIABLE_SET_ENUMERATION();

	/**
	 * Returns a new object of class '<em>Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Attribute</em>'.
	 * @generated
	 */
	Attribute createAttribute();

	/**
	 * Returns a new object of class '<em>Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Relationship Attribute</em>'.
	 * @generated
	 */
	RelationshipAttribute createRelationshipAttribute();

	/**
	 * Returns a new object of class '<em>One To One Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>One To One Relationship Attribute</em>'.
	 * @generated
	 */
	OneToOneRelationshipAttribute createOneToOneRelationshipAttribute();

	/**
	 * Returns a new object of class '<em>Many To One Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Many To One Relationship Attribute</em>'.
	 * @generated
	 */
	ManyToOneRelationshipAttribute createManyToOneRelationshipAttribute();

	/**
	 * Returns a new object of class '<em>One To Many Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>One To Many Relationship Attribute</em>'.
	 * @generated
	 */
	OneToManyRelationshipAttribute createOneToManyRelationshipAttribute();

	/**
	 * Returns a new object of class '<em>Many To Many Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Many To Many Relationship Attribute</em>'.
	 * @generated
	 */
	ManyToManyRelationshipAttribute createManyToManyRelationshipAttribute();

	/**
	 * Returns a new object of class '<em>Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Entity Module</em>'.
	 * @generated
	 */
	EntityModule createEntityModule();

	/**
	 * Returns a new object of class '<em>Generated Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Generated Entity</em>'.
	 * @generated
	 */
	GeneratedEntity createGeneratedEntity();

	/**
	 * Returns a new object of class '<em>Derived Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Derived Entity</em>'.
	 * @generated
	 */
	DerivedEntity createDerivedEntity();

	/**
	 * Returns a new object of class '<em>Basic Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Basic Entity</em>'.
	 * @generated
	 */
	BasicEntity createBasicEntity();

	/**
	 * Returns a new object of class '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module</em>'.
	 * @generated
	 */
	Module createModule();

	/**
	 * Returns a new object of class '<em>Module Long Name</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module Long Name</em>'.
	 * @generated
	 */
	ModuleLongName createModuleLongName();

	/**
	 * Returns a new object of class '<em>Module Dependencies</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module Dependencies</em>'.
	 * @generated
	 */
	ModuleDependencies createModuleDependencies();

	/**
	 * Returns a new object of class '<em>Module Dependency</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module Dependency</em>'.
	 * @generated
	 */
	ModuleDependency createModuleDependency();

	/**
	 * Returns a new object of class '<em>BIRD Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>BIRD Model</em>'.
	 * @generated
	 */
	BIRDModel createBIRDModel();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Bird_modelPackage getBird_modelPackage();

} //Bird_modelFactory
