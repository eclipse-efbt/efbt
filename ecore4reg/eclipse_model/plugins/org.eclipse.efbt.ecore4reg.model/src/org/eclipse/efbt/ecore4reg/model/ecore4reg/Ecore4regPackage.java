/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regFactory
 * @model kind="package"
 * @generated
 */
public interface Ecore4regPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "ecore4reg";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/ecore4reg";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "ecore4reg";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Ecore4regPackage eINSTANCE = org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ImportImpl <em>Import</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ImportImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getImport()
	 * @generated
	 */
	int IMPORT = 0;

	/**
	 * The feature id for the '<em><b>Imported Namespace</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT__IMPORTED_NAMESPACE = 0;

	/**
	 * The number of structural features of the '<em>Import</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Import</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ModuleImpl <em>Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getModule()
	 * @generated
	 */
	int MODULE = 1;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__DEPENDENCIES = 0;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__THE_DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__LICENSE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__NAME = 3;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__VERSION = 4;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__IMPORTS = 5;

	/**
	 * The number of structural features of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ModuleListImpl <em>Module List</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ModuleListImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getModuleList()
	 * @generated
	 */
	int MODULE_LIST = 2;

	/**
	 * The feature id for the '<em><b>Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LIST__MODULES = 0;

	/**
	 * The number of structural features of the '<em>Module List</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LIST_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Module List</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LIST_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.AllowedTypesImpl <em>Allowed Types</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.AllowedTypesImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getAllowedTypes()
	 * @generated
	 */
	int ALLOWED_TYPES = 3;

	/**
	 * The feature id for the '<em><b>Allowed Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ALLOWED_TYPES__ALLOWED_TYPES = 0;

	/**
	 * The number of structural features of the '<em>Allowed Types</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ALLOWED_TYPES_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Allowed Types</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ALLOWED_TYPES_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsModuleImpl <em>Requirements Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRequirementsModule()
	 * @generated
	 */
	int REQUIREMENTS_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>Rules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__RULES = MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Allowedtypes</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__ALLOWEDTYPES = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Requirements Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Requirements Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionImpl <em>Requirements Section</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRequirementsSection()
	 * @generated
	 */
	int REQUIREMENTS_SECTION = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION__NAME = 0;

	/**
	 * The number of structural features of the '<em>Requirements Section</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Requirements Section</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionImageImpl <em>Requirements Section Image</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionImageImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRequirementsSectionImage()
	 * @generated
	 */
	int REQUIREMENTS_SECTION_IMAGE = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_IMAGE__NAME = REQUIREMENTS_SECTION__NAME;

	/**
	 * The feature id for the '<em><b>Style</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_IMAGE__STYLE = REQUIREMENTS_SECTION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Uri</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_IMAGE__URI = REQUIREMENTS_SECTION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Requirements Section Image</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_IMAGE_FEATURE_COUNT = REQUIREMENTS_SECTION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Requirements Section Image</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_IMAGE_OPERATION_COUNT = REQUIREMENTS_SECTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionLinkWithTextImpl <em>Requirements Section Link With Text</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionLinkWithTextImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRequirementsSectionLinkWithText()
	 * @generated
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT__NAME = REQUIREMENTS_SECTION__NAME;

	/**
	 * The feature id for the '<em><b>Linked Rule Section</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION = REQUIREMENTS_SECTION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Link Text</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT = REQUIREMENTS_SECTION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Subsection</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION = REQUIREMENTS_SECTION_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Requirements Section Link With Text</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT_FEATURE_COUNT = REQUIREMENTS_SECTION_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Requirements Section Link With Text</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT_OPERATION_COUNT = REQUIREMENTS_SECTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionTextImpl <em>Requirements Section Text</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionTextImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRequirementsSectionText()
	 * @generated
	 */
	int REQUIREMENTS_SECTION_TEXT = 8;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_TEXT__NAME = REQUIREMENTS_SECTION__NAME;

	/**
	 * The feature id for the '<em><b>Text</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_TEXT__TEXT = REQUIREMENTS_SECTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Requirements Section Text</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_TEXT_FEATURE_COUNT = REQUIREMENTS_SECTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Requirements Section Text</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_SECTION_TEXT_OPERATION_COUNT = REQUIREMENTS_SECTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementTypeImpl <em>Requirement Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementTypeImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRequirementType()
	 * @generated
	 */
	int REQUIREMENT_TYPE = 9;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENT_TYPE__NAME = 0;

	/**
	 * The number of structural features of the '<em>Requirement Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENT_TYPE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Requirement Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENT_TYPE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TitledRequirementsSectionImpl <em>Titled Requirements Section</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TitledRequirementsSectionImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getTitledRequirementsSection()
	 * @generated
	 */
	int TITLED_REQUIREMENTS_SECTION = 10;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TITLED_REQUIREMENTS_SECTION__NAME = REQUIREMENTS_SECTION__NAME;

	/**
	 * The feature id for the '<em><b>Sections</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TITLED_REQUIREMENTS_SECTION__SECTIONS = REQUIREMENTS_SECTION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Referencing Sections</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS = REQUIREMENTS_SECTION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Requirements Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE = REQUIREMENTS_SECTION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Title</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TITLED_REQUIREMENTS_SECTION__TITLE = REQUIREMENTS_SECTION_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Titled Requirements Section</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TITLED_REQUIREMENTS_SECTION_FEATURE_COUNT = REQUIREMENTS_SECTION_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Titled Requirements Section</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TITLED_REQUIREMENTS_SECTION_OPERATION_COUNT = REQUIREMENTS_SECTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TagImpl <em>Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TagImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getTag()
	 * @generated
	 */
	int TAG = 11;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG__DISPLAY_NAME = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG__NAME = 1;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG__REQUIREMENTS = 2;

	/**
	 * The number of structural features of the '<em>Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TagGroupImpl <em>Tag Group</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TagGroupImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getTagGroup()
	 * @generated
	 */
	int TAG_GROUP = 12;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__TAGS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Tag Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Tag Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RulesForReportImpl <em>Rules For Report</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RulesForReportImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRulesForReport()
	 * @generated
	 */
	int RULES_FOR_REPORT = 13;

	/**
	 * The feature id for the '<em><b>Output Layer Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT__OUTPUT_LAYER_CUBE = 0;

	/**
	 * The feature id for the '<em><b>Rules For Table</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT__RULES_FOR_TABLE = 1;

	/**
	 * The number of structural features of the '<em>Rules For Report</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Rules For Report</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_REPORT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RulesForILTableImpl <em>Rules For IL Table</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RulesForILTableImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRulesForILTable()
	 * @generated
	 */
	int RULES_FOR_IL_TABLE = 14;

	/**
	 * The feature id for the '<em><b>Rules For Table Part</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE__RULES_FOR_TABLE_PART = 0;

	/**
	 * The feature id for the '<em><b>Input Layer Table</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE__INPUT_LAYER_TABLE = 1;

	/**
	 * The number of structural features of the '<em>Rules For IL Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Rules For IL Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULES_FOR_IL_TABLE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectColumnImpl <em>Select Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectColumnImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getSelectColumn()
	 * @generated
	 */
	int SELECT_COLUMN = 15;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN__AS_ATTRIBUTE = 0;

	/**
	 * The number of structural features of the '<em>Select Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Select Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectColumnMemberAsImpl <em>Select Column Member As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectColumnMemberAsImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getSelectColumnMemberAs()
	 * @generated
	 */
	int SELECT_COLUMN_MEMBER_AS = 16;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS__AS_ATTRIBUTE = SELECT_COLUMN__AS_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Member As Constant</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS__MEMBER_AS_CONSTANT = SELECT_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Select Column Member As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS_FEATURE_COUNT = SELECT_COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Select Column Member As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_MEMBER_AS_OPERATION_COUNT = SELECT_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectColumnAttributeAsImpl <em>Select Column Attribute As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectColumnAttributeAsImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getSelectColumnAttributeAs()
	 * @generated
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS = 17;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS__AS_ATTRIBUTE = SELECT_COLUMN__AS_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS__ATTRIBUTE = SELECT_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Select Column Attribute As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS_FEATURE_COUNT = SELECT_COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Select Column Attribute As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_COLUMN_ATTRIBUTE_AS_OPERATION_COUNT = SELECT_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectValueAsImpl <em>Select Value As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.SelectValueAsImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getSelectValueAs()
	 * @generated
	 */
	int SELECT_VALUE_AS = 18;

	/**
	 * The feature id for the '<em><b>As Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS__AS_ATTRIBUTE = SELECT_COLUMN__AS_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS__VALUE = SELECT_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Select Value As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS_FEATURE_COUNT = SELECT_COLUMN_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Select Value As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_VALUE_AS_OPERATION_COUNT = SELECT_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TableFilterImpl <em>Table Filter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.TableFilterImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getTableFilter()
	 * @generated
	 */
	int TABLE_FILTER = 19;

	/**
	 * The feature id for the '<em><b>Attribute1</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER__ATTRIBUTE1 = 0;

	/**
	 * The feature id for the '<em><b>Comparitor</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER__COMPARITOR = 1;

	/**
	 * The feature id for the '<em><b>Member</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER__MEMBER = 2;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER__VALUE = 3;

	/**
	 * The number of structural features of the '<em>Table Filter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Table Filter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FILTER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.GenerationRulesModuleImpl <em>Generation Rules Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.GenerationRulesModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getGenerationRulesModule()
	 * @generated
	 */
	int GENERATION_RULES_MODULE = 20;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>Rules For Report</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE__RULES_FOR_REPORT = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Generation Rules Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Generation Rules Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATION_RULES_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RuleForILTablePartImpl <em>Rule For IL Table Part</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RuleForILTablePartImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getRuleForILTablePart()
	 * @generated
	 */
	int RULE_FOR_IL_TABLE_PART = 21;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART__NAME = 0;

	/**
	 * The feature id for the '<em><b>Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART__COLUMNS = 1;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE = 2;

	/**
	 * The number of structural features of the '<em>Rule For IL Table Part</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Rule For IL Table Part</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RULE_FOR_IL_TABLE_PART_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELModelElementImpl <em>EL Model Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELModelElementImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELModelElement()
	 * @generated
	 */
	int EL_MODEL_ELEMENT = 28;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_MODEL_ELEMENT__EANNOTATIONS = 0;

	/**
	 * The number of structural features of the '<em>EL Model Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_MODEL_ELEMENT_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>EL Model Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_MODEL_ELEMENT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELNamedElementImpl <em>EL Named Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELNamedElementImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELNamedElement()
	 * @generated
	 */
	int EL_NAMED_ELEMENT = 29;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT__EANNOTATIONS = EL_MODEL_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT__NAME = EL_MODEL_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT_FEATURE_COUNT = EL_MODEL_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_NAMED_ELEMENT_OPERATION_COUNT = EL_MODEL_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELTypedElementImpl <em>EL Typed Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELTypedElementImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELTypedElement()
	 * @generated
	 */
	int EL_TYPED_ELEMENT = 34;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__EANNOTATIONS = EL_NAMED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__NAME = EL_NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__ETYPE = EL_NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__UPPER_BOUND = EL_NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT__LOWER_BOUND = EL_NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>EL Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT_FEATURE_COUNT = EL_NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>EL Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_TYPED_ELEMENT_OPERATION_COUNT = EL_NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELStructuralFeatureImpl <em>EL Structural Feature</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELStructuralFeatureImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELStructuralFeature()
	 * @generated
	 */
	int EL_STRUCTURAL_FEATURE = 33;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__EANNOTATIONS = EL_TYPED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__NAME = EL_TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__ETYPE = EL_TYPED_ELEMENT__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__UPPER_BOUND = EL_TYPED_ELEMENT__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE__LOWER_BOUND = EL_TYPED_ELEMENT__LOWER_BOUND;

	/**
	 * The number of structural features of the '<em>EL Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE_FEATURE_COUNT = EL_TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>EL Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRUCTURAL_FEATURE_OPERATION_COUNT = EL_TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELAttributeImpl <em>EL Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELAttributeImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELAttribute()
	 * @generated
	 */
	int EL_ATTRIBUTE = 22;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__EANNOTATIONS = EL_STRUCTURAL_FEATURE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__NAME = EL_STRUCTURAL_FEATURE__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__ETYPE = EL_STRUCTURAL_FEATURE__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__UPPER_BOUND = EL_STRUCTURAL_FEATURE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__LOWER_BOUND = EL_STRUCTURAL_FEATURE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__ID = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE__EATTRIBUTE_TYPE = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE_FEATURE_COUNT = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ATTRIBUTE_OPERATION_COUNT = EL_STRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassifierImpl <em>EL Classifier</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassifierImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELClassifier()
	 * @generated
	 */
	int EL_CLASSIFIER = 24;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER__EANNOTATIONS = EL_NAMED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER__NAME = EL_NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER__PACKAGE = EL_NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER_FEATURE_COUNT = EL_NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASSIFIER_OPERATION_COUNT = EL_NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassImpl <em>EL Class</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELClass()
	 * @generated
	 */
	int EL_CLASS = 23;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__EANNOTATIONS = EL_CLASSIFIER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__NAME = EL_CLASSIFIER__NAME;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__PACKAGE = EL_CLASSIFIER__PACKAGE;

	/**
	 * The feature id for the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__ABSTRACT = EL_CLASSIFIER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__ESUPER_TYPES = EL_CLASSIFIER_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__ESTRUCTURAL_FEATURES = EL_CLASSIFIER_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS__EOPERATIONS = EL_CLASSIFIER_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>EL Class</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS_FEATURE_COUNT = EL_CLASSIFIER_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>EL Class</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_CLASS_OPERATION_COUNT = EL_CLASSIFIER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELDataTypeImpl <em>EL Data Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELDataTypeImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELDataType()
	 * @generated
	 */
	int EL_DATA_TYPE = 25;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__EANNOTATIONS = EL_CLASSIFIER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__NAME = EL_CLASSIFIER__NAME;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__PACKAGE = EL_CLASSIFIER__PACKAGE;

	/**
	 * The feature id for the '<em><b>Industry Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE__INDUSTRY_NAME = EL_CLASSIFIER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Data Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE_FEATURE_COUNT = EL_CLASSIFIER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Data Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_DATA_TYPE_OPERATION_COUNT = EL_CLASSIFIER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELEnumImpl <em>EL Enum</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELEnumImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELEnum()
	 * @generated
	 */
	int EL_ENUM = 26;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__EANNOTATIONS = EL_DATA_TYPE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__NAME = EL_DATA_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__PACKAGE = EL_DATA_TYPE__PACKAGE;

	/**
	 * The feature id for the '<em><b>Industry Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__INDUSTRY_NAME = EL_DATA_TYPE__INDUSTRY_NAME;

	/**
	 * The feature id for the '<em><b>ELiterals</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM__ELITERALS = EL_DATA_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Enum</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_FEATURE_COUNT = EL_DATA_TYPE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Enum</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_OPERATION_COUNT = EL_DATA_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELEnumLiteralImpl <em>EL Enum Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELEnumLiteralImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELEnumLiteral()
	 * @generated
	 */
	int EL_ENUM_LITERAL = 27;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__EANNOTATIONS = EL_NAMED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__NAME = EL_NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__VALUE = EL_NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL__LITERAL = EL_NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Enum Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL_FEATURE_COUNT = EL_NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Enum Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ENUM_LITERAL_OPERATION_COUNT = EL_NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELOperationImpl <em>EL Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELOperationImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELOperation()
	 * @generated
	 */
	int EL_OPERATION = 30;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__EANNOTATIONS = EL_TYPED_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__NAME = EL_TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__ETYPE = EL_TYPED_ELEMENT__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__UPPER_BOUND = EL_TYPED_ELEMENT__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__LOWER_BOUND = EL_TYPED_ELEMENT__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Body</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION__BODY = EL_TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>EL Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION_FEATURE_COUNT = EL_TYPED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>EL Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_OPERATION_OPERATION_COUNT = EL_TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELPackageImpl <em>EL Package</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELPackageImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELPackage()
	 * @generated
	 */
	int EL_PACKAGE = 31;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>EClassifiers</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__ECLASSIFIERS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Ns URI</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__NS_URI = MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Ns Prefix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE__NS_PREFIX = MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>EL Package</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>EL Package</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_PACKAGE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELReferenceImpl <em>EL Reference</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELReferenceImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELReference()
	 * @generated
	 */
	int EL_REFERENCE = 32;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__EANNOTATIONS = EL_STRUCTURAL_FEATURE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__NAME = EL_STRUCTURAL_FEATURE__NAME;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__ETYPE = EL_STRUCTURAL_FEATURE__ETYPE;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__UPPER_BOUND = EL_STRUCTURAL_FEATURE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__LOWER_BOUND = EL_STRUCTURAL_FEATURE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__CONTAINMENT = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EReference Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE__EREFERENCE_TYPE = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Reference</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE_FEATURE_COUNT = EL_STRUCTURAL_FEATURE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Reference</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_REFERENCE_OPERATION_COUNT = EL_STRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELAnnotationImpl <em>EL Annotation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELAnnotationImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELAnnotation()
	 * @generated
	 */
	int EL_ANNOTATION = 35;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION__EANNOTATIONS = EL_MODEL_ELEMENT__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Details</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION__DETAILS = EL_MODEL_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Source</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION__SOURCE = EL_MODEL_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>EL Annotation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION_FEATURE_COUNT = EL_MODEL_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>EL Annotation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_ANNOTATION_OPERATION_COUNT = EL_MODEL_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELStringToStringMapEntryImpl <em>EL String To String Map Entry</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELStringToStringMapEntryImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getELStringToStringMapEntry()
	 * @generated
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY = 36;

	/**
	 * The feature id for the '<em><b>Key</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY__KEY = 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY__VALUE = 1;

	/**
	 * The number of structural features of the '<em>EL String To String Map Entry</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>EL String To String Map Entry</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EL_STRING_TO_STRING_MAP_ENTRY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLEnrichedCubeImpl <em>VTL Enriched Cube</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLEnrichedCubeImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLEnrichedCube()
	 * @generated
	 */
	int VTL_ENRICHED_CUBE = 37;

	/**
	 * The feature id for the '<em><b>Transformations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_ENRICHED_CUBE__TRANSFORMATIONS = 0;

	/**
	 * The number of structural features of the '<em>VTL Enriched Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_ENRICHED_CUBE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>VTL Enriched Cube</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_ENRICHED_CUBE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedOutputlayerImpl <em>VTL Generated Outputlayer</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedOutputlayerImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLGeneratedOutputlayer()
	 * @generated
	 */
	int VTL_GENERATED_OUTPUTLAYER = 38;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER__NAME = 0;

	/**
	 * The feature id for the '<em><b>Dependant intermediate layers</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER__DEPENDANT_INTERMEDIATE_LAYERS = 1;

	/**
	 * The feature id for the '<em><b>VTL For Output Layer And Intemedate Layer Combinations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER__VTL_FOR_OUTPUT_LAYER_AND_INTEMEDATE_LAYER_COMBINATIONS = 2;

	/**
	 * The feature id for the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER__OUTPUT_LAYER = 3;

	/**
	 * The number of structural features of the '<em>VTL Generated Outputlayer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>VTL Generated Outputlayer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationImpl <em>VTL For Output Layer And Intermediate Layer Combination</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLForOutputLayerAndIntermediateLayerCombination()
	 * @generated
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION = 39;

	/**
	 * The feature id for the '<em><b>Transformations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__TRANSFORMATIONS = 0;

	/**
	 * The feature id for the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__OUTPUT_LAYER = 1;

	/**
	 * The feature id for the '<em><b>Intermediate Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__INTERMEDIATE_LAYER = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION__NAME = 3;

	/**
	 * The number of structural features of the '<em>VTL For Output Layer And Intermediate Layer Combination</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>VTL For Output Layer And Intermediate Layer Combination</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerImpl <em>VTL Generated Intermediate Layer</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLGeneratedIntermediateLayer()
	 * @generated
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER = 40;

	/**
	 * The feature id for the '<em><b>Dependant enriched cubes</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER__DEPENDANT_ENRICHED_CUBES = 0;

	/**
	 * The feature id for the '<em><b>Transformations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER__TRANSFORMATIONS = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER__NAME = 2;

	/**
	 * The feature id for the '<em><b>Is Enrichment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER__IS_ENRICHMENT = 3;

	/**
	 * The number of structural features of the '<em>VTL Generated Intermediate Layer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>VTL Generated Intermediate Layer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLTransformationImpl <em>VTL Transformation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLTransformationImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLTransformation()
	 * @generated
	 */
	int VTL_TRANSFORMATION = 41;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_TRANSFORMATION__EXPRESSION = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_TRANSFORMATION__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Transformation id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_TRANSFORMATION__TRANSFORMATION_ID = 2;

	/**
	 * The feature id for the '<em><b>Order in scheme</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_TRANSFORMATION__ORDER_IN_SCHEME = 3;

	/**
	 * The number of structural features of the '<em>VTL Transformation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_TRANSFORMATION_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>VTL Transformation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_TRANSFORMATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLSchemeImpl <em>VTL Scheme</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLSchemeImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLScheme()
	 * @generated
	 */
	int VTL_SCHEME = 42;

	/**
	 * The feature id for the '<em><b>Expressions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEME__EXPRESSIONS = 0;

	/**
	 * The feature id for the '<em><b>Scheme id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEME__SCHEME_ID = 1;

	/**
	 * The number of structural features of the '<em>VTL Scheme</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEME_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>VTL Scheme</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEME_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerImpl <em>VTL For Selection Layer</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLForSelectionLayer()
	 * @generated
	 */
	int VTL_FOR_SELECTION_LAYER = 43;

	/**
	 * The feature id for the '<em><b>Selection Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER__SELECTION_LAYER = 0;

	/**
	 * The feature id for the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER__OUTPUT_LAYER = 1;

	/**
	 * The feature id for the '<em><b>Intermediate Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER__INTERMEDIATE_LAYER = 2;

	/**
	 * The number of structural features of the '<em>VTL For Selection Layer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>VTL For Selection Layer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl <em>VTL Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLModule()
	 * @generated
	 */
	int VTL_MODULE = 44;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>VTL Schemes</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__VTL_SCHEMES = MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>VTL Generated Output Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__VTL_GENERATED_OUTPUT_LAYERS = MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>VTL Generated Intermediate Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS = MODULE_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>VTL Enriched Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__VTL_ENRICHED_LAYERS = MODULE_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>VTL For Selection Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__VTL_FOR_SELECTION_LAYERS = MODULE_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Entity To VTL Intermediate Layer Links</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS = MODULE_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>VTL For Views</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE__VTL_FOR_VIEWS = MODULE_FEATURE_COUNT + 6;

	/**
	 * The number of structural features of the '<em>VTL Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 7;

	/**
	 * The number of operations of the '<em>VTL Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkImpl <em>Entity To VTL Intermediate Layer Link</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getEntityToVTLIntermediateLayerLink()
	 * @generated
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK = 45;

	/**
	 * The feature id for the '<em><b>VTL Intermediate Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__VTL_INTERMEDIATE_LAYER = 0;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__ENTITY = 1;

	/**
	 * The feature id for the '<em><b>Filter</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK__FILTER = 2;

	/**
	 * The number of structural features of the '<em>Entity To VTL Intermediate Layer Link</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Entity To VTL Intermediate Layer Link</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewImpl <em>VTL For View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLForView()
	 * @generated
	 */
	int VTL_FOR_VIEW = 46;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW__NAME = 0;

	/**
	 * The feature id for the '<em><b>View</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW__VIEW = 1;

	/**
	 * The feature id for the '<em><b>Vtl</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW__VTL = 2;

	/**
	 * The number of structural features of the '<em>VTL For View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>VTL For View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedOutputlayerModuleImpl <em>VTL Generated Outputlayer Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedOutputlayerModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLGeneratedOutputlayerModule()
	 * @generated
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE = 47;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>VTL Generated Outputlayer Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE__VTL_GENERATED_OUTPUTLAYER_MODULES = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>VTL Generated Outputlayer Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>VTL Generated Outputlayer Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_OUTPUTLAYER_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl <em>VTL For Output Layer And Intermediate Layer Combination Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLForOutputLayerAndIntermediateLayerCombinationModule()
	 * @generated
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE = 48;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>VTL For Output Layer And Intermediate Layer Combinations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE__VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATIONS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>VTL For Output Layer And Intermediate Layer Combination Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>VTL For Output Layer And Intermediate Layer Combination Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerModuleImpl <em>VTL Generated Intermediate Layer Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLGeneratedIntermediateLayerModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLGeneratedIntermediateLayerModule()
	 * @generated
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE = 49;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>VTL Generated Intermediate Layers</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE__VTL_GENERATED_INTERMEDIATE_LAYERS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>VTL Generated Intermediate Layer Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>VTL Generated Intermediate Layer Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_GENERATED_INTERMEDIATE_LAYER_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLSchemesModuleImpl <em>VTL Schemes Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLSchemesModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLSchemesModule()
	 * @generated
	 */
	int VTL_SCHEMES_MODULE = 50;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>VTL Schemes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE__VTL_SCHEMES = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>VTL Schemes Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>VTL Schemes Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_SCHEMES_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerModuleImpl <em>VTL For Selection Layer Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForSelectionLayerModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLForSelectionLayerModule()
	 * @generated
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE = 51;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>VTL For Selection Layers</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE__VTL_FOR_SELECTION_LAYERS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>VTL For Selection Layer Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>VTL For Selection Layer Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_SELECTION_LAYER_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkModuleImpl <em>Entity To VTL Intermediate Layer Link Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.EntityToVTLIntermediateLayerLinkModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getEntityToVTLIntermediateLayerLinkModule()
	 * @generated
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE = 52;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>Entity To VTL Intermediate Layer Links</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE__ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINKS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Entity To VTL Intermediate Layer Link Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Entity To VTL Intermediate Layer Link Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewModuleImpl <em>VTL For View Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewModuleImpl
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getVTLForViewModule()
	 * @generated
	 */
	int VTL_FOR_VIEW_MODULE = 53;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE__IMPORTS = MODULE__IMPORTS;

	/**
	 * The feature id for the '<em><b>VTL For Views</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE__VTL_FOR_VIEWS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>VTL For View Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>VTL For View Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VTL_FOR_VIEW_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Comparitor <em>Comparitor</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Comparitor
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.Ecore4regPackageImpl#getComparitor()
	 * @generated
	 */
	int COMPARITOR = 54;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Import <em>Import</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Import</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Import
	 * @generated
	 */
	EClass getImport();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Import#getImportedNamespace <em>Imported Namespace</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Imported Namespace</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Import#getImportedNamespace()
	 * @see #getImport()
	 * @generated
	 */
	EAttribute getImport_ImportedNamespace();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module
	 * @generated
	 */
	EClass getModule();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getDependencies <em>Dependencies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Dependencies</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getDependencies()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_Dependencies();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getTheDescription <em>The Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>The Description</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getTheDescription()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_TheDescription();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getLicense <em>License</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>License</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getLicense()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_License();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getName()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getVersion()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Version();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getImports <em>Imports</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Imports</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module#getImports()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_Imports();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList <em>Module List</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module List</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList
	 * @generated
	 */
	EClass getModuleList();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList#getModules <em>Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Modules</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList#getModules()
	 * @see #getModuleList()
	 * @generated
	 */
	EReference getModuleList_Modules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes <em>Allowed Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Allowed Types</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes
	 * @generated
	 */
	EClass getAllowedTypes();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes#getAllowedTypes <em>Allowed Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Allowed Types</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes#getAllowedTypes()
	 * @see #getAllowedTypes()
	 * @generated
	 */
	EReference getAllowedTypes_AllowedTypes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule <em>Requirements Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule
	 * @generated
	 */
	EClass getRequirementsModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule#getRules <em>Rules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule#getRules()
	 * @see #getRequirementsModule()
	 * @generated
	 */
	EReference getRequirementsModule_Rules();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule#getAllowedtypes <em>Allowedtypes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Allowedtypes</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule#getAllowedtypes()
	 * @see #getRequirementsModule()
	 * @generated
	 */
	EReference getRequirementsModule_Allowedtypes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection <em>Requirements Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection
	 * @generated
	 */
	EClass getRequirementsSection();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection#getName()
	 * @see #getRequirementsSection()
	 * @generated
	 */
	EAttribute getRequirementsSection_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage <em>Requirements Section Image</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section Image</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage
	 * @generated
	 */
	EClass getRequirementsSectionImage();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage#getStyle <em>Style</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Style</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage#getStyle()
	 * @see #getRequirementsSectionImage()
	 * @generated
	 */
	EAttribute getRequirementsSectionImage_Style();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage#getUri <em>Uri</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Uri</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage#getUri()
	 * @see #getRequirementsSectionImage()
	 * @generated
	 */
	EAttribute getRequirementsSectionImage_Uri();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText <em>Requirements Section Link With Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section Link With Text</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText
	 * @generated
	 */
	EClass getRequirementsSectionLinkWithText();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getLinkedRuleSection <em>Linked Rule Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Linked Rule Section</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getLinkedRuleSection()
	 * @see #getRequirementsSectionLinkWithText()
	 * @generated
	 */
	EReference getRequirementsSectionLinkWithText_LinkedRuleSection();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getLinkText <em>Link Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Link Text</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getLinkText()
	 * @see #getRequirementsSectionLinkWithText()
	 * @generated
	 */
	EAttribute getRequirementsSectionLinkWithText_LinkText();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getSubsection <em>Subsection</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Subsection</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText#getSubsection()
	 * @see #getRequirementsSectionLinkWithText()
	 * @generated
	 */
	EAttribute getRequirementsSectionLinkWithText_Subsection();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionText <em>Requirements Section Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section Text</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionText
	 * @generated
	 */
	EClass getRequirementsSectionText();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionText#getText <em>Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Text</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionText#getText()
	 * @see #getRequirementsSectionText()
	 * @generated
	 */
	EAttribute getRequirementsSectionText_Text();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType <em>Requirement Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirement Type</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType
	 * @generated
	 */
	EClass getRequirementType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType#getName()
	 * @see #getRequirementType()
	 * @generated
	 */
	EAttribute getRequirementType_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection <em>Titled Requirements Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Titled Requirements Section</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection
	 * @generated
	 */
	EClass getTitledRequirementsSection();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getSections <em>Sections</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sections</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getSections()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EReference getTitledRequirementsSection_Sections();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getReferencingSections <em>Referencing Sections</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Referencing Sections</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getReferencingSections()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EReference getTitledRequirementsSection_ReferencingSections();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getRequirementsType <em>Requirements Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Requirements Type</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getRequirementsType()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EReference getTitledRequirementsSection_RequirementsType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getTitle <em>Title</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Title</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection#getTitle()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EAttribute getTitledRequirementsSection_Title();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag <em>Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Tag</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag
	 * @generated
	 */
	EClass getTag();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag#getDisplayName()
	 * @see #getTag()
	 * @generated
	 */
	EAttribute getTag_DisplayName();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag#getName()
	 * @see #getTag()
	 * @generated
	 */
	EAttribute getTag_Name();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag#getRequirements()
	 * @see #getTag()
	 * @generated
	 */
	EReference getTag_Requirements();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TagGroup <em>Tag Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Tag Group</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TagGroup
	 * @generated
	 */
	EClass getTagGroup();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TagGroup#getTags <em>Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tags</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TagGroup#getTags()
	 * @see #getTagGroup()
	 * @generated
	 */
	EReference getTagGroup_Tags();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport <em>Rules For Report</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rules For Report</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport
	 * @generated
	 */
	EClass getRulesForReport();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport#getOutputLayerCube <em>Output Layer Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Output Layer Cube</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport#getOutputLayerCube()
	 * @see #getRulesForReport()
	 * @generated
	 */
	EReference getRulesForReport_OutputLayerCube();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport#getRulesForTable <em>Rules For Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules For Table</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport#getRulesForTable()
	 * @see #getRulesForReport()
	 * @generated
	 */
	EReference getRulesForReport_RulesForTable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable <em>Rules For IL Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rules For IL Table</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable
	 * @generated
	 */
	EClass getRulesForILTable();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable#getRulesForTablePart <em>Rules For Table Part</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules For Table Part</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable#getRulesForTablePart()
	 * @see #getRulesForILTable()
	 * @generated
	 */
	EReference getRulesForILTable_RulesForTablePart();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable#getInputLayerTable <em>Input Layer Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Input Layer Table</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable#getInputLayerTable()
	 * @see #getRulesForILTable()
	 * @generated
	 */
	EReference getRulesForILTable_InputLayerTable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn <em>Select Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Column</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn
	 * @generated
	 */
	EClass getSelectColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn#getAsAttribute <em>As Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>As Attribute</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn#getAsAttribute()
	 * @see #getSelectColumn()
	 * @generated
	 */
	EReference getSelectColumn_AsAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnMemberAs <em>Select Column Member As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Column Member As</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnMemberAs
	 * @generated
	 */
	EClass getSelectColumnMemberAs();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnMemberAs#getMemberAsConstant <em>Member As Constant</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member As Constant</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnMemberAs#getMemberAsConstant()
	 * @see #getSelectColumnMemberAs()
	 * @generated
	 */
	EReference getSelectColumnMemberAs_MemberAsConstant();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnAttributeAs <em>Select Column Attribute As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Column Attribute As</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnAttributeAs
	 * @generated
	 */
	EClass getSelectColumnAttributeAs();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnAttributeAs#getAttribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnAttributeAs#getAttribute()
	 * @see #getSelectColumnAttributeAs()
	 * @generated
	 */
	EReference getSelectColumnAttributeAs_Attribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectValueAs <em>Select Value As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Value As</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectValueAs
	 * @generated
	 */
	EClass getSelectValueAs();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectValueAs#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectValueAs#getValue()
	 * @see #getSelectValueAs()
	 * @generated
	 */
	EAttribute getSelectValueAs_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter <em>Table Filter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Table Filter</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter
	 * @generated
	 */
	EClass getTableFilter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getAttribute1 <em>Attribute1</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute1</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getAttribute1()
	 * @see #getTableFilter()
	 * @generated
	 */
	EReference getTableFilter_Attribute1();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getComparitor <em>Comparitor</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Comparitor</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getComparitor()
	 * @see #getTableFilter()
	 * @generated
	 */
	EAttribute getTableFilter_Comparitor();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getMember <em>Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getMember()
	 * @see #getTableFilter()
	 * @generated
	 */
	EReference getTableFilter_Member();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter#getValue()
	 * @see #getTableFilter()
	 * @generated
	 */
	EAttribute getTableFilter_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule <em>Generation Rules Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generation Rules Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule
	 * @generated
	 */
	EClass getGenerationRulesModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule#getRulesForReport <em>Rules For Report</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules For Report</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule#getRulesForReport()
	 * @see #getGenerationRulesModule()
	 * @generated
	 */
	EReference getGenerationRulesModule_RulesForReport();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart <em>Rule For IL Table Part</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rule For IL Table Part</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart
	 * @generated
	 */
	EClass getRuleForILTablePart();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getName()
	 * @see #getRuleForILTablePart()
	 * @generated
	 */
	EAttribute getRuleForILTablePart_Name();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getColumns <em>Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Columns</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getColumns()
	 * @see #getRuleForILTablePart()
	 * @generated
	 */
	EReference getRuleForILTablePart_Columns();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart#getWhereClause()
	 * @see #getRuleForILTablePart()
	 * @generated
	 */
	EReference getRuleForILTablePart_WhereClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute <em>EL Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Attribute</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute
	 * @generated
	 */
	EClass getELAttribute();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#isID <em>ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>ID</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#isID()
	 * @see #getELAttribute()
	 * @generated
	 */
	EAttribute getELAttribute_ID();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#getEAttributeType <em>EAttribute Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>EAttribute Type</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute#getEAttributeType()
	 * @see #getELAttribute()
	 * @generated
	 */
	EReference getELAttribute_EAttributeType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass <em>EL Class</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Class</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass
	 * @generated
	 */
	EClass getELClass();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#isAbstract <em>Abstract</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Abstract</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#isAbstract()
	 * @see #getELClass()
	 * @generated
	 */
	EAttribute getELClass_Abstract();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getESuperTypes <em>ESuper Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>ESuper Types</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getESuperTypes()
	 * @see #getELClass()
	 * @generated
	 */
	EReference getELClass_ESuperTypes();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getEStructuralFeatures <em>EStructural Features</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>EStructural Features</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getEStructuralFeatures()
	 * @see #getELClass()
	 * @generated
	 */
	EReference getELClass_EStructuralFeatures();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getEOperations <em>EOperations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>EOperations</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass#getEOperations()
	 * @see #getELClass()
	 * @generated
	 */
	EReference getELClass_EOperations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier <em>EL Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Classifier</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier
	 * @generated
	 */
	EClass getELClassifier();

	/**
	 * Returns the meta object for the container reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier#getPackage <em>Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Package</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier#getPackage()
	 * @see #getELClassifier()
	 * @generated
	 */
	EReference getELClassifier_Package();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType <em>EL Data Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Data Type</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType
	 * @generated
	 */
	EClass getELDataType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType#getIndustryName <em>Industry Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Industry Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType#getIndustryName()
	 * @see #getELDataType()
	 * @generated
	 */
	EAttribute getELDataType_IndustryName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum <em>EL Enum</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Enum</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum
	 * @generated
	 */
	EClass getELEnum();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum#getELiterals <em>ELiterals</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>ELiterals</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum#getELiterals()
	 * @see #getELEnum()
	 * @generated
	 */
	EReference getELEnum_ELiterals();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral <em>EL Enum Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Enum Literal</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral
	 * @generated
	 */
	EClass getELEnumLiteral();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral#getValue()
	 * @see #getELEnumLiteral()
	 * @generated
	 */
	EAttribute getELEnumLiteral_Value();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral#getLiteral <em>Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Literal</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral#getLiteral()
	 * @see #getELEnumLiteral()
	 * @generated
	 */
	EAttribute getELEnumLiteral_Literal();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement <em>EL Model Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Model Element</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement
	 * @generated
	 */
	EClass getELModelElement();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement#getEAnnotations <em>EAnnotations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>EAnnotations</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement#getEAnnotations()
	 * @see #getELModelElement()
	 * @generated
	 */
	EReference getELModelElement_EAnnotations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELNamedElement <em>EL Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Named Element</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELNamedElement
	 * @generated
	 */
	EClass getELNamedElement();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELNamedElement#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELNamedElement#getName()
	 * @see #getELNamedElement()
	 * @generated
	 */
	EAttribute getELNamedElement_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation <em>EL Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Operation</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation
	 * @generated
	 */
	EClass getELOperation();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Body</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation#getBody()
	 * @see #getELOperation()
	 * @generated
	 */
	EAttribute getELOperation_Body();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage <em>EL Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Package</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage
	 * @generated
	 */
	EClass getELPackage();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getEClassifiers <em>EClassifiers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>EClassifiers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getEClassifiers()
	 * @see #getELPackage()
	 * @generated
	 */
	EReference getELPackage_EClassifiers();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getNsURI <em>Ns URI</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns URI</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getNsURI()
	 * @see #getELPackage()
	 * @generated
	 */
	EAttribute getELPackage_NsURI();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getNsPrefix <em>Ns Prefix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns Prefix</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage#getNsPrefix()
	 * @see #getELPackage()
	 * @generated
	 */
	EAttribute getELPackage_NsPrefix();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference <em>EL Reference</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Reference</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference
	 * @generated
	 */
	EClass getELReference();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference#isContainment <em>Containment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Containment</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference#isContainment()
	 * @see #getELReference()
	 * @generated
	 */
	EAttribute getELReference_Containment();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference#getEReferenceType <em>EReference Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>EReference Type</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference#getEReferenceType()
	 * @see #getELReference()
	 * @generated
	 */
	EReference getELReference_EReferenceType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStructuralFeature <em>EL Structural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Structural Feature</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStructuralFeature
	 * @generated
	 */
	EClass getELStructuralFeature();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement <em>EL Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Typed Element</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement
	 * @generated
	 */
	EClass getELTypedElement();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement#getEType <em>EType</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>EType</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement#getEType()
	 * @see #getELTypedElement()
	 * @generated
	 */
	EReference getELTypedElement_EType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement#getUpperBound <em>Upper Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Upper Bound</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement#getUpperBound()
	 * @see #getELTypedElement()
	 * @generated
	 */
	EAttribute getELTypedElement_UpperBound();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement#getLowerBound <em>Lower Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Lower Bound</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement#getLowerBound()
	 * @see #getELTypedElement()
	 * @generated
	 */
	EAttribute getELTypedElement_LowerBound();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation <em>EL Annotation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL Annotation</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation
	 * @generated
	 */
	EClass getELAnnotation();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation#getDetails <em>Details</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Details</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation#getDetails()
	 * @see #getELAnnotation()
	 * @generated
	 */
	EReference getELAnnotation_Details();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation#getSource <em>Source</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Source</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation#getSource()
	 * @see #getELAnnotation()
	 * @generated
	 */
	EAttribute getELAnnotation_Source();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry <em>EL String To String Map Entry</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>EL String To String Map Entry</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry
	 * @generated
	 */
	EClass getELStringToStringMapEntry();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry#getKey <em>Key</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Key</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry#getKey()
	 * @see #getELStringToStringMapEntry()
	 * @generated
	 */
	EAttribute getELStringToStringMapEntry_Key();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry#getValue()
	 * @see #getELStringToStringMapEntry()
	 * @generated
	 */
	EAttribute getELStringToStringMapEntry_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLEnrichedCube <em>VTL Enriched Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Enriched Cube</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLEnrichedCube
	 * @generated
	 */
	EClass getVTLEnrichedCube();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLEnrichedCube#getTransformations <em>Transformations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Transformations</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLEnrichedCube#getTransformations()
	 * @see #getVTLEnrichedCube()
	 * @generated
	 */
	EReference getVTLEnrichedCube_Transformations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer <em>VTL Generated Outputlayer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Generated Outputlayer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer
	 * @generated
	 */
	EClass getVTLGeneratedOutputlayer();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getName()
	 * @see #getVTLGeneratedOutputlayer()
	 * @generated
	 */
	EAttribute getVTLGeneratedOutputlayer_Name();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getDependant_intermediate_layers <em>Dependant intermediate layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Dependant intermediate layers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getDependant_intermediate_layers()
	 * @see #getVTLGeneratedOutputlayer()
	 * @generated
	 */
	EReference getVTLGeneratedOutputlayer_Dependant_intermediate_layers();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getVTLForOutputLayerAndIntemedateLayerCombinations <em>VTL For Output Layer And Intemedate Layer Combinations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>VTL For Output Layer And Intemedate Layer Combinations</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getVTLForOutputLayerAndIntemedateLayerCombinations()
	 * @see #getVTLGeneratedOutputlayer()
	 * @generated
	 */
	EReference getVTLGeneratedOutputlayer_VTLForOutputLayerAndIntemedateLayerCombinations();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getOutputLayer <em>Output Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Output Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getOutputLayer()
	 * @see #getVTLGeneratedOutputlayer()
	 * @generated
	 */
	EReference getVTLGeneratedOutputlayer_OutputLayer();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination <em>VTL For Output Layer And Intermediate Layer Combination</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL For Output Layer And Intermediate Layer Combination</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination
	 * @generated
	 */
	EClass getVTLForOutputLayerAndIntermediateLayerCombination();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getTransformations <em>Transformations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Transformations</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getTransformations()
	 * @see #getVTLForOutputLayerAndIntermediateLayerCombination()
	 * @generated
	 */
	EReference getVTLForOutputLayerAndIntermediateLayerCombination_Transformations();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getOutputLayer <em>Output Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Output Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getOutputLayer()
	 * @see #getVTLForOutputLayerAndIntermediateLayerCombination()
	 * @generated
	 */
	EReference getVTLForOutputLayerAndIntermediateLayerCombination_OutputLayer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getIntermediateLayer <em>Intermediate Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Intermediate Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getIntermediateLayer()
	 * @see #getVTLForOutputLayerAndIntermediateLayerCombination()
	 * @generated
	 */
	EReference getVTLForOutputLayerAndIntermediateLayerCombination_IntermediateLayer();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination#getName()
	 * @see #getVTLForOutputLayerAndIntermediateLayerCombination()
	 * @generated
	 */
	EAttribute getVTLForOutputLayerAndIntermediateLayerCombination_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer <em>VTL Generated Intermediate Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Generated Intermediate Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer
	 * @generated
	 */
	EClass getVTLGeneratedIntermediateLayer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getDependant_enriched_cubes <em>Dependant enriched cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Dependant enriched cubes</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getDependant_enriched_cubes()
	 * @see #getVTLGeneratedIntermediateLayer()
	 * @generated
	 */
	EReference getVTLGeneratedIntermediateLayer_Dependant_enriched_cubes();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getTransformations <em>Transformations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Transformations</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getTransformations()
	 * @see #getVTLGeneratedIntermediateLayer()
	 * @generated
	 */
	EReference getVTLGeneratedIntermediateLayer_Transformations();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getName()
	 * @see #getVTLGeneratedIntermediateLayer()
	 * @generated
	 */
	EAttribute getVTLGeneratedIntermediateLayer_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#isIsEnrichment <em>Is Enrichment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Enrichment</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#isIsEnrichment()
	 * @see #getVTLGeneratedIntermediateLayer()
	 * @generated
	 */
	EAttribute getVTLGeneratedIntermediateLayer_IsEnrichment();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation <em>VTL Transformation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Transformation</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation
	 * @generated
	 */
	EClass getVTLTransformation();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Expression</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getExpression()
	 * @see #getVTLTransformation()
	 * @generated
	 */
	EAttribute getVTLTransformation_Expression();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getDescription()
	 * @see #getVTLTransformation()
	 * @generated
	 */
	EAttribute getVTLTransformation_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getTransformation_id <em>Transformation id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Transformation id</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getTransformation_id()
	 * @see #getVTLTransformation()
	 * @generated
	 */
	EAttribute getVTLTransformation_Transformation_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getOrder_in_scheme <em>Order in scheme</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order in scheme</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation#getOrder_in_scheme()
	 * @see #getVTLTransformation()
	 * @generated
	 */
	EAttribute getVTLTransformation_Order_in_scheme();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme <em>VTL Scheme</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Scheme</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme
	 * @generated
	 */
	EClass getVTLScheme();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme#getExpressions <em>Expressions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Expressions</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme#getExpressions()
	 * @see #getVTLScheme()
	 * @generated
	 */
	EReference getVTLScheme_Expressions();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme#getScheme_id <em>Scheme id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Scheme id</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme#getScheme_id()
	 * @see #getVTLScheme()
	 * @generated
	 */
	EAttribute getVTLScheme_Scheme_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer <em>VTL For Selection Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL For Selection Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer
	 * @generated
	 */
	EClass getVTLForSelectionLayer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer#getSelectionLayer <em>Selection Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Selection Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer#getSelectionLayer()
	 * @see #getVTLForSelectionLayer()
	 * @generated
	 */
	EReference getVTLForSelectionLayer_SelectionLayer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer#getOutputLayer <em>Output Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Output Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer#getOutputLayer()
	 * @see #getVTLForSelectionLayer()
	 * @generated
	 */
	EReference getVTLForSelectionLayer_OutputLayer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer#getIntermediateLayer <em>Intermediate Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Intermediate Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer#getIntermediateLayer()
	 * @see #getVTLForSelectionLayer()
	 * @generated
	 */
	EReference getVTLForSelectionLayer_IntermediateLayer();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule <em>VTL Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule
	 * @generated
	 */
	EClass getVTLModule();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLSchemes <em>VTL Schemes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>VTL Schemes</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLSchemes()
	 * @see #getVTLModule()
	 * @generated
	 */
	EReference getVTLModule_VTLSchemes();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedOutputLayers <em>VTL Generated Output Layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>VTL Generated Output Layers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedOutputLayers()
	 * @see #getVTLModule()
	 * @generated
	 */
	EReference getVTLModule_VTLGeneratedOutputLayers();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedIntermediateLayers <em>VTL Generated Intermediate Layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>VTL Generated Intermediate Layers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedIntermediateLayers()
	 * @see #getVTLModule()
	 * @generated
	 */
	EReference getVTLModule_VTLGeneratedIntermediateLayers();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLEnrichedLayers <em>VTL Enriched Layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>VTL Enriched Layers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLEnrichedLayers()
	 * @see #getVTLModule()
	 * @generated
	 */
	EReference getVTLModule_VTLEnrichedLayers();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForSelectionLayers <em>VTL For Selection Layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>VTL For Selection Layers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForSelectionLayers()
	 * @see #getVTLModule()
	 * @generated
	 */
	EReference getVTLModule_VTLForSelectionLayers();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getEntityToVTLIntermediateLayerLinks <em>Entity To VTL Intermediate Layer Links</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Entity To VTL Intermediate Layer Links</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getEntityToVTLIntermediateLayerLinks()
	 * @see #getVTLModule()
	 * @generated
	 */
	EReference getVTLModule_EntityToVTLIntermediateLayerLinks();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForViews <em>VTL For Views</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>VTL For Views</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForViews()
	 * @see #getVTLModule()
	 * @generated
	 */
	EReference getVTLModule_VTLForViews();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink <em>Entity To VTL Intermediate Layer Link</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity To VTL Intermediate Layer Link</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink
	 * @generated
	 */
	EClass getEntityToVTLIntermediateLayerLink();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getVTLIntermediateLayer <em>VTL Intermediate Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>VTL Intermediate Layer</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getVTLIntermediateLayer()
	 * @see #getEntityToVTLIntermediateLayerLink()
	 * @generated
	 */
	EReference getEntityToVTLIntermediateLayerLink_VTLIntermediateLayer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getEntity()
	 * @see #getEntityToVTLIntermediateLayerLink()
	 * @generated
	 */
	EReference getEntityToVTLIntermediateLayerLink_Entity();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getFilter <em>Filter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Filter</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getFilter()
	 * @see #getEntityToVTLIntermediateLayerLink()
	 * @generated
	 */
	EAttribute getEntityToVTLIntermediateLayerLink_Filter();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView <em>VTL For View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL For View</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView
	 * @generated
	 */
	EClass getVTLForView();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView#getName()
	 * @see #getVTLForView()
	 * @generated
	 */
	EAttribute getVTLForView_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView#getView <em>View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>View</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView#getView()
	 * @see #getVTLForView()
	 * @generated
	 */
	EReference getVTLForView_View();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView#getVtl <em>Vtl</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Vtl</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView#getVtl()
	 * @see #getVTLForView()
	 * @generated
	 */
	EReference getVTLForView_Vtl();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule <em>VTL Generated Outputlayer Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Generated Outputlayer Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule
	 * @generated
	 */
	EClass getVTLGeneratedOutputlayerModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule#getVTLGeneratedOutputlayerModules <em>VTL Generated Outputlayer Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>VTL Generated Outputlayer Modules</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule#getVTLGeneratedOutputlayerModules()
	 * @see #getVTLGeneratedOutputlayerModule()
	 * @generated
	 */
	EReference getVTLGeneratedOutputlayerModule_VTLGeneratedOutputlayerModules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule <em>VTL For Output Layer And Intermediate Layer Combination Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL For Output Layer And Intermediate Layer Combination Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule
	 * @generated
	 */
	EClass getVTLForOutputLayerAndIntermediateLayerCombinationModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule#getVTLForOutputLayerAndIntermediateLayerCombinations <em>VTL For Output Layer And Intermediate Layer Combinations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>VTL For Output Layer And Intermediate Layer Combinations</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule#getVTLForOutputLayerAndIntermediateLayerCombinations()
	 * @see #getVTLForOutputLayerAndIntermediateLayerCombinationModule()
	 * @generated
	 */
	EReference getVTLForOutputLayerAndIntermediateLayerCombinationModule_VTLForOutputLayerAndIntermediateLayerCombinations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule <em>VTL Generated Intermediate Layer Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Generated Intermediate Layer Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule
	 * @generated
	 */
	EClass getVTLGeneratedIntermediateLayerModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule#getVTLGeneratedIntermediateLayers <em>VTL Generated Intermediate Layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>VTL Generated Intermediate Layers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule#getVTLGeneratedIntermediateLayers()
	 * @see #getVTLGeneratedIntermediateLayerModule()
	 * @generated
	 */
	EReference getVTLGeneratedIntermediateLayerModule_VTLGeneratedIntermediateLayers();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule <em>VTL Schemes Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL Schemes Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule
	 * @generated
	 */
	EClass getVTLSchemesModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule#getVTLSchemes <em>VTL Schemes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>VTL Schemes</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule#getVTLSchemes()
	 * @see #getVTLSchemesModule()
	 * @generated
	 */
	EReference getVTLSchemesModule_VTLSchemes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule <em>VTL For Selection Layer Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL For Selection Layer Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule
	 * @generated
	 */
	EClass getVTLForSelectionLayerModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule#getVTLForSelectionLayers <em>VTL For Selection Layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>VTL For Selection Layers</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule#getVTLForSelectionLayers()
	 * @see #getVTLForSelectionLayerModule()
	 * @generated
	 */
	EReference getVTLForSelectionLayerModule_VTLForSelectionLayers();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule <em>Entity To VTL Intermediate Layer Link Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity To VTL Intermediate Layer Link Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule
	 * @generated
	 */
	EClass getEntityToVTLIntermediateLayerLinkModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule#getEntityToVTLIntermediateLayerLinks <em>Entity To VTL Intermediate Layer Links</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Entity To VTL Intermediate Layer Links</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule#getEntityToVTLIntermediateLayerLinks()
	 * @see #getEntityToVTLIntermediateLayerLinkModule()
	 * @generated
	 */
	EReference getEntityToVTLIntermediateLayerLinkModule_EntityToVTLIntermediateLayerLinks();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule <em>VTL For View Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VTL For View Module</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule
	 * @generated
	 */
	EClass getVTLForViewModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule#getVTLForViews <em>VTL For Views</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>VTL For Views</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule#getVTLForViews()
	 * @see #getVTLForViewModule()
	 * @generated
	 */
	EReference getVTLForViewModule_VTLForViews();

	/**
	 * Returns the meta object for enum '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Comparitor <em>Comparitor</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Comparitor</em>'.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Comparitor
	 * @generated
	 */
	EEnum getComparitor();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Ecore4regFactory getEcore4regFactory();

} //Ecore4regPackage
