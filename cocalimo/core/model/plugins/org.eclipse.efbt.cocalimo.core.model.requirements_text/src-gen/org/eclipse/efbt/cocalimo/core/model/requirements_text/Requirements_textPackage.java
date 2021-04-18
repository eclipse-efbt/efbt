/**
 */
package org.eclipse.efbt.cocalimo.core.model.requirements_text;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
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
 * <!-- begin-model-doc -->
 * This package describes the classes for storing Structured Requirements text
 *  
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Requirements_textPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "requirements_text";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/requirements_text";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "requirements_text";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Requirements_textPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.AllowedTypesImpl <em>Allowed Types</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.AllowedTypesImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getAllowedTypes()
	 * @generated
	 */
	int ALLOWED_TYPES = 0;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsModuleImpl <em>Requirements Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsModule()
	 * @generated
	 */
	int REQUIREMENTS_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Rules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__RULES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Allowedtypes</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE__ALLOWEDTYPES = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Requirements Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Requirements Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REQUIREMENTS_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImpl <em>Requirements Section</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSection()
	 * @generated
	 */
	int REQUIREMENTS_SECTION = 2;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImageImpl <em>Requirements Section Image</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImageImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSectionImage()
	 * @generated
	 */
	int REQUIREMENTS_SECTION_IMAGE = 3;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionLinkWithTextImpl <em>Requirements Section Link With Text</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionLinkWithTextImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSectionLinkWithText()
	 * @generated
	 */
	int REQUIREMENTS_SECTION_LINK_WITH_TEXT = 4;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionTextImpl <em>Requirements Section Text</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionTextImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSectionText()
	 * @generated
	 */
	int REQUIREMENTS_SECTION_TEXT = 5;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementTypeImpl <em>Requirement Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementTypeImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementType()
	 * @generated
	 */
	int REQUIREMENT_TYPE = 6;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TitledRequirementsSectionImpl <em>Titled Requirements Section</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TitledRequirementsSectionImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getTitledRequirementsSection()
	 * @generated
	 */
	int TITLED_REQUIREMENTS_SECTION = 7;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagImpl <em>Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getTag()
	 * @generated
	 */
	int TAG = 8;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagGroupImpl <em>Tag Group</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagGroupImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getTagGroup()
	 * @generated
	 */
	int TAG_GROUP = 9;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP__TAGS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Tag Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Tag Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_GROUP_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.AllowedTypes <em>Allowed Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Allowed Types</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.AllowedTypes
	 * @generated
	 */
	EClass getAllowedTypes();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.AllowedTypes#getAllowedTypes <em>Allowed Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Allowed Types</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.AllowedTypes#getAllowedTypes()
	 * @see #getAllowedTypes()
	 * @generated
	 */
	EReference getAllowedTypes_AllowedTypes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule <em>Requirements Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule
	 * @generated
	 */
	EClass getRequirementsModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule#getRules <em>Rules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule#getRules()
	 * @see #getRequirementsModule()
	 * @generated
	 */
	EReference getRequirementsModule_Rules();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule#getAllowedtypes <em>Allowedtypes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Allowedtypes</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule#getAllowedtypes()
	 * @see #getRequirementsModule()
	 * @generated
	 */
	EReference getRequirementsModule_Allowedtypes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSection <em>Requirements Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSection
	 * @generated
	 */
	EClass getRequirementsSection();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSection#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSection#getName()
	 * @see #getRequirementsSection()
	 * @generated
	 */
	EAttribute getRequirementsSection_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage <em>Requirements Section Image</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section Image</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage
	 * @generated
	 */
	EClass getRequirementsSectionImage();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getStyle <em>Style</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Style</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getStyle()
	 * @see #getRequirementsSectionImage()
	 * @generated
	 */
	EAttribute getRequirementsSectionImage_Style();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getUri <em>Uri</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Uri</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionImage#getUri()
	 * @see #getRequirementsSectionImage()
	 * @generated
	 */
	EAttribute getRequirementsSectionImage_Uri();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText <em>Requirements Section Link With Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section Link With Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText
	 * @generated
	 */
	EClass getRequirementsSectionLinkWithText();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getLinkedRuleSection <em>Linked Rule Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Linked Rule Section</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getLinkedRuleSection()
	 * @see #getRequirementsSectionLinkWithText()
	 * @generated
	 */
	EReference getRequirementsSectionLinkWithText_LinkedRuleSection();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getLinkText <em>Link Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Link Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getLinkText()
	 * @see #getRequirementsSectionLinkWithText()
	 * @generated
	 */
	EAttribute getRequirementsSectionLinkWithText_LinkText();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getSubsection <em>Subsection</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Subsection</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionLinkWithText#getSubsection()
	 * @see #getRequirementsSectionLinkWithText()
	 * @generated
	 */
	EAttribute getRequirementsSectionLinkWithText_Subsection();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionText <em>Requirements Section Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirements Section Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionText
	 * @generated
	 */
	EClass getRequirementsSectionText();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionText#getText <em>Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Text</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsSectionText#getText()
	 * @see #getRequirementsSectionText()
	 * @generated
	 */
	EAttribute getRequirementsSectionText_Text();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementType <em>Requirement Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Requirement Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementType
	 * @generated
	 */
	EClass getRequirementType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementType#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementType#getName()
	 * @see #getRequirementType()
	 * @generated
	 */
	EAttribute getRequirementType_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection <em>Titled Requirements Section</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Titled Requirements Section</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection
	 * @generated
	 */
	EClass getTitledRequirementsSection();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getSections <em>Sections</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sections</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getSections()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EReference getTitledRequirementsSection_Sections();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getReferencingSections <em>Referencing Sections</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Referencing Sections</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getReferencingSections()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EReference getTitledRequirementsSection_ReferencingSections();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getRequirementsType <em>Requirements Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Requirements Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getRequirementsType()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EReference getTitledRequirementsSection_RequirementsType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getTitle <em>Title</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Title</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection#getTitle()
	 * @see #getTitledRequirementsSection()
	 * @generated
	 */
	EAttribute getTitledRequirementsSection_Title();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag <em>Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Tag</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag
	 * @generated
	 */
	EClass getTag();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag#getDisplayName()
	 * @see #getTag()
	 * @generated
	 */
	EAttribute getTag_DisplayName();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag#getName()
	 * @see #getTag()
	 * @generated
	 */
	EAttribute getTag_Name();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag#getRequirements()
	 * @see #getTag()
	 * @generated
	 */
	EReference getTag_Requirements();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TagGroup <em>Tag Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Tag Group</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.TagGroup
	 * @generated
	 */
	EClass getTagGroup();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TagGroup#getTags <em>Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tags</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.TagGroup#getTags()
	 * @see #getTagGroup()
	 * @generated
	 */
	EReference getTagGroup_Tags();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Requirements_textFactory getRequirements_textFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals
	{
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.AllowedTypesImpl <em>Allowed Types</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.AllowedTypesImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getAllowedTypes()
		 * @generated
		 */
		EClass ALLOWED_TYPES = eINSTANCE.getAllowedTypes();

		/**
		 * The meta object literal for the '<em><b>Allowed Types</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ALLOWED_TYPES__ALLOWED_TYPES = eINSTANCE.getAllowedTypes_AllowedTypes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsModuleImpl <em>Requirements Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsModule()
		 * @generated
		 */
		EClass REQUIREMENTS_MODULE = eINSTANCE.getRequirementsModule();

		/**
		 * The meta object literal for the '<em><b>Rules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REQUIREMENTS_MODULE__RULES = eINSTANCE.getRequirementsModule_Rules();

		/**
		 * The meta object literal for the '<em><b>Allowedtypes</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REQUIREMENTS_MODULE__ALLOWEDTYPES = eINSTANCE.getRequirementsModule_Allowedtypes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImpl <em>Requirements Section</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSection()
		 * @generated
		 */
		EClass REQUIREMENTS_SECTION = eINSTANCE.getRequirementsSection();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REQUIREMENTS_SECTION__NAME = eINSTANCE.getRequirementsSection_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImageImpl <em>Requirements Section Image</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionImageImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSectionImage()
		 * @generated
		 */
		EClass REQUIREMENTS_SECTION_IMAGE = eINSTANCE.getRequirementsSectionImage();

		/**
		 * The meta object literal for the '<em><b>Style</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REQUIREMENTS_SECTION_IMAGE__STYLE = eINSTANCE.getRequirementsSectionImage_Style();

		/**
		 * The meta object literal for the '<em><b>Uri</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REQUIREMENTS_SECTION_IMAGE__URI = eINSTANCE.getRequirementsSectionImage_Uri();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionLinkWithTextImpl <em>Requirements Section Link With Text</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionLinkWithTextImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSectionLinkWithText()
		 * @generated
		 */
		EClass REQUIREMENTS_SECTION_LINK_WITH_TEXT = eINSTANCE.getRequirementsSectionLinkWithText();

		/**
		 * The meta object literal for the '<em><b>Linked Rule Section</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION = eINSTANCE.getRequirementsSectionLinkWithText_LinkedRuleSection();

		/**
		 * The meta object literal for the '<em><b>Link Text</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT = eINSTANCE.getRequirementsSectionLinkWithText_LinkText();

		/**
		 * The meta object literal for the '<em><b>Subsection</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION = eINSTANCE.getRequirementsSectionLinkWithText_Subsection();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionTextImpl <em>Requirements Section Text</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementsSectionTextImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementsSectionText()
		 * @generated
		 */
		EClass REQUIREMENTS_SECTION_TEXT = eINSTANCE.getRequirementsSectionText();

		/**
		 * The meta object literal for the '<em><b>Text</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REQUIREMENTS_SECTION_TEXT__TEXT = eINSTANCE.getRequirementsSectionText_Text();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementTypeImpl <em>Requirement Type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.RequirementTypeImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getRequirementType()
		 * @generated
		 */
		EClass REQUIREMENT_TYPE = eINSTANCE.getRequirementType();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REQUIREMENT_TYPE__NAME = eINSTANCE.getRequirementType_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TitledRequirementsSectionImpl <em>Titled Requirements Section</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TitledRequirementsSectionImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getTitledRequirementsSection()
		 * @generated
		 */
		EClass TITLED_REQUIREMENTS_SECTION = eINSTANCE.getTitledRequirementsSection();

		/**
		 * The meta object literal for the '<em><b>Sections</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TITLED_REQUIREMENTS_SECTION__SECTIONS = eINSTANCE.getTitledRequirementsSection_Sections();

		/**
		 * The meta object literal for the '<em><b>Referencing Sections</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS = eINSTANCE.getTitledRequirementsSection_ReferencingSections();

		/**
		 * The meta object literal for the '<em><b>Requirements Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE = eINSTANCE.getTitledRequirementsSection_RequirementsType();

		/**
		 * The meta object literal for the '<em><b>Title</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TITLED_REQUIREMENTS_SECTION__TITLE = eINSTANCE.getTitledRequirementsSection_Title();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagImpl <em>Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getTag()
		 * @generated
		 */
		EClass TAG = eINSTANCE.getTag();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TAG__DISPLAY_NAME = eINSTANCE.getTag_DisplayName();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TAG__NAME = eINSTANCE.getTag_Name();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TAG__REQUIREMENTS = eINSTANCE.getTag_Requirements();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagGroupImpl <em>Tag Group</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagGroupImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.Requirements_textPackageImpl#getTagGroup()
		 * @generated
		 */
		EClass TAG_GROUP = eINSTANCE.getTagGroup();

		/**
		 * The meta object literal for the '<em><b>Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TAG_GROUP__TAGS = eINSTANCE.getTagGroup_Tags();

	}

} //Requirements_textPackage
