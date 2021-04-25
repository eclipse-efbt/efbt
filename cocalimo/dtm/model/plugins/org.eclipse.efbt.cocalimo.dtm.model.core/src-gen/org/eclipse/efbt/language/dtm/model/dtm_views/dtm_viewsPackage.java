/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;

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
 * This package describes the set of DTMViews, each DTMView represents a DataSetTransformation which transforms
 *     data from one DataSet to another in an immutable side-effect free fashion. They use concepts from the well-known SQL StructuredQueryLanguage.
 *     
 *     The transformed dataset is created just once by the transformation and then never amended (it is immutable)
 *     The only data that can be used in the transformation is the data from the source dataset(s), so it cannot use anything else including
 *     'global variables' (which do not exist in the compitational model, the only data created by the transformation is the data in the resulting transformed dataset,
 *     no other data is created or affected so the transformations are side-effect free.
 * 
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface dtm_viewsPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "dtm_views";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/dtm_views";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "dtm_views";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	dtm_viewsPackage eINSTANCE = org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.DTMViewImpl <em>DTM View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.DTMViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getDTMView()
	 * @generated
	 */
	int DTM_VIEW = 10;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_VIEW__CUBE = TransformationPackage.DATA_SET_TRANSFORMATION__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_VIEW__NAME = TransformationPackage.DATA_SET_TRANSFORMATION__NAME;

	/**
	 * The number of structural features of the '<em>DTM View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_VIEW_FEATURE_COUNT = TransformationPackage.DATA_SET_TRANSFORMATION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>DTM View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DTM_VIEW_OPERATION_COUNT = TransformationPackage.DATA_SET_TRANSFORMATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewImpl <em>Aggregate Enrichment View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getAggregateEnrichmentView()
	 * @generated
	 */
	int AGGREGATE_ENRICHMENT_VIEW = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Functions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Group By Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Aggregate Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Aggregate Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.CopyViewImpl <em>Copy View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.CopyViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getCopyView()
	 * @generated
	 */
	int COPY_VIEW = 1;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The number of structural features of the '<em>Copy View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Copy View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.EnrichmentViewImpl <em>Enrichment View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.EnrichmentViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getEnrichmentView()
	 * @generated
	 */
	int ENRICHMENT_VIEW = 2;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Functions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__FUNCTIONS = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Renames</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__RENAMES = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Ignores</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__IGNORES = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByConditionViewImpl <em>Filter By Condition View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByConditionViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getFilterByConditionView()
	 * @generated
	 */
	int FILTER_BY_CONDITION_VIEW = 3;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW__WHERE_CLAUSE = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Filter By Condition View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Filter By Condition View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.GenericViewImpl <em>Generic View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.GenericViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getGenericView()
	 * @generated
	 */
	int GENERIC_VIEW = 4;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Select Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__SELECT_CLAUSE = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__WHERE_CLAUSE = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Group By Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__GROUP_BY_CLAUSE = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Generic View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Generic View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.GroupByClauseImpl <em>Group By Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.GroupByClauseImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getGroupByClause()
	 * @generated
	 */
	int GROUP_BY_CLAUSE = 5;

	/**
	 * The feature id for the '<em><b>Group By Columns</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_CLAUSE__GROUP_BY_COLUMNS = 0;

	/**
	 * The number of structural features of the '<em>Group By Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_CLAUSE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Group By Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_CLAUSE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.IgnoreColumnImpl <em>Ignore Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.IgnoreColumnImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getIgnoreColumn()
	 * @generated
	 */
	int IGNORE_COLUMN = 6;

	/**
	 * The feature id for the '<em><b>Column To Ignore</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IGNORE_COLUMN__COLUMN_TO_IGNORE = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IGNORE_COLUMN__CUBE = 1;

	/**
	 * The number of structural features of the '<em>Ignore Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IGNORE_COLUMN_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Ignore Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IGNORE_COLUMN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.JoinViewImpl <em>Join View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.JoinViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getJoinView()
	 * @generated
	 */
	int JOIN_VIEW = 7;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__WHERE_CLAUSE = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Renames</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__RENAMES = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Ignores</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__IGNORES = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Join View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Join View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.RenameAsImpl <em>Rename As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.RenameAsImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getRenameAs()
	 * @generated
	 */
	int RENAME_AS = 8;

	/**
	 * The feature id for the '<em><b>From Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RENAME_AS__FROM_CUBE = 0;

	/**
	 * The feature id for the '<em><b>From Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RENAME_AS__FROM_VARIABLE = 1;

	/**
	 * The feature id for the '<em><b>To Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RENAME_AS__TO_VARIABLE = 2;

	/**
	 * The number of structural features of the '<em>Rename As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RENAME_AS_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Rename As</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RENAME_AS_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.SelectClauseImpl <em>Select Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.SelectClauseImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getSelectClause()
	 * @generated
	 */
	int SELECT_CLAUSE = 9;

	/**
	 * The feature id for the '<em><b>Aggregate Functions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE__AGGREGATE_FUNCTIONS = 0;

	/**
	 * The feature id for the '<em><b>Basic Functions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE__BASIC_FUNCTIONS = 1;

	/**
	 * The feature id for the '<em><b>Varaibles</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE__VARAIBLES = 2;

	/**
	 * The number of structural features of the '<em>Select Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Select Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECT_CLAUSE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.UnionViewImpl <em>Union View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.UnionViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getUnionView()
	 * @generated
	 */
	int UNION_VIEW = 11;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The number of structural features of the '<em>Union View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Union View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.VersionedDTMViewsModuleImpl <em>Versioned DTM Views Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.VersionedDTMViewsModuleImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getVersionedDTMViewsModule()
	 * @generated
	 */
	int VERSIONED_DTM_VIEWS_MODULE = 12;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE__DEPENDENCIES = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE__THE_DESCRIPTION = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE__LICENSE = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE__NAME = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE__VERSION = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE__LONG_NAME = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__LONG_NAME;

	/**
	 * The feature id for the '<em><b>DTM Views</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Versioned DTM Views Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE_FEATURE_COUNT = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Versioned DTM Views Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_DTM_VIEWS_MODULE_OPERATION_COUNT = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.WhereClauseImpl <em>Where Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.WhereClauseImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getWhereClause()
	 * @generated
	 */
	int WHERE_CLAUSE = 13;

	/**
	 * The feature id for the '<em><b>Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHERE_CLAUSE__FUNCTION = 0;

	/**
	 * The number of structural features of the '<em>Where Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHERE_CLAUSE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Where Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHERE_CLAUSE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.CastColumnViewImpl <em>Cast Column View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.CastColumnViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getCastColumnView()
	 * @generated
	 */
	int CAST_COLUMN_VIEW = 14;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_COLUMN_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_COLUMN_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Source Struct Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Target Struct Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Cast Column View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_COLUMN_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Cast Column View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_COLUMN_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeArrayOfStructsViewImpl <em>Explode Array Of Structs View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeArrayOfStructsViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getExplodeArrayOfStructsView()
	 * @generated
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW = 15;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Array Source Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Index Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Target Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Explode Array Of Structs View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Explode Array Of Structs View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeStructViewImpl <em>Explode Struct View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeStructViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getExplodeStructView()
	 * @generated
	 */
	int EXPLODE_STRUCT_VIEW = 16;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_STRUCT_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_STRUCT_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Column To Unpack</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Explode Struct View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_STRUCT_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Explode Struct View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_STRUCT_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByStructClassColumnViewImpl <em>Filter By Struct Class Column View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByStructClassColumnViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getFilterByStructClassColumnView()
	 * @generated
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW = 17;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Rename As Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Struct Class</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Filter By Struct Class Column View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Filter By Struct Class Column View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_STRUCT_CLASS_COLUMN_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeArrayOfStructsViewImpl <em>Make Array Of Structs View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeArrayOfStructsViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getMakeArrayOfStructsView()
	 * @generated
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW = 18;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Grouping Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Source Struct Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Target Array Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Make Array Of Structs View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Make Array Of Structs View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_ARRAY_OF_STRUCTS_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeStructViewImpl <em>Make Struct View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeStructViewImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getMakeStructView()
	 * @generated
	 */
	int MAKE_STRUCT_VIEW = 19;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_STRUCT_VIEW__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_STRUCT_VIEW__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Target Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_STRUCT_VIEW__TARGET_VARIABLE = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Make Struct View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_STRUCT_VIEW_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Make Struct View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAKE_STRUCT_VIEW_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewAndOrderByImpl <em>Aggregate Enrichment View And Order By</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewAndOrderByImpl
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getAggregateEnrichmentViewAndOrderBy()
	 * @generated
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY = 20;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__CUBE = DTM_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__NAME = DTM_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>Functions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS = DTM_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Group By Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE = DTM_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Order By Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE = DTM_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Aggregate Enrichment View And Order By</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY_FEATURE_COUNT = DTM_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Aggregate Enrichment View And Order By</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY_OPERATION_COUNT = DTM_VIEW_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView <em>Aggregate Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Enrichment View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView
	 * @generated
	 */
	EClass getAggregateEnrichmentView();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView#getFunctions <em>Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Functions</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView#getFunctions()
	 * @see #getAggregateEnrichmentView()
	 * @generated
	 */
	EReference getAggregateEnrichmentView_Functions();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView#getGroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Group By Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentView#getGroupByClause()
	 * @see #getAggregateEnrichmentView()
	 * @generated
	 */
	EReference getAggregateEnrichmentView_GroupByClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CopyView <em>Copy View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Copy View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.CopyView
	 * @generated
	 */
	EClass getCopyView();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView <em>Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Enrichment View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView
	 * @generated
	 */
	EClass getEnrichmentView();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView#getFunctions <em>Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Functions</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView#getFunctions()
	 * @see #getEnrichmentView()
	 * @generated
	 */
	EReference getEnrichmentView_Functions();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView#getRenames <em>Renames</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Renames</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView#getRenames()
	 * @see #getEnrichmentView()
	 * @generated
	 */
	EReference getEnrichmentView_Renames();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView#getIgnores <em>Ignores</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Ignores</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView#getIgnores()
	 * @see #getEnrichmentView()
	 * @generated
	 */
	EReference getEnrichmentView_Ignores();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByConditionView <em>Filter By Condition View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Filter By Condition View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByConditionView
	 * @generated
	 */
	EClass getFilterByConditionView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByConditionView#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByConditionView#getWhereClause()
	 * @see #getFilterByConditionView()
	 * @generated
	 */
	EReference getFilterByConditionView_WhereClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GenericView <em>Generic View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generic View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GenericView
	 * @generated
	 */
	EClass getGenericView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GenericView#getSelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Select Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GenericView#getSelectClause()
	 * @see #getGenericView()
	 * @generated
	 */
	EReference getGenericView_SelectClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GenericView#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GenericView#getWhereClause()
	 * @see #getGenericView()
	 * @generated
	 */
	EReference getGenericView_WhereClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GenericView#getGroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Group By Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GenericView#getGroupByClause()
	 * @see #getGenericView()
	 * @generated
	 */
	EReference getGenericView_GroupByClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Group By Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause
	 * @generated
	 */
	EClass getGroupByClause();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause#getGroupByColumns <em>Group By Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Group By Columns</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.GroupByClause#getGroupByColumns()
	 * @see #getGroupByClause()
	 * @generated
	 */
	EReference getGroupByClause_GroupByColumns();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn <em>Ignore Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Ignore Column</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn
	 * @generated
	 */
	EClass getIgnoreColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getColumnToIgnore <em>Column To Ignore</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column To Ignore</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getColumnToIgnore()
	 * @see #getIgnoreColumn()
	 * @generated
	 */
	EReference getIgnoreColumn_ColumnToIgnore();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getCube()
	 * @see #getIgnoreColumn()
	 * @generated
	 */
	EReference getIgnoreColumn_Cube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.JoinView <em>Join View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Join View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.JoinView
	 * @generated
	 */
	EClass getJoinView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.JoinView#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.JoinView#getWhereClause()
	 * @see #getJoinView()
	 * @generated
	 */
	EReference getJoinView_WhereClause();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.JoinView#getRenames <em>Renames</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Renames</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.JoinView#getRenames()
	 * @see #getJoinView()
	 * @generated
	 */
	EReference getJoinView_Renames();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.JoinView#getIgnores <em>Ignores</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Ignores</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.JoinView#getIgnores()
	 * @see #getJoinView()
	 * @generated
	 */
	EReference getJoinView_Ignores();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs <em>Rename As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rename As</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs
	 * @generated
	 */
	EClass getRenameAs();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs#getFromCube <em>From Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>From Cube</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs#getFromCube()
	 * @see #getRenameAs()
	 * @generated
	 */
	EReference getRenameAs_FromCube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs#getFromVariable <em>From Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>From Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs#getFromVariable()
	 * @see #getRenameAs()
	 * @generated
	 */
	EReference getRenameAs_FromVariable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs#getToVariable <em>To Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>To Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs#getToVariable()
	 * @see #getRenameAs()
	 * @generated
	 */
	EReference getRenameAs_ToVariable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause
	 * @generated
	 */
	EClass getSelectClause();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause#getAggregateFunctions <em>Aggregate Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Aggregate Functions</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause#getAggregateFunctions()
	 * @see #getSelectClause()
	 * @generated
	 */
	EReference getSelectClause_AggregateFunctions();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause#getBasicFunctions <em>Basic Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Basic Functions</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause#getBasicFunctions()
	 * @see #getSelectClause()
	 * @generated
	 */
	EReference getSelectClause_BasicFunctions();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause#getVaraibles <em>Varaibles</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Varaibles</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.SelectClause#getVaraibles()
	 * @see #getSelectClause()
	 * @generated
	 */
	EReference getSelectClause_Varaibles();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.DTMView <em>DTM View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>DTM View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.DTMView
	 * @generated
	 */
	EClass getDTMView();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.UnionView <em>Union View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Union View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.UnionView
	 * @generated
	 */
	EClass getUnionView();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule <em>Versioned DTM Views Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Versioned DTM Views Module</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule
	 * @generated
	 */
	EClass getVersionedDTMViewsModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule#getDTMViews <em>DTM Views</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>DTM Views</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule#getDTMViews()
	 * @see #getVersionedDTMViewsModule()
	 * @generated
	 */
	EReference getVersionedDTMViewsModule_DTMViews();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause
	 * @generated
	 */
	EClass getWhereClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause#getFunction <em>Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Function</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.WhereClause#getFunction()
	 * @see #getWhereClause()
	 * @generated
	 */
	EReference getWhereClause_Function();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView <em>Cast Column View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cast Column View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView
	 * @generated
	 */
	EClass getCastColumnView();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getSourceStructVariable <em>Source Struct Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Source Struct Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getSourceStructVariable()
	 * @see #getCastColumnView()
	 * @generated
	 */
	EReference getCastColumnView_SourceStructVariable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getTargetStructVariable <em>Target Struct Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Target Struct Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView#getTargetStructVariable()
	 * @see #getCastColumnView()
	 * @generated
	 */
	EReference getCastColumnView_TargetStructVariable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView <em>Explode Array Of Structs View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Explode Array Of Structs View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView
	 * @generated
	 */
	EClass getExplodeArrayOfStructsView();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getArraySourceColumn <em>Array Source Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Array Source Column</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getArraySourceColumn()
	 * @see #getExplodeArrayOfStructsView()
	 * @generated
	 */
	EReference getExplodeArrayOfStructsView_ArraySourceColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getIndexColumn <em>Index Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Index Column</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getIndexColumn()
	 * @see #getExplodeArrayOfStructsView()
	 * @generated
	 */
	EReference getExplodeArrayOfStructsView_IndexColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getTargetStructColumn <em>Target Struct Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Target Struct Column</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeArrayOfStructsView#getTargetStructColumn()
	 * @see #getExplodeArrayOfStructsView()
	 * @generated
	 */
	EReference getExplodeArrayOfStructsView_TargetStructColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView <em>Explode Struct View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Explode Struct View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView
	 * @generated
	 */
	EClass getExplodeStructView();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView#getColumnToUnpack <em>Column To Unpack</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column To Unpack</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView#getColumnToUnpack()
	 * @see #getExplodeStructView()
	 * @generated
	 */
	EReference getExplodeStructView_ColumnToUnpack();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView <em>Filter By Struct Class Column View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Filter By Struct Class Column View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView
	 * @generated
	 */
	EClass getFilterByStructClassColumnView();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView#getRenameAsStructColumn <em>Rename As Struct Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Rename As Struct Column</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView#getRenameAsStructColumn()
	 * @see #getFilterByStructClassColumnView()
	 * @generated
	 */
	EReference getFilterByStructClassColumnView_RenameAsStructColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView#getStructClass <em>Struct Class</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Struct Class</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView#getStructClass()
	 * @see #getFilterByStructClassColumnView()
	 * @generated
	 */
	EReference getFilterByStructClassColumnView_StructClass();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView#getStructColumn <em>Struct Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Struct Column</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.FilterByStructClassColumnView#getStructColumn()
	 * @see #getFilterByStructClassColumnView()
	 * @generated
	 */
	EReference getFilterByStructClassColumnView_StructColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView <em>Make Array Of Structs View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Make Array Of Structs View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView
	 * @generated
	 */
	EClass getMakeArrayOfStructsView();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getGroupingVariable <em>Grouping Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Grouping Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getGroupingVariable()
	 * @see #getMakeArrayOfStructsView()
	 * @generated
	 */
	EReference getMakeArrayOfStructsView_GroupingVariable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getSourceStructVariable <em>Source Struct Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Source Struct Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getSourceStructVariable()
	 * @see #getMakeArrayOfStructsView()
	 * @generated
	 */
	EReference getMakeArrayOfStructsView_SourceStructVariable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getTargetArrayVariable <em>Target Array Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Target Array Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeArrayOfStructsView#getTargetArrayVariable()
	 * @see #getMakeArrayOfStructsView()
	 * @generated
	 */
	EReference getMakeArrayOfStructsView_TargetArrayVariable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView <em>Make Struct View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Make Struct View</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView
	 * @generated
	 */
	EClass getMakeStructView();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView#getTargetVariable <em>Target Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Target Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView#getTargetVariable()
	 * @see #getMakeStructView()
	 * @generated
	 */
	EReference getMakeStructView_TargetVariable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy <em>Aggregate Enrichment View And Order By</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Enrichment View And Order By</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy
	 * @generated
	 */
	EClass getAggregateEnrichmentViewAndOrderBy();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy#getFunctions <em>Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Functions</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy#getFunctions()
	 * @see #getAggregateEnrichmentViewAndOrderBy()
	 * @generated
	 */
	EReference getAggregateEnrichmentViewAndOrderBy_Functions();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy#getGroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Group By Clause</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy#getGroupByClause()
	 * @see #getAggregateEnrichmentViewAndOrderBy()
	 * @generated
	 */
	EReference getAggregateEnrichmentViewAndOrderBy_GroupByClause();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy#getOrderByVariable <em>Order By Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Order By Variable</em>'.
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy#getOrderByVariable()
	 * @see #getAggregateEnrichmentViewAndOrderBy()
	 * @generated
	 */
	EReference getAggregateEnrichmentViewAndOrderBy_OrderByVariable();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	dtm_viewsFactory getdtm_viewsFactory();

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
	interface Literals {
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewImpl <em>Aggregate Enrichment View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getAggregateEnrichmentView()
		 * @generated
		 */
		EClass AGGREGATE_ENRICHMENT_VIEW = eINSTANCE.getAggregateEnrichmentView();

		/**
		 * The meta object literal for the '<em><b>Functions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS = eINSTANCE.getAggregateEnrichmentView_Functions();

		/**
		 * The meta object literal for the '<em><b>Group By Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE = eINSTANCE.getAggregateEnrichmentView_GroupByClause();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.CopyViewImpl <em>Copy View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.CopyViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getCopyView()
		 * @generated
		 */
		EClass COPY_VIEW = eINSTANCE.getCopyView();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.EnrichmentViewImpl <em>Enrichment View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.EnrichmentViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getEnrichmentView()
		 * @generated
		 */
		EClass ENRICHMENT_VIEW = eINSTANCE.getEnrichmentView();

		/**
		 * The meta object literal for the '<em><b>Functions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENRICHMENT_VIEW__FUNCTIONS = eINSTANCE.getEnrichmentView_Functions();

		/**
		 * The meta object literal for the '<em><b>Renames</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENRICHMENT_VIEW__RENAMES = eINSTANCE.getEnrichmentView_Renames();

		/**
		 * The meta object literal for the '<em><b>Ignores</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENRICHMENT_VIEW__IGNORES = eINSTANCE.getEnrichmentView_Ignores();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByConditionViewImpl <em>Filter By Condition View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByConditionViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getFilterByConditionView()
		 * @generated
		 */
		EClass FILTER_BY_CONDITION_VIEW = eINSTANCE.getFilterByConditionView();

		/**
		 * The meta object literal for the '<em><b>Where Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FILTER_BY_CONDITION_VIEW__WHERE_CLAUSE = eINSTANCE.getFilterByConditionView_WhereClause();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.GenericViewImpl <em>Generic View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.GenericViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getGenericView()
		 * @generated
		 */
		EClass GENERIC_VIEW = eINSTANCE.getGenericView();

		/**
		 * The meta object literal for the '<em><b>Select Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERIC_VIEW__SELECT_CLAUSE = eINSTANCE.getGenericView_SelectClause();

		/**
		 * The meta object literal for the '<em><b>Where Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERIC_VIEW__WHERE_CLAUSE = eINSTANCE.getGenericView_WhereClause();

		/**
		 * The meta object literal for the '<em><b>Group By Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERIC_VIEW__GROUP_BY_CLAUSE = eINSTANCE.getGenericView_GroupByClause();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.GroupByClauseImpl <em>Group By Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.GroupByClauseImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getGroupByClause()
		 * @generated
		 */
		EClass GROUP_BY_CLAUSE = eINSTANCE.getGroupByClause();

		/**
		 * The meta object literal for the '<em><b>Group By Columns</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GROUP_BY_CLAUSE__GROUP_BY_COLUMNS = eINSTANCE.getGroupByClause_GroupByColumns();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.IgnoreColumnImpl <em>Ignore Column</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.IgnoreColumnImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getIgnoreColumn()
		 * @generated
		 */
		EClass IGNORE_COLUMN = eINSTANCE.getIgnoreColumn();

		/**
		 * The meta object literal for the '<em><b>Column To Ignore</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference IGNORE_COLUMN__COLUMN_TO_IGNORE = eINSTANCE.getIgnoreColumn_ColumnToIgnore();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference IGNORE_COLUMN__CUBE = eINSTANCE.getIgnoreColumn_Cube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.JoinViewImpl <em>Join View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.JoinViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getJoinView()
		 * @generated
		 */
		EClass JOIN_VIEW = eINSTANCE.getJoinView();

		/**
		 * The meta object literal for the '<em><b>Where Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference JOIN_VIEW__WHERE_CLAUSE = eINSTANCE.getJoinView_WhereClause();

		/**
		 * The meta object literal for the '<em><b>Renames</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference JOIN_VIEW__RENAMES = eINSTANCE.getJoinView_Renames();

		/**
		 * The meta object literal for the '<em><b>Ignores</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference JOIN_VIEW__IGNORES = eINSTANCE.getJoinView_Ignores();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.RenameAsImpl <em>Rename As</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.RenameAsImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getRenameAs()
		 * @generated
		 */
		EClass RENAME_AS = eINSTANCE.getRenameAs();

		/**
		 * The meta object literal for the '<em><b>From Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RENAME_AS__FROM_CUBE = eINSTANCE.getRenameAs_FromCube();

		/**
		 * The meta object literal for the '<em><b>From Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RENAME_AS__FROM_VARIABLE = eINSTANCE.getRenameAs_FromVariable();

		/**
		 * The meta object literal for the '<em><b>To Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RENAME_AS__TO_VARIABLE = eINSTANCE.getRenameAs_ToVariable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.SelectClauseImpl <em>Select Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.SelectClauseImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getSelectClause()
		 * @generated
		 */
		EClass SELECT_CLAUSE = eINSTANCE.getSelectClause();

		/**
		 * The meta object literal for the '<em><b>Aggregate Functions</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SELECT_CLAUSE__AGGREGATE_FUNCTIONS = eINSTANCE.getSelectClause_AggregateFunctions();

		/**
		 * The meta object literal for the '<em><b>Basic Functions</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SELECT_CLAUSE__BASIC_FUNCTIONS = eINSTANCE.getSelectClause_BasicFunctions();

		/**
		 * The meta object literal for the '<em><b>Varaibles</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SELECT_CLAUSE__VARAIBLES = eINSTANCE.getSelectClause_Varaibles();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.DTMViewImpl <em>DTM View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.DTMViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getDTMView()
		 * @generated
		 */
		EClass DTM_VIEW = eINSTANCE.getDTMView();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.UnionViewImpl <em>Union View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.UnionViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getUnionView()
		 * @generated
		 */
		EClass UNION_VIEW = eINSTANCE.getUnionView();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.VersionedDTMViewsModuleImpl <em>Versioned DTM Views Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.VersionedDTMViewsModuleImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getVersionedDTMViewsModule()
		 * @generated
		 */
		EClass VERSIONED_DTM_VIEWS_MODULE = eINSTANCE.getVersionedDTMViewsModule();

		/**
		 * The meta object literal for the '<em><b>DTM Views</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS = eINSTANCE.getVersionedDTMViewsModule_DTMViews();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.WhereClauseImpl <em>Where Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.WhereClauseImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getWhereClause()
		 * @generated
		 */
		EClass WHERE_CLAUSE = eINSTANCE.getWhereClause();

		/**
		 * The meta object literal for the '<em><b>Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference WHERE_CLAUSE__FUNCTION = eINSTANCE.getWhereClause_Function();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.CastColumnViewImpl <em>Cast Column View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.CastColumnViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getCastColumnView()
		 * @generated
		 */
		EClass CAST_COLUMN_VIEW = eINSTANCE.getCastColumnView();

		/**
		 * The meta object literal for the '<em><b>Source Struct Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CAST_COLUMN_VIEW__SOURCE_STRUCT_VARIABLE = eINSTANCE.getCastColumnView_SourceStructVariable();

		/**
		 * The meta object literal for the '<em><b>Target Struct Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CAST_COLUMN_VIEW__TARGET_STRUCT_VARIABLE = eINSTANCE.getCastColumnView_TargetStructVariable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeArrayOfStructsViewImpl <em>Explode Array Of Structs View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeArrayOfStructsViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getExplodeArrayOfStructsView()
		 * @generated
		 */
		EClass EXPLODE_ARRAY_OF_STRUCTS_VIEW = eINSTANCE.getExplodeArrayOfStructsView();

		/**
		 * The meta object literal for the '<em><b>Array Source Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN = eINSTANCE.getExplodeArrayOfStructsView_ArraySourceColumn();

		/**
		 * The meta object literal for the '<em><b>Index Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN = eINSTANCE.getExplodeArrayOfStructsView_IndexColumn();

		/**
		 * The meta object literal for the '<em><b>Target Struct Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN = eINSTANCE.getExplodeArrayOfStructsView_TargetStructColumn();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeStructViewImpl <em>Explode Struct View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.ExplodeStructViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getExplodeStructView()
		 * @generated
		 */
		EClass EXPLODE_STRUCT_VIEW = eINSTANCE.getExplodeStructView();

		/**
		 * The meta object literal for the '<em><b>Column To Unpack</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXPLODE_STRUCT_VIEW__COLUMN_TO_UNPACK = eINSTANCE.getExplodeStructView_ColumnToUnpack();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByStructClassColumnViewImpl <em>Filter By Struct Class Column View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.FilterByStructClassColumnViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getFilterByStructClassColumnView()
		 * @generated
		 */
		EClass FILTER_BY_STRUCT_CLASS_COLUMN_VIEW = eINSTANCE.getFilterByStructClassColumnView();

		/**
		 * The meta object literal for the '<em><b>Rename As Struct Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN = eINSTANCE.getFilterByStructClassColumnView_RenameAsStructColumn();

		/**
		 * The meta object literal for the '<em><b>Struct Class</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS = eINSTANCE.getFilterByStructClassColumnView_StructClass();

		/**
		 * The meta object literal for the '<em><b>Struct Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN = eINSTANCE.getFilterByStructClassColumnView_StructColumn();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeArrayOfStructsViewImpl <em>Make Array Of Structs View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeArrayOfStructsViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getMakeArrayOfStructsView()
		 * @generated
		 */
		EClass MAKE_ARRAY_OF_STRUCTS_VIEW = eINSTANCE.getMakeArrayOfStructsView();

		/**
		 * The meta object literal for the '<em><b>Grouping Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAKE_ARRAY_OF_STRUCTS_VIEW__GROUPING_VARIABLE = eINSTANCE.getMakeArrayOfStructsView_GroupingVariable();

		/**
		 * The meta object literal for the '<em><b>Source Struct Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAKE_ARRAY_OF_STRUCTS_VIEW__SOURCE_STRUCT_VARIABLE = eINSTANCE.getMakeArrayOfStructsView_SourceStructVariable();

		/**
		 * The meta object literal for the '<em><b>Target Array Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAKE_ARRAY_OF_STRUCTS_VIEW__TARGET_ARRAY_VARIABLE = eINSTANCE.getMakeArrayOfStructsView_TargetArrayVariable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeStructViewImpl <em>Make Struct View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.MakeStructViewImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getMakeStructView()
		 * @generated
		 */
		EClass MAKE_STRUCT_VIEW = eINSTANCE.getMakeStructView();

		/**
		 * The meta object literal for the '<em><b>Target Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAKE_STRUCT_VIEW__TARGET_VARIABLE = eINSTANCE.getMakeStructView_TargetVariable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewAndOrderByImpl <em>Aggregate Enrichment View And Order By</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.AggregateEnrichmentViewAndOrderByImpl
		 * @see org.eclipse.efbt.language.dtm.model.dtm_views.impl.dtm_viewsPackageImpl#getAggregateEnrichmentViewAndOrderBy()
		 * @generated
		 */
		EClass AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY = eINSTANCE.getAggregateEnrichmentViewAndOrderBy();

		/**
		 * The meta object literal for the '<em><b>Functions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS = eINSTANCE.getAggregateEnrichmentViewAndOrderBy_Functions();

		/**
		 * The meta object literal for the '<em><b>Group By Clause</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE = eINSTANCE.getAggregateEnrichmentViewAndOrderBy_GroupByClause();

		/**
		 * The meta object literal for the '<em><b>Order By Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE = eINSTANCE.getAggregateEnrichmentViewAndOrderBy_OrderByVariable();

	}

} //dtm_viewsPackage
