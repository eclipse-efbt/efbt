/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views;

import org.eclipse.efbt.language.trl.model.transformation.TransformationPackage;

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
 * This package describes the set of SQLViews, each SQLView represents a DataSetTransformation which transforms
 *     data from one DataSet to another in an immutable side-effect free fashion. They use concepts from the well-known SQL StructuredQueryLanguage.
 *     
 *     The transformed dataset is created just once by the transformation and then never amended (it is immutable)
 *     The only data that can be used in the transformation is the data from the source dataset(s), so it cannot use anything else including
 *     'global variables' (which do not exist in the compitational model, the only data created by the transformation is the data in the resulting transformed dataset,
 *     no other data is created or affected so the transformations are side-effect free.
 * 
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Trl_sql_viewsPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "trl_sql_views";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/trl_sql_views";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "trl_sql_views";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Trl_sql_viewsPackage eINSTANCE = org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SQLViewImpl <em>SQL View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SQLViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getSQLView()
	 * @generated
	 */
	int SQL_VIEW = 10;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_VIEW__CUBE = TransformationPackage.DATA_SET_TRANSFORMATION__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_VIEW__NAME = TransformationPackage.DATA_SET_TRANSFORMATION__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_VIEW__VTL_CODE = TransformationPackage.DATA_SET_TRANSFORMATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>SQL View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_VIEW_FEATURE_COUNT = TransformationPackage.DATA_SET_TRANSFORMATION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>SQL View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SQL_VIEW_OPERATION_COUNT = TransformationPackage.DATA_SET_TRANSFORMATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.AggregateEnrichmentViewImpl <em>Aggregate Enrichment View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.AggregateEnrichmentViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getAggregateEnrichmentView()
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
	int AGGREGATE_ENRICHMENT_VIEW__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Functions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__FUNCTIONS = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Group By Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW__GROUP_BY_CLAUSE = SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Aggregate Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Aggregate Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_ENRICHMENT_VIEW_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.CopyViewImpl <em>Copy View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.CopyViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getCopyView()
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
	int COPY_VIEW__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The number of structural features of the '<em>Copy View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Copy View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COPY_VIEW_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.EnrichmentViewImpl <em>Enrichment View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.EnrichmentViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getEnrichmentView()
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
	int ENRICHMENT_VIEW__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Functions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__FUNCTIONS = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Renames</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW__RENAMES = SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Enrichment View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENRICHMENT_VIEW_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.FilterByConditionViewImpl <em>Filter By Condition View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.FilterByConditionViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getFilterByConditionView()
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
	int FILTER_BY_CONDITION_VIEW__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW__WHERE_CLAUSE = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Filter By Condition View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Filter By Condition View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_BY_CONDITION_VIEW_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GenericViewImpl <em>Generic View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GenericViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getGenericView()
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
	int GENERIC_VIEW__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Select Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__SELECT_CLAUSE = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__WHERE_CLAUSE = SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Group By Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW__GROUP_BY_CLAUSE = SQL_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Generic View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Generic View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERIC_VIEW_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GroupByClauseImpl <em>Group By Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GroupByClauseImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getGroupByClause()
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
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.IgnoreColumnImpl <em>Ignore Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.IgnoreColumnImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getIgnoreColumn()
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
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.JoinViewImpl <em>Join View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.JoinViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getJoinView()
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
	int JOIN_VIEW__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Where Clause</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__WHERE_CLAUSE = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Renames</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__RENAMES = SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Ignores</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW__IGNORES = SQL_VIEW_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Join View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Join View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JOIN_VIEW_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.RenameAsImpl <em>Rename As</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.RenameAsImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getRenameAs()
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
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SelectClauseImpl <em>Select Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SelectClauseImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getSelectClause()
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
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.UnionViewImpl <em>Union View</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.UnionViewImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getUnionView()
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
	int UNION_VIEW__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The number of structural features of the '<em>Union View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Union View</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_VIEW_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.VersionedSQLViewsModuleImpl <em>Versioned SQL Views Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.VersionedSQLViewsModuleImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getVersionedSQLViewsModule()
	 * @generated
	 */
	int VERSIONED_SQL_VIEWS_MODULE = 12;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE__DEPENDENCIES = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE__THE_DESCRIPTION = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE__LICENSE = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE__NAME = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE__VERSION = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE__LONG_NAME = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Sql Views</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Versioned SQL Views Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE_FEATURE_COUNT = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Versioned SQL Views Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VERSIONED_SQL_VIEWS_MODULE_OPERATION_COUNT = TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.WhereClauseImpl <em>Where Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.WhereClauseImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getWhereClause()
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
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.BaseViewIncorporatingDeltasImpl <em>Base View Incorporating Deltas</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.BaseViewIncorporatingDeltasImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getBaseViewIncorporatingDeltas()
	 * @generated
	 */
	int BASE_VIEW_INCORPORATING_DELTAS = 14;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_INCORPORATING_DELTAS__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_INCORPORATING_DELTAS__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_INCORPORATING_DELTAS__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Ultimate Source Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base View Incorporating Deltas</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_INCORPORATING_DELTAS_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base View Incorporating Deltas</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_VIEW_INCORPORATING_DELTAS_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.DeltaAccumulationImpl <em>Delta Accumulation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.DeltaAccumulationImpl
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getDeltaAccumulation()
	 * @generated
	 */
	int DELTA_ACCUMULATION = 15;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION__CUBE = SQL_VIEW__CUBE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION__NAME = SQL_VIEW__NAME;

	/**
	 * The feature id for the '<em><b>VTL Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION__VTL_CODE = SQL_VIEW__VTL_CODE;

	/**
	 * The feature id for the '<em><b>Ultimate Source Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION__ULTIMATE_SOURCE_CUBE = SQL_VIEW_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Delta Accumulation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_FEATURE_COUNT = SQL_VIEW_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Delta Accumulation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DELTA_ACCUMULATION_OPERATION_COUNT = SQL_VIEW_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView <em>Aggregate Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Enrichment View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView
	 * @generated
	 */
	EClass getAggregateEnrichmentView();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView#getFunctions <em>Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Functions</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView#getFunctions()
	 * @see #getAggregateEnrichmentView()
	 * @generated
	 */
	EReference getAggregateEnrichmentView_Functions();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView#getGroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Group By Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView#getGroupByClause()
	 * @see #getAggregateEnrichmentView()
	 * @generated
	 */
	EReference getAggregateEnrichmentView_GroupByClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.CopyView <em>Copy View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Copy View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.CopyView
	 * @generated
	 */
	EClass getCopyView();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView <em>Enrichment View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Enrichment View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView
	 * @generated
	 */
	EClass getEnrichmentView();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView#getFunctions <em>Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Functions</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView#getFunctions()
	 * @see #getEnrichmentView()
	 * @generated
	 */
	EReference getEnrichmentView_Functions();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView#getRenames <em>Renames</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Renames</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView#getRenames()
	 * @see #getEnrichmentView()
	 * @generated
	 */
	EReference getEnrichmentView_Renames();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView <em>Filter By Condition View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Filter By Condition View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView
	 * @generated
	 */
	EClass getFilterByConditionView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView#getWhereClause()
	 * @see #getFilterByConditionView()
	 * @generated
	 */
	EReference getFilterByConditionView_WhereClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView <em>Generic View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generic View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView
	 * @generated
	 */
	EClass getGenericView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView#getSelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Select Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView#getSelectClause()
	 * @see #getGenericView()
	 * @generated
	 */
	EReference getGenericView_SelectClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView#getWhereClause()
	 * @see #getGenericView()
	 * @generated
	 */
	EReference getGenericView_WhereClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView#getGroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Group By Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GenericView#getGroupByClause()
	 * @see #getGenericView()
	 * @generated
	 */
	EReference getGenericView_GroupByClause();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause <em>Group By Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Group By Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause
	 * @generated
	 */
	EClass getGroupByClause();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause#getGroupByColumns <em>Group By Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Group By Columns</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.GroupByClause#getGroupByColumns()
	 * @see #getGroupByClause()
	 * @generated
	 */
	EReference getGroupByClause_GroupByColumns();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn <em>Ignore Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Ignore Column</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn
	 * @generated
	 */
	EClass getIgnoreColumn();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn#getColumnToIgnore <em>Column To Ignore</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column To Ignore</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn#getColumnToIgnore()
	 * @see #getIgnoreColumn()
	 * @generated
	 */
	EReference getIgnoreColumn_ColumnToIgnore();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.IgnoreColumn#getCube()
	 * @see #getIgnoreColumn()
	 * @generated
	 */
	EReference getIgnoreColumn_Cube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView <em>Join View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Join View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView
	 * @generated
	 */
	EClass getJoinView();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView#getWhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView#getWhereClause()
	 * @see #getJoinView()
	 * @generated
	 */
	EReference getJoinView_WhereClause();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView#getRenames <em>Renames</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Renames</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView#getRenames()
	 * @see #getJoinView()
	 * @generated
	 */
	EReference getJoinView_Renames();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView#getIgnores <em>Ignores</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Ignores</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView#getIgnores()
	 * @see #getJoinView()
	 * @generated
	 */
	EReference getJoinView_Ignores();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs <em>Rename As</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Rename As</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs
	 * @generated
	 */
	EClass getRenameAs();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromCube <em>From Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>From Cube</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromCube()
	 * @see #getRenameAs()
	 * @generated
	 */
	EReference getRenameAs_FromCube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromVariable <em>From Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>From Variable</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromVariable()
	 * @see #getRenameAs()
	 * @generated
	 */
	EReference getRenameAs_FromVariable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getToVariable <em>To Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>To Variable</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getToVariable()
	 * @see #getRenameAs()
	 * @generated
	 */
	EReference getRenameAs_ToVariable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause <em>Select Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Select Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause
	 * @generated
	 */
	EClass getSelectClause();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause#getAggregateFunctions <em>Aggregate Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Aggregate Functions</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause#getAggregateFunctions()
	 * @see #getSelectClause()
	 * @generated
	 */
	EReference getSelectClause_AggregateFunctions();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause#getBasicFunctions <em>Basic Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Basic Functions</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause#getBasicFunctions()
	 * @see #getSelectClause()
	 * @generated
	 */
	EReference getSelectClause_BasicFunctions();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause#getVaraibles <em>Varaibles</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Varaibles</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause#getVaraibles()
	 * @see #getSelectClause()
	 * @generated
	 */
	EReference getSelectClause_Varaibles();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView <em>SQL View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SQL View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView
	 * @generated
	 */
	EClass getSQLView();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView#getVTL_Code <em>VTL Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>VTL Code</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView#getVTL_Code()
	 * @see #getSQLView()
	 * @generated
	 */
	EAttribute getSQLView_VTL_Code();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.UnionView <em>Union View</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Union View</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.UnionView
	 * @generated
	 */
	EClass getUnionView();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule <em>Versioned SQL Views Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Versioned SQL Views Module</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule
	 * @generated
	 */
	EClass getVersionedSQLViewsModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule#getSqlViews <em>Sql Views</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sql Views</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule#getSqlViews()
	 * @see #getVersionedSQLViewsModule()
	 * @generated
	 */
	EReference getVersionedSQLViewsModule_SqlViews();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause <em>Where Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Where Clause</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause
	 * @generated
	 */
	EClass getWhereClause();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause#getFunction <em>Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Function</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause#getFunction()
	 * @see #getWhereClause()
	 * @generated
	 */
	EReference getWhereClause_Function();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas <em>Base View Incorporating Deltas</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base View Incorporating Deltas</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas
	 * @generated
	 */
	EClass getBaseViewIncorporatingDeltas();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas#getUltimateSourceCube <em>Ultimate Source Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Ultimate Source Cube</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas#getUltimateSourceCube()
	 * @see #getBaseViewIncorporatingDeltas()
	 * @generated
	 */
	EReference getBaseViewIncorporatingDeltas_UltimateSourceCube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation <em>Delta Accumulation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Delta Accumulation</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation
	 * @generated
	 */
	EClass getDeltaAccumulation();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation#getUltimateSourceCube <em>Ultimate Source Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Ultimate Source Cube</em>'.
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation#getUltimateSourceCube()
	 * @see #getDeltaAccumulation()
	 * @generated
	 */
	EReference getDeltaAccumulation_UltimateSourceCube();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Trl_sql_viewsFactory getTrl_sql_viewsFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.AggregateEnrichmentViewImpl <em>Aggregate Enrichment View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.AggregateEnrichmentViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getAggregateEnrichmentView()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.CopyViewImpl <em>Copy View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.CopyViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getCopyView()
		 * @generated
		 */
		EClass COPY_VIEW = eINSTANCE.getCopyView();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.EnrichmentViewImpl <em>Enrichment View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.EnrichmentViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getEnrichmentView()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.FilterByConditionViewImpl <em>Filter By Condition View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.FilterByConditionViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getFilterByConditionView()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GenericViewImpl <em>Generic View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GenericViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getGenericView()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GroupByClauseImpl <em>Group By Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.GroupByClauseImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getGroupByClause()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.IgnoreColumnImpl <em>Ignore Column</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.IgnoreColumnImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getIgnoreColumn()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.JoinViewImpl <em>Join View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.JoinViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getJoinView()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.RenameAsImpl <em>Rename As</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.RenameAsImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getRenameAs()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SelectClauseImpl <em>Select Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SelectClauseImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getSelectClause()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SQLViewImpl <em>SQL View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SQLViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getSQLView()
		 * @generated
		 */
		EClass SQL_VIEW = eINSTANCE.getSQLView();

		/**
		 * The meta object literal for the '<em><b>VTL Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SQL_VIEW__VTL_CODE = eINSTANCE.getSQLView_VTL_Code();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.UnionViewImpl <em>Union View</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.UnionViewImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getUnionView()
		 * @generated
		 */
		EClass UNION_VIEW = eINSTANCE.getUnionView();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.VersionedSQLViewsModuleImpl <em>Versioned SQL Views Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.VersionedSQLViewsModuleImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getVersionedSQLViewsModule()
		 * @generated
		 */
		EClass VERSIONED_SQL_VIEWS_MODULE = eINSTANCE.getVersionedSQLViewsModule();

		/**
		 * The meta object literal for the '<em><b>Sql Views</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS = eINSTANCE.getVersionedSQLViewsModule_SqlViews();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.WhereClauseImpl <em>Where Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.WhereClauseImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getWhereClause()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.BaseViewIncorporatingDeltasImpl <em>Base View Incorporating Deltas</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.BaseViewIncorporatingDeltasImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getBaseViewIncorporatingDeltas()
		 * @generated
		 */
		EClass BASE_VIEW_INCORPORATING_DELTAS = eINSTANCE.getBaseViewIncorporatingDeltas();

		/**
		 * The meta object literal for the '<em><b>Ultimate Source Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_VIEW_INCORPORATING_DELTAS__ULTIMATE_SOURCE_CUBE = eINSTANCE.getBaseViewIncorporatingDeltas_UltimateSourceCube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.DeltaAccumulationImpl <em>Delta Accumulation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.DeltaAccumulationImpl
		 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.impl.Trl_sql_viewsPackageImpl#getDeltaAccumulation()
		 * @generated
		 */
		EClass DELTA_ACCUMULATION = eINSTANCE.getDeltaAccumulation();

		/**
		 * The meta object literal for the '<em><b>Ultimate Source Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DELTA_ACCUMULATION__ULTIMATE_SOURCE_CUBE = eINSTANCE.getDeltaAccumulation_UltimateSourceCube();

	}

} //Trl_sql_viewsPackage
