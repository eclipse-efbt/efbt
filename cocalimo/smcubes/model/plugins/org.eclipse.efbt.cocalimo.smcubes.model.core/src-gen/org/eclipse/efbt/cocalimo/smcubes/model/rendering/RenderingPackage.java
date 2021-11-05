/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.rendering;

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
 * This package describes the rendering package from SMCubes, using fields from SDD, see ECB documentation for details
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\rThis file contains a representation of the rendering package  of the SMCubes Information Model. The SMCubes Information Model is freely available at https://www.ecb.europa.eu/stats/ecb_statistics/co-operation_and_standards/smcube/html/index.en.html'"
 * @generated
 */
public interface RenderingPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "rendering";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/rendering";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "rendering";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	RenderingPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXISImpl <em>AXIS</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXISImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getAXIS()
	 * @generated
	 */
	int AXIS = 0;

	/**
	 * The feature id for the '<em><b>Axis id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__AXIS_ID = 0;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__CODE = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Is open axis</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__IS_OPEN_AXIS = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__NAME = 4;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__ORDER = 5;

	/**
	 * The feature id for the '<em><b>Orientation</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__ORIENTATION = 6;

	/**
	 * The feature id for the '<em><b>Table id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS__TABLE_ID = 7;

	/**
	 * The number of structural features of the '<em>AXIS</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>AXIS</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl <em>AXIS ORDINATE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getAXIS_ORDINATE()
	 * @generated
	 */
	int AXIS_ORDINATE = 1;

	/**
	 * The feature id for the '<em><b>Axis id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__AXIS_ID = 0;

	/**
	 * The feature id for the '<em><b>Axis ordinate id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__AXIS_ORDINATE_ID = 1;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__CODE = 2;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__DESCRIPTION = 3;

	/**
	 * The feature id for the '<em><b>Is abstract header</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__IS_ABSTRACT_HEADER = 4;

	/**
	 * The feature id for the '<em><b>Level</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__LEVEL = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__NAME = 6;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__ORDER = 7;

	/**
	 * The feature id for the '<em><b>Parent axis ordinate id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID = 8;

	/**
	 * The feature id for the '<em><b>Path</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE__PATH = 9;

	/**
	 * The number of structural features of the '<em>AXIS ORDINATE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_FEATURE_COUNT = 10;

	/**
	 * The number of operations of the '<em>AXIS ORDINATE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.CELL_POSITIONImpl <em>CELL POSITION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.CELL_POSITIONImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getCELL_POSITION()
	 * @generated
	 */
	int CELL_POSITION = 2;

	/**
	 * The feature id for the '<em><b>Axis ordinate id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION__AXIS_ORDINATE_ID = 0;

	/**
	 * The feature id for the '<em><b>Cell id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION__CELL_ID = 1;

	/**
	 * The number of structural features of the '<em>CELL POSITION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>CELL POSITION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl <em>ORDINATE ITEM</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getORDINATE_ITEM()
	 * @generated
	 */
	int ORDINATE_ITEM = 3;

	/**
	 * The feature id for the '<em><b>Axis ordinate id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM__AXIS_ORDINATE_ID = 0;

	/**
	 * The feature id for the '<em><b>Is starting member included</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED = 1;

	/**
	 * The feature id for the '<em><b>Member hierarchy id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM__MEMBER_HIERARCHY_ID = 2;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM__MEMBER_ID = 3;

	/**
	 * The feature id for the '<em><b>Starting member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM__STARTING_MEMBER_ID = 4;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM__VARIABLE_ID = 5;

	/**
	 * The number of structural features of the '<em>ORDINATE ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>ORDINATE ITEM</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLEImpl <em>TABLE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLEImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getTABLE()
	 * @generated
	 */
	int TABLE = 4;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__MAINTENANCE_AGENCY_ID = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__NAME = 3;

	/**
	 * The feature id for the '<em><b>Table id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__TABLE_ID = 4;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__VALID_FROM = 5;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__VALID_TO = 6;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE__VERSION = 7;

	/**
	 * The number of structural features of the '<em>TABLE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>TABLE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLE_CELLImpl <em>TABLE CELL</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLE_CELLImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getTABLE_CELL()
	 * @generated
	 */
	int TABLE_CELL = 5;

	/**
	 * The feature id for the '<em><b>Cell id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL__CELL_ID = 0;

	/**
	 * The feature id for the '<em><b>Combination id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL__COMBINATION_ID = 1;

	/**
	 * The feature id for the '<em><b>Is shaded</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL__IS_SHADED = 2;

	/**
	 * The feature id for the '<em><b>Table id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL__TABLE_ID = 3;

	/**
	 * The number of structural features of the '<em>TABLE CELL</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>TABLE CELL</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS <em>AXIS</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>AXIS</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS
	 * @generated
	 */
	EClass getAXIS();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getAxis_id <em>Axis id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Axis id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getAxis_id()
	 * @see #getAXIS()
	 * @generated
	 */
	EAttribute getAXIS_Axis_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getCode()
	 * @see #getAXIS()
	 * @generated
	 */
	EAttribute getAXIS_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getDescription()
	 * @see #getAXIS()
	 * @generated
	 */
	EAttribute getAXIS_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getIs_open_axis <em>Is open axis</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is open axis</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getIs_open_axis()
	 * @see #getAXIS()
	 * @generated
	 */
	EAttribute getAXIS_Is_open_axis();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getName()
	 * @see #getAXIS()
	 * @generated
	 */
	EAttribute getAXIS_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getOrder()
	 * @see #getAXIS()
	 * @generated
	 */
	EAttribute getAXIS_Order();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getOrientation <em>Orientation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Orientation</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getOrientation()
	 * @see #getAXIS()
	 * @generated
	 */
	EAttribute getAXIS_Orientation();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getTable_id <em>Table id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Table id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS#getTable_id()
	 * @see #getAXIS()
	 * @generated
	 */
	EReference getAXIS_Table_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE <em>AXIS ORDINATE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>AXIS ORDINATE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE
	 * @generated
	 */
	EClass getAXIS_ORDINATE();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getAxis_id <em>Axis id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Axis id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getAxis_id()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EReference getAXIS_ORDINATE_Axis_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getAxis_ordinate_id <em>Axis ordinate id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Axis ordinate id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getAxis_ordinate_id()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Axis_ordinate_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getCode()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getDescription()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Description();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getIs_abstract_header <em>Is abstract header</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is abstract header</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getIs_abstract_header()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Is_abstract_header();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getLevel <em>Level</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Level</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getLevel()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Level();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getName()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getOrder()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Order();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getParent_axis_ordinate_id <em>Parent axis ordinate id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Parent axis ordinate id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getParent_axis_ordinate_id()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EReference getAXIS_ORDINATE_Parent_axis_ordinate_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getPath <em>Path</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Path</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE#getPath()
	 * @see #getAXIS_ORDINATE()
	 * @generated
	 */
	EAttribute getAXIS_ORDINATE_Path();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION <em>CELL POSITION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CELL POSITION</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION
	 * @generated
	 */
	EClass getCELL_POSITION();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getAxis_ordinate_id <em>Axis ordinate id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Axis ordinate id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getAxis_ordinate_id()
	 * @see #getCELL_POSITION()
	 * @generated
	 */
	EReference getCELL_POSITION_Axis_ordinate_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getCell_id <em>Cell id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cell id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getCell_id()
	 * @see #getCELL_POSITION()
	 * @generated
	 */
	EReference getCELL_POSITION_Cell_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM <em>ORDINATE ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>ORDINATE ITEM</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM
	 * @generated
	 */
	EClass getORDINATE_ITEM();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getAxis_ordinate_id <em>Axis ordinate id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Axis ordinate id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getAxis_ordinate_id()
	 * @see #getORDINATE_ITEM()
	 * @generated
	 */
	EReference getORDINATE_ITEM_Axis_ordinate_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getIs_starting_member_included <em>Is starting member included</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is starting member included</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getIs_starting_member_included()
	 * @see #getORDINATE_ITEM()
	 * @generated
	 */
	EAttribute getORDINATE_ITEM_Is_starting_member_included();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_hierarchy_id <em>Member hierarchy id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member hierarchy id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_hierarchy_id()
	 * @see #getORDINATE_ITEM()
	 * @generated
	 */
	EReference getORDINATE_ITEM_Member_hierarchy_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getMember_id()
	 * @see #getORDINATE_ITEM()
	 * @generated
	 */
	EReference getORDINATE_ITEM_Member_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getStarting_member_id <em>Starting member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Starting member id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getStarting_member_id()
	 * @see #getORDINATE_ITEM()
	 * @generated
	 */
	EReference getORDINATE_ITEM_Starting_member_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getVariable_id <em>Variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM#getVariable_id()
	 * @see #getORDINATE_ITEM()
	 * @generated
	 */
	EReference getORDINATE_ITEM_Variable_id();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE <em>TABLE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>TABLE</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE
	 * @generated
	 */
	EClass getTABLE();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getCode()
	 * @see #getTABLE()
	 * @generated
	 */
	EAttribute getTABLE_Code();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getDescription()
	 * @see #getTABLE()
	 * @generated
	 */
	EAttribute getTABLE_Description();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getMaintenance_agency_id()
	 * @see #getTABLE()
	 * @generated
	 */
	EReference getTABLE_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getName()
	 * @see #getTABLE()
	 * @generated
	 */
	EAttribute getTABLE_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getTable_id <em>Table id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Table id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getTable_id()
	 * @see #getTABLE()
	 * @generated
	 */
	EAttribute getTABLE_Table_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getValid_from()
	 * @see #getTABLE()
	 * @generated
	 */
	EAttribute getTABLE_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getValid_to()
	 * @see #getTABLE()
	 * @generated
	 */
	EAttribute getTABLE_Valid_to();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE#getVersion()
	 * @see #getTABLE()
	 * @generated
	 */
	EAttribute getTABLE_Version();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL <em>TABLE CELL</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>TABLE CELL</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL
	 * @generated
	 */
	EClass getTABLE_CELL();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCell_id <em>Cell id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cell id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCell_id()
	 * @see #getTABLE_CELL()
	 * @generated
	 */
	EAttribute getTABLE_CELL_Cell_id();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCombination_id <em>Combination id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Combination id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCombination_id()
	 * @see #getTABLE_CELL()
	 * @generated
	 */
	EReference getTABLE_CELL_Combination_id();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getIs_shaded <em>Is shaded</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is shaded</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getIs_shaded()
	 * @see #getTABLE_CELL()
	 * @generated
	 */
	EAttribute getTABLE_CELL_Is_shaded();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getTable_id <em>Table id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Table id</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getTable_id()
	 * @see #getTABLE_CELL()
	 * @generated
	 */
	EReference getTABLE_CELL_Table_id();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	RenderingFactory getRenderingFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXISImpl <em>AXIS</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXISImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getAXIS()
		 * @generated
		 */
		EClass AXIS = eINSTANCE.getAXIS();

		/**
		 * The meta object literal for the '<em><b>Axis id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS__AXIS_ID = eINSTANCE.getAXIS_Axis_id();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS__CODE = eINSTANCE.getAXIS_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS__DESCRIPTION = eINSTANCE.getAXIS_Description();

		/**
		 * The meta object literal for the '<em><b>Is open axis</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS__IS_OPEN_AXIS = eINSTANCE.getAXIS_Is_open_axis();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS__NAME = eINSTANCE.getAXIS_Name();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS__ORDER = eINSTANCE.getAXIS_Order();

		/**
		 * The meta object literal for the '<em><b>Orientation</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS__ORIENTATION = eINSTANCE.getAXIS_Orientation();

		/**
		 * The meta object literal for the '<em><b>Table id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AXIS__TABLE_ID = eINSTANCE.getAXIS_Table_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl <em>AXIS ORDINATE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getAXIS_ORDINATE()
		 * @generated
		 */
		EClass AXIS_ORDINATE = eINSTANCE.getAXIS_ORDINATE();

		/**
		 * The meta object literal for the '<em><b>Axis id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AXIS_ORDINATE__AXIS_ID = eINSTANCE.getAXIS_ORDINATE_Axis_id();

		/**
		 * The meta object literal for the '<em><b>Axis ordinate id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__AXIS_ORDINATE_ID = eINSTANCE.getAXIS_ORDINATE_Axis_ordinate_id();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__CODE = eINSTANCE.getAXIS_ORDINATE_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__DESCRIPTION = eINSTANCE.getAXIS_ORDINATE_Description();

		/**
		 * The meta object literal for the '<em><b>Is abstract header</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__IS_ABSTRACT_HEADER = eINSTANCE.getAXIS_ORDINATE_Is_abstract_header();

		/**
		 * The meta object literal for the '<em><b>Level</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__LEVEL = eINSTANCE.getAXIS_ORDINATE_Level();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__NAME = eINSTANCE.getAXIS_ORDINATE_Name();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__ORDER = eINSTANCE.getAXIS_ORDINATE_Order();

		/**
		 * The meta object literal for the '<em><b>Parent axis ordinate id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID = eINSTANCE.getAXIS_ORDINATE_Parent_axis_ordinate_id();

		/**
		 * The meta object literal for the '<em><b>Path</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AXIS_ORDINATE__PATH = eINSTANCE.getAXIS_ORDINATE_Path();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.CELL_POSITIONImpl <em>CELL POSITION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.CELL_POSITIONImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getCELL_POSITION()
		 * @generated
		 */
		EClass CELL_POSITION = eINSTANCE.getCELL_POSITION();

		/**
		 * The meta object literal for the '<em><b>Axis ordinate id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CELL_POSITION__AXIS_ORDINATE_ID = eINSTANCE.getCELL_POSITION_Axis_ordinate_id();

		/**
		 * The meta object literal for the '<em><b>Cell id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CELL_POSITION__CELL_ID = eINSTANCE.getCELL_POSITION_Cell_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl <em>ORDINATE ITEM</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getORDINATE_ITEM()
		 * @generated
		 */
		EClass ORDINATE_ITEM = eINSTANCE.getORDINATE_ITEM();

		/**
		 * The meta object literal for the '<em><b>Axis ordinate id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ORDINATE_ITEM__AXIS_ORDINATE_ID = eINSTANCE.getORDINATE_ITEM_Axis_ordinate_id();

		/**
		 * The meta object literal for the '<em><b>Is starting member included</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED = eINSTANCE.getORDINATE_ITEM_Is_starting_member_included();

		/**
		 * The meta object literal for the '<em><b>Member hierarchy id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ORDINATE_ITEM__MEMBER_HIERARCHY_ID = eINSTANCE.getORDINATE_ITEM_Member_hierarchy_id();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ORDINATE_ITEM__MEMBER_ID = eINSTANCE.getORDINATE_ITEM_Member_id();

		/**
		 * The meta object literal for the '<em><b>Starting member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ORDINATE_ITEM__STARTING_MEMBER_ID = eINSTANCE.getORDINATE_ITEM_Starting_member_id();

		/**
		 * The meta object literal for the '<em><b>Variable id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ORDINATE_ITEM__VARIABLE_ID = eINSTANCE.getORDINATE_ITEM_Variable_id();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLEImpl <em>TABLE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLEImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getTABLE()
		 * @generated
		 */
		EClass TABLE = eINSTANCE.getTABLE();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE__CODE = eINSTANCE.getTABLE_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE__DESCRIPTION = eINSTANCE.getTABLE_Description();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TABLE__MAINTENANCE_AGENCY_ID = eINSTANCE.getTABLE_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE__NAME = eINSTANCE.getTABLE_Name();

		/**
		 * The meta object literal for the '<em><b>Table id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE__TABLE_ID = eINSTANCE.getTABLE_Table_id();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE__VALID_FROM = eINSTANCE.getTABLE_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE__VALID_TO = eINSTANCE.getTABLE_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE__VERSION = eINSTANCE.getTABLE_Version();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLE_CELLImpl <em>TABLE CELL</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.TABLE_CELLImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.RenderingPackageImpl#getTABLE_CELL()
		 * @generated
		 */
		EClass TABLE_CELL = eINSTANCE.getTABLE_CELL();

		/**
		 * The meta object literal for the '<em><b>Cell id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE_CELL__CELL_ID = eINSTANCE.getTABLE_CELL_Cell_id();

		/**
		 * The meta object literal for the '<em><b>Combination id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TABLE_CELL__COMBINATION_ID = eINSTANCE.getTABLE_CELL_Combination_id();

		/**
		 * The meta object literal for the '<em><b>Is shaded</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TABLE_CELL__IS_SHADED = eINSTANCE.getTABLE_CELL_Is_shaded();

		/**
		 * The meta object literal for the '<em><b>Table id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TABLE_CELL__TABLE_ID = eINSTANCE.getTABLE_CELL_Table_id();

	}

} //RenderingPackage
