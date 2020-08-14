/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl;

import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Column Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * @generated
 */
public class ColumnFunctionImpl extends CubeColumnImpl implements ColumnFunction
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ColumnFunctionImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Column_transformation_logicPackage.Literals.COLUMN_FUNCTION;
	}

} //ColumnFunctionImpl
