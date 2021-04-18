/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.RowDataImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Derived Row Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedRowDataImpl#getFormulaCells <em>Formula Cells</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedRowDataImpl#getSpecialDerivedCells <em>Special Derived Cells</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedRowDataImpl#getSourceRows <em>Source Rows</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedRowDataImpl extends RowDataImpl implements DerivedRowData
{
	/**
	 * The cached value of the '{@link #getFormulaCells() <em>Formula Cells</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFormulaCells()
	 * @generated
	 * @ordered
	 */
	protected EList<FormulaCell> formulaCells;

	/**
	 * The cached value of the '{@link #getSpecialDerivedCells() <em>Special Derived Cells</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSpecialDerivedCells()
	 * @generated
	 * @ordered
	 */
	protected FormulaCell specialDerivedCells;

	/**
	 * The cached value of the '{@link #getSourceRows() <em>Source Rows</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceRows()
	 * @generated
	 * @ordered
	 */
	protected EList<RowData> sourceRows;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedRowDataImpl()
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
		return Cell_transformation_logicPackage.Literals.DERIVED_ROW_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<FormulaCell> getFormulaCells()
	{
		if (formulaCells == null) {
			formulaCells = new EObjectContainmentEList<FormulaCell>(FormulaCell.class, this, Cell_transformation_logicPackage.DERIVED_ROW_DATA__FORMULA_CELLS);
		}
		return formulaCells;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FormulaCell getSpecialDerivedCells()
	{
		return specialDerivedCells;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSpecialDerivedCells(FormulaCell newSpecialDerivedCells, NotificationChain msgs)
	{
		FormulaCell oldSpecialDerivedCells = specialDerivedCells;
		specialDerivedCells = newSpecialDerivedCells;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS, oldSpecialDerivedCells, newSpecialDerivedCells);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSpecialDerivedCells(FormulaCell newSpecialDerivedCells)
	{
		if (newSpecialDerivedCells != specialDerivedCells) {
			NotificationChain msgs = null;
			if (specialDerivedCells != null)
				msgs = ((InternalEObject)specialDerivedCells).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS, null, msgs);
			if (newSpecialDerivedCells != null)
				msgs = ((InternalEObject)newSpecialDerivedCells).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS, null, msgs);
			msgs = basicSetSpecialDerivedCells(newSpecialDerivedCells, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS, newSpecialDerivedCells, newSpecialDerivedCells));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RowData> getSourceRows()
	{
		if (sourceRows == null) {
			sourceRows = new EObjectResolvingEList<RowData>(RowData.class, this, Cell_transformation_logicPackage.DERIVED_ROW_DATA__SOURCE_ROWS);
		}
		return sourceRows;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__FORMULA_CELLS:
				return ((InternalEList<?>)getFormulaCells()).basicRemove(otherEnd, msgs);
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS:
				return basicSetSpecialDerivedCells(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__FORMULA_CELLS:
				return getFormulaCells();
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS:
				return getSpecialDerivedCells();
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SOURCE_ROWS:
				return getSourceRows();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__FORMULA_CELLS:
				getFormulaCells().clear();
				getFormulaCells().addAll((Collection<? extends FormulaCell>)newValue);
				return;
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS:
				setSpecialDerivedCells((FormulaCell)newValue);
				return;
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SOURCE_ROWS:
				getSourceRows().clear();
				getSourceRows().addAll((Collection<? extends RowData>)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__FORMULA_CELLS:
				getFormulaCells().clear();
				return;
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS:
				setSpecialDerivedCells((FormulaCell)null);
				return;
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SOURCE_ROWS:
				getSourceRows().clear();
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__FORMULA_CELLS:
				return formulaCells != null && !formulaCells.isEmpty();
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS:
				return specialDerivedCells != null;
			case Cell_transformation_logicPackage.DERIVED_ROW_DATA__SOURCE_ROWS:
				return sourceRows != null && !sourceRows.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DerivedRowDataImpl
