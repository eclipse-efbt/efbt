/**
 */
package advanced_variable_lineagefunctions.impl;

import advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;
import advanced_variable_lineagefunctions.SpeculativeStructColumnParameter;

import functions.impl.ParameterImpl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Speculative Struct Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl#getColumnInsideStruct <em>Column Inside Struct</em>}</li>
 *   <li>{@link advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl#getStructColumn <em>Struct Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SpeculativeStructColumnParameterImpl extends ParameterImpl implements SpeculativeStructColumnParameter {
	/**
	 * The cached value of the '{@link #getColumnInsideStruct() <em>Column Inside Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnInsideStruct()
	 * @generated
	 * @ordered
	 */
	protected EAttribute columnInsideStruct;

	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected EClass cube;

	/**
	 * The cached value of the '{@link #getStructColumn() <em>Struct Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStructColumn()
	 * @generated
	 * @ordered
	 */
	protected EStructuralFeature structColumn;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SpeculativeStructColumnParameterImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Advanced_variable_lineagefunctionsPackage.Literals.SPECULATIVE_STRUCT_COLUMN_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getColumnInsideStruct() {
		if (columnInsideStruct != null && columnInsideStruct.eIsProxy()) {
			InternalEObject oldColumnInsideStruct = (InternalEObject)columnInsideStruct;
			columnInsideStruct = (EAttribute)eResolveProxy(oldColumnInsideStruct);
			if (columnInsideStruct != oldColumnInsideStruct) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT, oldColumnInsideStruct, columnInsideStruct));
			}
		}
		return columnInsideStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute basicGetColumnInsideStruct() {
		return columnInsideStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setColumnInsideStruct(EAttribute newColumnInsideStruct) {
		EAttribute oldColumnInsideStruct = columnInsideStruct;
		columnInsideStruct = newColumnInsideStruct;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT, oldColumnInsideStruct, columnInsideStruct));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCube() {
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (EClass)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass basicGetCube() {
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube(EClass newCube) {
		EClass oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EStructuralFeature getStructColumn() {
		if (structColumn != null && structColumn.eIsProxy()) {
			InternalEObject oldStructColumn = (InternalEObject)structColumn;
			structColumn = (EStructuralFeature)eResolveProxy(oldStructColumn);
			if (structColumn != oldStructColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN, oldStructColumn, structColumn));
			}
		}
		return structColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EStructuralFeature basicGetStructColumn() {
		return structColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setStructColumn(EStructuralFeature newStructColumn) {
		EStructuralFeature oldStructColumn = structColumn;
		structColumn = newStructColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN, oldStructColumn, structColumn));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				if (resolve) return getColumnInsideStruct();
				return basicGetColumnInsideStruct();
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN:
				if (resolve) return getStructColumn();
				return basicGetStructColumn();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				setColumnInsideStruct((EAttribute)newValue);
				return;
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE:
				setCube((EClass)newValue);
				return;
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN:
				setStructColumn((EStructuralFeature)newValue);
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
	public void eUnset(int featureID) {
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				setColumnInsideStruct((EAttribute)null);
				return;
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE:
				setCube((EClass)null);
				return;
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN:
				setStructColumn((EStructuralFeature)null);
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
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				return columnInsideStruct != null;
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE:
				return cube != null;
			case Advanced_variable_lineagefunctionsPackage.SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN:
				return structColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //SpeculativeStructColumnParameterImpl
