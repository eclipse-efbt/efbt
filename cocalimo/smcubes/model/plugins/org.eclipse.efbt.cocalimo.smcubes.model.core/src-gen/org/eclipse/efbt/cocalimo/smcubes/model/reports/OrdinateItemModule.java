/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Ordinate Item Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of OrdinateItems 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.OrdinateItemModule#getOrdinateItems <em>Ordinate Items</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getOrdinateItemModule()
 * @model extendedMetaData="name='OrdinateItemModule' kind='elementOnly'"
 * @generated
 */
public interface OrdinateItemModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Ordinate Items</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of OrdinateItems
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Ordinate Items</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getOrdinateItemModule_OrdinateItems()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='ordinateItems'"
	 * @generated
	 */
	EList<ORDINATE_ITEM> getOrdinateItems();

} // OrdinateItemModule
