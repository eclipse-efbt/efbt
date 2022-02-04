/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Custom Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *  A CustomRowCreationApproach is responsible for creating rows and linking them to rows in the source cubes when we wish 	to do this in a way that doesn't follow common patterns such as filtering or grouping. 
 * Examples could be matching     	financial collateral and exposures in the most efficient way, which typically requires iterative or recursive logic.
 * The end result in this case would still be created rows which link back to the required collateral row and exposure row, even though the logic of finding the correct pairing of collateral and exposure may be quite complex.
 *             	 
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getCustomRowCreationApproach()
 * @model extendedMetaData="name='CustomRowCreationApproach' kind='elementOnly'"
 * @generated
 */
public interface CustomRowCreationApproach extends RowCreationApproach {
} // CustomRowCreationApproach
