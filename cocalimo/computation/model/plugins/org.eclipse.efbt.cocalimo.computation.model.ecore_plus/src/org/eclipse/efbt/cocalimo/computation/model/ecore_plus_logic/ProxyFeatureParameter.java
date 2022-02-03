/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Proxy Feature Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Parameter where we are taking a CubeColumn from a cube.
 *  Speculative here relates to the fact that this cubeColumn may or may not actually exist. 
 *  Note that during development of a program that set of actually existing CubeColumns changes.
 *  Allowing a specultiveCubeColumnParameter    means that we can develop more quickly, and run checks occasionally to check that all SpeculativeCubeColumnParameters can be resolved  to real CubeColumns.
 *                
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getEntity <em>Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getVaraible <em>Varaible</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getProxyFeatureParameter()
 * @model extendedMetaData="name='SpeculativeCubeColumnParameter' kind='empty'"
 * @generated
 */
public interface ProxyFeatureParameter extends Parameter {
	/**
	 * Returns the value of the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cube of the desired cube Column
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Entity</em>' reference.
	 * @see #setEntity(Entity)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getProxyFeatureParameter_Entity()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	Entity getEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getEntity <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity</em>' reference.
	 * @see #getEntity()
	 * @generated
	 */
	void setEntity(Entity value);

	/**
	 * Returns the value of the '<em><b>Varaible</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Varaible</em>' reference.
	 * @see #setVaraible(VARIABLE)
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getProxyFeatureParameter_Varaible()
	 * @model
	 * @generated
	 */
	VARIABLE getVaraible();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getVaraible <em>Varaible</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Varaible</em>' reference.
	 * @see #getVaraible()
	 * @generated
	 */
	void setVaraible(VARIABLE value);

} // ProxyFeatureParameter
