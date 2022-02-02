/**
 */
package attribute_lineage;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute Lineage Model Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * a module containing a set of AttributeLineageModels
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link attribute_lineage.AttributeLineageModelModule#getAttributeLineageModels <em>Attribute Lineage Models</em>}</li>
 * </ul>
 *
 * @see attribute_lineage.Attribute_lineagePackage#getAttributeLineageModelModule()
 * @model extendedMetaData="name='AttributeLineageModelModule' kind='elementOnly'"
 * @generated
 */
public interface AttributeLineageModelModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Attribute Lineage Models</b></em>' containment reference list.
	 * The list contents are of type {@link attribute_lineage.AttributeLineageModel}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The AttributeLineageModels
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Attribute Lineage Models</em>' containment reference list.
	 * @see attribute_lineage.Attribute_lineagePackage#getAttributeLineageModelModule_AttributeLineageModels()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='attributeLineageModels'"
	 * @generated
	 */
	EList<AttributeLineageModel> getAttributeLineageModels();

} // AttributeLineageModelModule
