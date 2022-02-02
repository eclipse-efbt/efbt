/**
 */
package attribute_lineage.impl;

import attribute_lineage.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Attribute_lineageFactoryImpl extends EFactoryImpl implements Attribute_lineageFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Attribute_lineageFactory init() {
		try {
			Attribute_lineageFactory theAttribute_lineageFactory = (Attribute_lineageFactory)EPackage.Registry.INSTANCE.getEFactory(Attribute_lineagePackage.eNS_URI);
			if (theAttribute_lineageFactory != null) {
				return theAttribute_lineageFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Attribute_lineageFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute_lineageFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL: return createAttributeLineageModel();
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL_MODULE: return createAttributeLineageModelModule();
			case Attribute_lineagePackage.IS_NOT_SUBSET: return createIsNotSubset();
			case Attribute_lineagePackage.IS_SUBSET: return createIsSubset();
			case Attribute_lineagePackage.SUBSET_BOOLEAN: return createSubsetBoolean();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeLineageModel createAttributeLineageModel() {
		AttributeLineageModelImpl attributeLineageModel = new AttributeLineageModelImpl();
		return attributeLineageModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeLineageModelModule createAttributeLineageModelModule() {
		AttributeLineageModelModuleImpl attributeLineageModelModule = new AttributeLineageModelModuleImpl();
		return attributeLineageModelModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public IsNotSubset createIsNotSubset() {
		IsNotSubsetImpl isNotSubset = new IsNotSubsetImpl();
		return isNotSubset;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public IsSubset createIsSubset() {
		IsSubsetImpl isSubset = new IsSubsetImpl();
		return isSubset;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SubsetBoolean createSubsetBoolean() {
		SubsetBooleanImpl subsetBoolean = new SubsetBooleanImpl();
		return subsetBoolean;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute_lineagePackage getAttribute_lineagePackage() {
		return (Attribute_lineagePackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Attribute_lineagePackage getPackage() {
		return Attribute_lineagePackage.eINSTANCE;
	}

} //Attribute_lineageFactoryImpl
