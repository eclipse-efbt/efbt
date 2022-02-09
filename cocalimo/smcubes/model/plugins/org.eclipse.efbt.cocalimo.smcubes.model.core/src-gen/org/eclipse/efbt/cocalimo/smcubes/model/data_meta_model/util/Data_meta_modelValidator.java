/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.util;

import java.util.Map;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.*;

import org.eclipse.emf.common.util.Diagnostic;
import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.EObjectValidator;

import org.eclipse.emf.ecore.xml.type.util.XMLTypeValidator;

/**
 * <!-- begin-user-doc -->
 * The <b>Validator</b> for the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage
 * @generated
 */
public class Data_meta_modelValidator extends EObjectValidator {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static final Data_meta_modelValidator INSTANCE = new Data_meta_modelValidator();

	/**
	 * A constant for the {@link org.eclipse.emf.common.util.Diagnostic#getSource() source} of diagnostic {@link org.eclipse.emf.common.util.Diagnostic#getCode() codes} from this package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.common.util.Diagnostic#getSource()
	 * @see org.eclipse.emf.common.util.Diagnostic#getCode()
	 * @generated
	 */
	public static final String DIAGNOSTIC_SOURCE = "org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model";

	/**
	 * A constant with a fixed name that can be used as the base value for additional hand written constants.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static final int GENERATED_DIAGNOSTIC_CODE_COUNT = 0;

	/**
	 * A constant with a fixed name that can be used as the base value for additional hand written constants in a derived class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static final int DIAGNOSTIC_CODE_COUNT = GENERATED_DIAGNOSTIC_CODE_COUNT;

	/**
	 * The cached base package validator.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected XMLTypeValidator xmlTypeValidator;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_meta_modelValidator() {
		super();
		xmlTypeValidator = XMLTypeValidator.INSTANCE;
	}

	/**
	 * Returns the package of this validator switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EPackage getEPackage() {
	  return Data_meta_modelPackage.eINSTANCE;
	}

	/**
	 * Calls <code>validateXXX</code> for the corresponding classifier of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected boolean validate(int classifierID, Object value, DiagnosticChain diagnostics, Map<Object, Object> context) {
		switch (classifierID) {
			case Data_meta_modelPackage.ATTRIBUTE:
				return validateAttribute((Attribute)value, diagnostics, context);
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE:
				return validateForeignKeyAttribute((ForeignKeyAttribute)value, diagnostics, context);
			case Data_meta_modelPackage.CLASSIFIER:
				return validateClassifier((Classifier)value, diagnostics, context);
			case Data_meta_modelPackage.ELEMENT:
				return validateElement((Element)value, diagnostics, context);
			case Data_meta_modelPackage.NAMED_ELEMENT:
				return validateNamedElement((NamedElement)value, diagnostics, context);
			case Data_meta_modelPackage.ENTITY_MODULE:
				return validateEntityModule((EntityModule)value, diagnostics, context);
			case Data_meta_modelPackage.STRUCTURAL_FEATURE:
				return validateStructuralFeature((StructuralFeature)value, diagnostics, context);
			case Data_meta_modelPackage.TYPED_ELEMENT:
				return validateTypedElement((TypedElement)value, diagnostics, context);
			case Data_meta_modelPackage.BASE_ENTITY:
				return validateBaseEntity((BaseEntity)value, diagnostics, context);
			case Data_meta_modelPackage.GENERATED_ENTITY:
				return validateGeneratedEntity((GeneratedEntity)value, diagnostics, context);
			case Data_meta_modelPackage.ENTITY:
				return validateEntity((Entity)value, diagnostics, context);
			case Data_meta_modelPackage.DERIVED_ENTITY:
				return validateDerivedEntity((DerivedEntity)value, diagnostics, context);
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT:
				return validateReturnedTypedElement((ReturnedTypedElement)value, diagnostics, context);
			case Data_meta_modelPackage.BOOLEAN:
				return validateBoolean(value, diagnostics, context);
			case Data_meta_modelPackage.INT:
				return validateInt(value, diagnostics, context);
			case Data_meta_modelPackage.STRING:
				return validateString(value, diagnostics, context);
			default:
				return true;
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAttribute(Attribute attribute, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(attribute, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateForeignKeyAttribute(ForeignKeyAttribute foreignKeyAttribute, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(foreignKeyAttribute, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateClassifier(Classifier classifier, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(classifier, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateElement(Element element, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(element, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateNamedElement(NamedElement namedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(namedElement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateEntityModule(EntityModule entityModule, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(entityModule, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateStructuralFeature(StructuralFeature structuralFeature, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(structuralFeature, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTypedElement(TypedElement typedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(typedElement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateBaseEntity(BaseEntity baseEntity, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(baseEntity, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateGeneratedEntity(GeneratedEntity generatedEntity, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(generatedEntity, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateEntity(Entity entity, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(entity, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateDerivedEntity(DerivedEntity derivedEntity, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(derivedEntity, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateReturnedTypedElement(ReturnedTypedElement returnedTypedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		if (!validate_NoCircularContainment(returnedTypedElement, diagnostics, context)) return false;
		boolean result = validate_EveryMultiplicityConforms(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryDataValueConforms(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryReferenceIsContained(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryBidirectionalReferenceIsPaired(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryProxyResolves(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validate_UniqueID(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryKeyUnique(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryMapEntryUnique(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validateReturnedTypedElement_ValidLowerBound(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validateReturnedTypedElement_ValidUpperBound(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validateReturnedTypedElement_ConsistentBounds(returnedTypedElement, diagnostics, context);
		if (result || diagnostics != null) result &= validateReturnedTypedElement_ValidType(returnedTypedElement, diagnostics, context);
		return result;
	}

	/**
	 * Validates the ValidLowerBound constraint of '<em>Returned Typed Element</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateReturnedTypedElement_ValidLowerBound(ReturnedTypedElement returnedTypedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		// TODO implement the constraint
		// -> specify the condition that violates the constraint
		// -> verify the diagnostic details, including severity, code, and message
		// Ensure that you remove @generated or mark it @generated NOT
		if (false) {
			if (diagnostics != null) {
				diagnostics.add
					(createDiagnostic
						(Diagnostic.ERROR,
						 DIAGNOSTIC_SOURCE,
						 0,
						 "_UI_GenericConstraint_diagnostic",
						 new Object[] { "ValidLowerBound", getObjectLabel(returnedTypedElement, context) },
						 new Object[] { returnedTypedElement },
						 context));
			}
			return false;
		}
		return true;
	}

	/**
	 * Validates the ValidUpperBound constraint of '<em>Returned Typed Element</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateReturnedTypedElement_ValidUpperBound(ReturnedTypedElement returnedTypedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		// TODO implement the constraint
		// -> specify the condition that violates the constraint
		// -> verify the diagnostic details, including severity, code, and message
		// Ensure that you remove @generated or mark it @generated NOT
		if (false) {
			if (diagnostics != null) {
				diagnostics.add
					(createDiagnostic
						(Diagnostic.ERROR,
						 DIAGNOSTIC_SOURCE,
						 0,
						 "_UI_GenericConstraint_diagnostic",
						 new Object[] { "ValidUpperBound", getObjectLabel(returnedTypedElement, context) },
						 new Object[] { returnedTypedElement },
						 context));
			}
			return false;
		}
		return true;
	}

	/**
	 * Validates the ConsistentBounds constraint of '<em>Returned Typed Element</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateReturnedTypedElement_ConsistentBounds(ReturnedTypedElement returnedTypedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		// TODO implement the constraint
		// -> specify the condition that violates the constraint
		// -> verify the diagnostic details, including severity, code, and message
		// Ensure that you remove @generated or mark it @generated NOT
		if (false) {
			if (diagnostics != null) {
				diagnostics.add
					(createDiagnostic
						(Diagnostic.ERROR,
						 DIAGNOSTIC_SOURCE,
						 0,
						 "_UI_GenericConstraint_diagnostic",
						 new Object[] { "ConsistentBounds", getObjectLabel(returnedTypedElement, context) },
						 new Object[] { returnedTypedElement },
						 context));
			}
			return false;
		}
		return true;
	}

	/**
	 * Validates the ValidType constraint of '<em>Returned Typed Element</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateReturnedTypedElement_ValidType(ReturnedTypedElement returnedTypedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		// TODO implement the constraint
		// -> specify the condition that violates the constraint
		// -> verify the diagnostic details, including severity, code, and message
		// Ensure that you remove @generated or mark it @generated NOT
		if (false) {
			if (diagnostics != null) {
				diagnostics.add
					(createDiagnostic
						(Diagnostic.ERROR,
						 DIAGNOSTIC_SOURCE,
						 0,
						 "_UI_GenericConstraint_diagnostic",
						 new Object[] { "ValidType", getObjectLabel(returnedTypedElement, context) },
						 new Object[] { returnedTypedElement },
						 context));
			}
			return false;
		}
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateBoolean(Object boolean_, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateInt(Object int_, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateString(Object string, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * Returns the resource locator that will be used to fetch messages for this validator's diagnostics.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		// TODO
		// Specialize this to return a resource locator for messages specific to this validator.
		// Ensure that you remove @generated or mark it @generated NOT
		return super.getResourceLocator();
	}

} //Data_meta_modelValidator
