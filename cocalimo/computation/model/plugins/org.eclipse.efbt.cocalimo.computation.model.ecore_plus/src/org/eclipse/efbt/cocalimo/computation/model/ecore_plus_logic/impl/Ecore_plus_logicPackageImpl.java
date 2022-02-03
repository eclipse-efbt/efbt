/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OneToOneRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Parameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.UnionRowCreationApproach;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.Ecore_plus_structurePackageImpl;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore_plus_logicPackageImpl extends EPackageImpl implements Ecore_plus_logicPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass intermediateDerivedEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass generatedEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateFunctionSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicFunctionSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass booleanFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass functionSpecModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass paramaterSpecEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass parameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass valueParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aggregateDerivedFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicDerivedFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass basicScenarioLogicEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass baseFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass derivedFeatureGroupEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass featureParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass proxyFeatureParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass getAttributeFromEntityDerivedFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributeFromEntityParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass proxyAttributeFromEntityParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass customRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass filterAndGroupToOneRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowCreationApproachForEntityEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass oneToOneRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass filterRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass groupByRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowJoinFunctionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass unionRowCreationApproachEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass explodeArrayOfStructsRowFunctionEClass = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Ecore_plus_logicPackageImpl() {
		super(eNS_URI, Ecore_plus_logicFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link Ecore_plus_logicPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Ecore_plus_logicPackage init() {
		if (isInited) return (Ecore_plus_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_logicPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEcore_plus_logicPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Ecore_plus_logicPackageImpl theEcore_plus_logicPackage = registeredEcore_plus_logicPackage instanceof Ecore_plus_logicPackageImpl ? (Ecore_plus_logicPackageImpl)registeredEcore_plus_logicPackage : new Ecore_plus_logicPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		Logical_transformationsPackage.eINSTANCE.eClass();
		Bpmn_litePackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_structurePackage.eNS_URI);
		Ecore_plus_structurePackageImpl theEcore_plus_structurePackage = (Ecore_plus_structurePackageImpl)(registeredPackage instanceof Ecore_plus_structurePackageImpl ? registeredPackage : Ecore_plus_structurePackage.eINSTANCE);

		// Create package meta-data objects
		theEcore_plus_logicPackage.createPackageContents();
		theEcore_plus_structurePackage.createPackageContents();

		// Initialize created meta-data
		theEcore_plus_logicPackage.initializePackageContents();
		theEcore_plus_structurePackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theEcore_plus_logicPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Ecore_plus_logicPackage.eNS_URI, theEcore_plus_logicPackage);
		return theEcore_plus_logicPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedEntity() {
		return derivedEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntity_SourceEntities() {
		return (EReference)derivedEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntity_DerivedFeatures() {
		return (EReference)derivedEntityEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntity_RowCreationApproachForEntity() {
		return (EReference)derivedEntityEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedEntity_Cube() {
		return (EReference)derivedEntityEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getIntermediateDerivedEntity() {
		return intermediateDerivedEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGeneratedEntity() {
		return generatedEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAggregateFunction() {
		return aggregateFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAggregateFunction_FunctionSpec() {
		return (EReference)aggregateFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAggregateFunctionSpec() {
		return aggregateFunctionSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicFunction() {
		return basicFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicFunction_FunctionSpec() {
		return (EReference)basicFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicFunctionSpec() {
		return basicFunctionSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getBasicFunctionSpec_IsInfix() {
		return (EAttribute)basicFunctionSpecEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBooleanFunction() {
		return booleanFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFunction() {
		return functionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFunction_Parameters() {
		return (EReference)functionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunction_Name() {
		return (EAttribute)functionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFunctionSpec() {
		return functionSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFunctionSpec_ParameterSpecs() {
		return (EReference)functionSpecEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_FunctionalName() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_Name() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getFunctionSpec_SqlName() {
		return (EAttribute)functionSpecEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFunctionSpecModule() {
		return functionSpecModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFunctionSpecModule_FunctionSpecs() {
		return (EReference)functionSpecModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getParamaterSpec() {
		return paramaterSpecEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getParamaterSpec_Name() {
		return (EAttribute)paramaterSpecEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getParameter() {
		return parameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getMemberParameter() {
		return memberParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getMemberParameter_Param() {
		return (EReference)memberParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getValueParameter() {
		return valueParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getValueParameter_Value() {
		return (EAttribute)valueParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getValueParameter_DataType() {
		return (EReference)valueParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAggregateDerivedFeature() {
		return aggregateDerivedFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAggregateDerivedFeature_AggregateFunction() {
		return (EReference)aggregateDerivedFeatureEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicDerivedFeature() {
		return basicDerivedFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicDerivedFeature_BasicScenarioLogic() {
		return (EReference)basicDerivedFeatureEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBasicScenarioLogic() {
		return basicScenarioLogicEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicScenarioLogic_BasicFunction() {
		return (EReference)basicScenarioLogicEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBasicScenarioLogic_Sceanrio() {
		return (EReference)basicScenarioLogicEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedFeature() {
		return derivedFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedFeature_Variable() {
		return (EReference)derivedFeatureEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getBaseFeature() {
		return baseFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getBaseFeature_Variable() {
		return (EReference)baseFeatureEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDerivedFeatureGroup() {
		return derivedFeatureGroupEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDerivedFeatureGroup_DerivedFeatures() {
		return (EReference)derivedFeatureGroupEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFeatureParameter() {
		return featureParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFeatureParameter_Feature() {
		return (EReference)featureParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getProxyFeatureParameter() {
		return proxyFeatureParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getProxyFeatureParameter_Entity() {
		return (EReference)proxyFeatureParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getProxyFeatureParameter_Varaible() {
		return (EReference)proxyFeatureParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGetAttributeFromEntityDerivedFeature() {
		return getAttributeFromEntityDerivedFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGetAttributeFromEntityDerivedFeature_AttributeInEntity() {
		return (EReference)getAttributeFromEntityDerivedFeatureEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGetAttributeFromEntityDerivedFeature_EntityFeature() {
		return (EReference)getAttributeFromEntityDerivedFeatureEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAttributeFromEntityParameter() {
		return attributeFromEntityParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributeFromEntityParameter_AttributeInEntity() {
		return (EReference)attributeFromEntityParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributeFromEntityParameter_EntityFeature() {
		return (EReference)attributeFromEntityParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getProxyAttributeFromEntityParameter() {
		return proxyAttributeFromEntityParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getProxyAttributeFromEntityParameter_Variable() {
		return (EReference)proxyAttributeFromEntityParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getProxyAttributeFromEntityParameter_Entity() {
		return (EReference)proxyAttributeFromEntityParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getProxyAttributeFromEntityParameter_Cube() {
		return (EReference)proxyAttributeFromEntityParameterEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCustomRowCreationApproach() {
		return customRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getCustomRowCreationApproach_DependantVariables() {
		return (EReference)customRowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFilterAndGroupToOneRowCreationApproach() {
		return filterAndGroupToOneRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFilterAndGroupToOneRowCreationApproach_FilterFunction() {
		return (EReference)filterAndGroupToOneRowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFilterAndGroupToOneRowCreationApproach_GroupByVariables() {
		return (EReference)filterAndGroupToOneRowCreationApproachEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRowCreationApproachForEntity() {
		return rowCreationApproachForEntityEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowCreationApproachForEntity_RowCreationApproach() {
		return (EReference)rowCreationApproachForEntityEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowCreationApproachForEntity_Entity() {
		return (EReference)rowCreationApproachForEntityEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRowCreationApproachForEntity_Name() {
		return (EAttribute)rowCreationApproachForEntityEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getOneToOneRowCreationApproach() {
		return oneToOneRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getFilterRowCreationApproach() {
		return filterRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getFilterRowCreationApproach_FilterFunction() {
		return (EReference)filterRowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRowCreationApproach() {
		return rowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowCreationApproach_DependantCubeColumns() {
		return (EReference)rowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowCreationApproach_CreatedCubeColumns() {
		return (EReference)rowCreationApproachEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowCreationApproach_DependantStructItemColumns() {
		return (EReference)rowCreationApproachEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGroupByRowCreationApproach() {
		return groupByRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGroupByRowCreationApproach_GroupByCubeColumnNames() {
		return (EReference)groupByRowCreationApproachEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGroupByRowCreationApproach_GroupByColumns() {
		return (EReference)groupByRowCreationApproachEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRowJoinFunction() {
		return rowJoinFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRowJoinFunction_JoinFunction() {
		return (EReference)rowJoinFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getUnionRowCreationApproach() {
		return unionRowCreationApproachEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getExplodeArrayOfStructsRowFunction() {
		return explodeArrayOfStructsRowFunctionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getExplodeArrayOfStructsRowFunction_ArraySourceVariable() {
		return (EReference)explodeArrayOfStructsRowFunctionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_logicFactory getEcore_plus_logicFactory() {
		return (Ecore_plus_logicFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		derivedEntityEClass = createEClass(DERIVED_ENTITY);
		createEReference(derivedEntityEClass, DERIVED_ENTITY__SOURCE_ENTITIES);
		createEReference(derivedEntityEClass, DERIVED_ENTITY__DERIVED_FEATURES);
		createEReference(derivedEntityEClass, DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY);
		createEReference(derivedEntityEClass, DERIVED_ENTITY__CUBE);

		intermediateDerivedEntityEClass = createEClass(INTERMEDIATE_DERIVED_ENTITY);

		generatedEntityEClass = createEClass(GENERATED_ENTITY);

		aggregateFunctionEClass = createEClass(AGGREGATE_FUNCTION);
		createEReference(aggregateFunctionEClass, AGGREGATE_FUNCTION__FUNCTION_SPEC);

		aggregateFunctionSpecEClass = createEClass(AGGREGATE_FUNCTION_SPEC);

		basicFunctionEClass = createEClass(BASIC_FUNCTION);
		createEReference(basicFunctionEClass, BASIC_FUNCTION__FUNCTION_SPEC);

		basicFunctionSpecEClass = createEClass(BASIC_FUNCTION_SPEC);
		createEAttribute(basicFunctionSpecEClass, BASIC_FUNCTION_SPEC__IS_INFIX);

		booleanFunctionEClass = createEClass(BOOLEAN_FUNCTION);

		functionEClass = createEClass(FUNCTION);
		createEReference(functionEClass, FUNCTION__PARAMETERS);
		createEAttribute(functionEClass, FUNCTION__NAME);

		functionSpecEClass = createEClass(FUNCTION_SPEC);
		createEReference(functionSpecEClass, FUNCTION_SPEC__PARAMETER_SPECS);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__FUNCTIONAL_NAME);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__NAME);
		createEAttribute(functionSpecEClass, FUNCTION_SPEC__SQL_NAME);

		functionSpecModuleEClass = createEClass(FUNCTION_SPEC_MODULE);
		createEReference(functionSpecModuleEClass, FUNCTION_SPEC_MODULE__FUNCTION_SPECS);

		paramaterSpecEClass = createEClass(PARAMATER_SPEC);
		createEAttribute(paramaterSpecEClass, PARAMATER_SPEC__NAME);

		parameterEClass = createEClass(PARAMETER);

		memberParameterEClass = createEClass(MEMBER_PARAMETER);
		createEReference(memberParameterEClass, MEMBER_PARAMETER__PARAM);

		valueParameterEClass = createEClass(VALUE_PARAMETER);
		createEAttribute(valueParameterEClass, VALUE_PARAMETER__VALUE);
		createEReference(valueParameterEClass, VALUE_PARAMETER__DATA_TYPE);

		aggregateDerivedFeatureEClass = createEClass(AGGREGATE_DERIVED_FEATURE);
		createEReference(aggregateDerivedFeatureEClass, AGGREGATE_DERIVED_FEATURE__AGGREGATE_FUNCTION);

		basicDerivedFeatureEClass = createEClass(BASIC_DERIVED_FEATURE);
		createEReference(basicDerivedFeatureEClass, BASIC_DERIVED_FEATURE__BASIC_SCENARIO_LOGIC);

		basicScenarioLogicEClass = createEClass(BASIC_SCENARIO_LOGIC);
		createEReference(basicScenarioLogicEClass, BASIC_SCENARIO_LOGIC__BASIC_FUNCTION);
		createEReference(basicScenarioLogicEClass, BASIC_SCENARIO_LOGIC__SCEANRIO);

		derivedFeatureEClass = createEClass(DERIVED_FEATURE);
		createEReference(derivedFeatureEClass, DERIVED_FEATURE__VARIABLE);

		baseFeatureEClass = createEClass(BASE_FEATURE);
		createEReference(baseFeatureEClass, BASE_FEATURE__VARIABLE);

		derivedFeatureGroupEClass = createEClass(DERIVED_FEATURE_GROUP);
		createEReference(derivedFeatureGroupEClass, DERIVED_FEATURE_GROUP__DERIVED_FEATURES);

		featureParameterEClass = createEClass(FEATURE_PARAMETER);
		createEReference(featureParameterEClass, FEATURE_PARAMETER__FEATURE);

		proxyFeatureParameterEClass = createEClass(PROXY_FEATURE_PARAMETER);
		createEReference(proxyFeatureParameterEClass, PROXY_FEATURE_PARAMETER__ENTITY);
		createEReference(proxyFeatureParameterEClass, PROXY_FEATURE_PARAMETER__VARAIBLE);

		getAttributeFromEntityDerivedFeatureEClass = createEClass(GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE);
		createEReference(getAttributeFromEntityDerivedFeatureEClass, GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ATTRIBUTE_IN_ENTITY);
		createEReference(getAttributeFromEntityDerivedFeatureEClass, GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ENTITY_FEATURE);

		attributeFromEntityParameterEClass = createEClass(ATTRIBUTE_FROM_ENTITY_PARAMETER);
		createEReference(attributeFromEntityParameterEClass, ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY);
		createEReference(attributeFromEntityParameterEClass, ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE);

		proxyAttributeFromEntityParameterEClass = createEClass(PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER);
		createEReference(proxyAttributeFromEntityParameterEClass, PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__VARIABLE);
		createEReference(proxyAttributeFromEntityParameterEClass, PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY);
		createEReference(proxyAttributeFromEntityParameterEClass, PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__CUBE);

		customRowCreationApproachEClass = createEClass(CUSTOM_ROW_CREATION_APPROACH);
		createEReference(customRowCreationApproachEClass, CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES);

		filterAndGroupToOneRowCreationApproachEClass = createEClass(FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH);
		createEReference(filterAndGroupToOneRowCreationApproachEClass, FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION);
		createEReference(filterAndGroupToOneRowCreationApproachEClass, FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES);

		rowCreationApproachForEntityEClass = createEClass(ROW_CREATION_APPROACH_FOR_ENTITY);
		createEReference(rowCreationApproachForEntityEClass, ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH);
		createEReference(rowCreationApproachForEntityEClass, ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY);
		createEAttribute(rowCreationApproachForEntityEClass, ROW_CREATION_APPROACH_FOR_ENTITY__NAME);

		oneToOneRowCreationApproachEClass = createEClass(ONE_TO_ONE_ROW_CREATION_APPROACH);

		filterRowCreationApproachEClass = createEClass(FILTER_ROW_CREATION_APPROACH);
		createEReference(filterRowCreationApproachEClass, FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION);

		rowCreationApproachEClass = createEClass(ROW_CREATION_APPROACH);
		createEReference(rowCreationApproachEClass, ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS);
		createEReference(rowCreationApproachEClass, ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS);
		createEReference(rowCreationApproachEClass, ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS);

		groupByRowCreationApproachEClass = createEClass(GROUP_BY_ROW_CREATION_APPROACH);
		createEReference(groupByRowCreationApproachEClass, GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES);
		createEReference(groupByRowCreationApproachEClass, GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS);

		rowJoinFunctionEClass = createEClass(ROW_JOIN_FUNCTION);
		createEReference(rowJoinFunctionEClass, ROW_JOIN_FUNCTION__JOIN_FUNCTION);

		unionRowCreationApproachEClass = createEClass(UNION_ROW_CREATION_APPROACH);

		explodeArrayOfStructsRowFunctionEClass = createEClass(EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION);
		createEReference(explodeArrayOfStructsRowFunctionEClass, EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Ecore_plus_structurePackage theEcore_plus_structurePackage = (Ecore_plus_structurePackage)EPackage.Registry.INSTANCE.getEPackage(Ecore_plus_structurePackage.eNS_URI);
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);
		Logical_transformationsPackage theLogical_transformationsPackage = (Logical_transformationsPackage)EPackage.Registry.INSTANCE.getEPackage(Logical_transformationsPackage.eNS_URI);
		CorePackage theCorePackage = (CorePackage)EPackage.Registry.INSTANCE.getEPackage(CorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		derivedEntityEClass.getESuperTypes().add(theEcore_plus_structurePackage.getEntity());
		intermediateDerivedEntityEClass.getESuperTypes().add(this.getDerivedEntity());
		generatedEntityEClass.getESuperTypes().add(theEcore_plus_structurePackage.getEntity());
		aggregateFunctionEClass.getESuperTypes().add(this.getFunction());
		aggregateFunctionSpecEClass.getESuperTypes().add(this.getFunctionSpec());
		basicFunctionEClass.getESuperTypes().add(this.getFunction());
		basicFunctionSpecEClass.getESuperTypes().add(this.getFunctionSpec());
		booleanFunctionEClass.getESuperTypes().add(this.getBasicFunction());
		functionEClass.getESuperTypes().add(this.getParameter());
		functionSpecModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		memberParameterEClass.getESuperTypes().add(this.getParameter());
		valueParameterEClass.getESuperTypes().add(this.getParameter());
		aggregateDerivedFeatureEClass.getESuperTypes().add(this.getDerivedFeature());
		basicDerivedFeatureEClass.getESuperTypes().add(this.getDerivedFeature());
		derivedFeatureEClass.getESuperTypes().add(theEcorePackage.getEStructuralFeature());
		baseFeatureEClass.getESuperTypes().add(theEcorePackage.getEAttribute());
		featureParameterEClass.getESuperTypes().add(this.getParameter());
		proxyFeatureParameterEClass.getESuperTypes().add(this.getParameter());
		getAttributeFromEntityDerivedFeatureEClass.getESuperTypes().add(this.getDerivedFeature());
		attributeFromEntityParameterEClass.getESuperTypes().add(theEcorePackage.getEParameter());
		proxyAttributeFromEntityParameterEClass.getESuperTypes().add(this.getParameter());
		customRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		filterAndGroupToOneRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		oneToOneRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		filterRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		groupByRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		rowJoinFunctionEClass.getESuperTypes().add(this.getRowCreationApproach());
		unionRowCreationApproachEClass.getESuperTypes().add(this.getRowCreationApproach());
		explodeArrayOfStructsRowFunctionEClass.getESuperTypes().add(this.getRowCreationApproach());

		// Initialize classes, features, and operations; add parameters
		initEClass(derivedEntityEClass, DerivedEntity.class, "DerivedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedEntity_SourceEntities(), theEcore_plus_structurePackage.getEntity(), null, "sourceEntities", null, 0, -1, DerivedEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedEntity_DerivedFeatures(), this.getDerivedFeature(), null, "derivedFeatures", null, 0, -1, DerivedEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedEntity_RowCreationApproachForEntity(), this.getRowCreationApproachForEntity(), null, "rowCreationApproachForEntity", null, 0, 1, DerivedEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDerivedEntity_Cube(), theData_definitionPackage.getCUBE(), null, "cube", null, 0, 1, DerivedEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(intermediateDerivedEntityEClass, IntermediateDerivedEntity.class, "IntermediateDerivedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(generatedEntityEClass, GeneratedEntity.class, "GeneratedEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(aggregateFunctionEClass, AggregateFunction.class, "AggregateFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateFunction_FunctionSpec(), this.getAggregateFunctionSpec(), null, "functionSpec", null, 0, 1, AggregateFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(aggregateFunctionSpecEClass, AggregateFunctionSpec.class, "AggregateFunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(basicFunctionEClass, BasicFunction.class, "BasicFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicFunction_FunctionSpec(), this.getBasicFunctionSpec(), null, "functionSpec", null, 0, 1, BasicFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicFunctionSpecEClass, BasicFunctionSpec.class, "BasicFunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getBasicFunctionSpec_IsInfix(), ecorePackage.getEBoolean(), "isInfix", null, 0, 1, BasicFunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(booleanFunctionEClass, BooleanFunction.class, "BooleanFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(functionEClass, Function.class, "Function", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunction_Parameters(), this.getParameter(), null, "parameters", null, 0, -1, Function.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunction_Name(), ecorePackage.getEString(), "name", null, 0, 1, Function.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(functionSpecEClass, FunctionSpec.class, "FunctionSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionSpec_ParameterSpecs(), this.getParamaterSpec(), null, "parameterSpecs", null, 0, -1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_FunctionalName(), ecorePackage.getEString(), "functionalName", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_Name(), ecorePackage.getEString(), "name", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getFunctionSpec_SqlName(), ecorePackage.getEString(), "sqlName", null, 0, 1, FunctionSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(functionSpecModuleEClass, FunctionSpecModule.class, "FunctionSpecModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFunctionSpecModule_FunctionSpecs(), this.getFunctionSpec(), null, "functionSpecs", null, 0, -1, FunctionSpecModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(paramaterSpecEClass, ParamaterSpec.class, "ParamaterSpec", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getParamaterSpec_Name(), ecorePackage.getEString(), "name", null, 0, 1, ParamaterSpec.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(parameterEClass, Parameter.class, "Parameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(memberParameterEClass, MemberParameter.class, "MemberParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberParameter_Param(), theEcorePackage.getEEnumLiteral(), null, "param", null, 0, 1, MemberParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(valueParameterEClass, ValueParameter.class, "ValueParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getValueParameter_Value(), ecorePackage.getEString(), "value", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getValueParameter_DataType(), theEcorePackage.getEDataType(), null, "dataType", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(aggregateDerivedFeatureEClass, AggregateDerivedFeature.class, "AggregateDerivedFeature", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAggregateDerivedFeature_AggregateFunction(), this.getAggregateFunction(), null, "aggregateFunction", null, 0, 1, AggregateDerivedFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicDerivedFeatureEClass, BasicDerivedFeature.class, "BasicDerivedFeature", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicDerivedFeature_BasicScenarioLogic(), this.getBasicScenarioLogic(), null, "basicScenarioLogic", null, 0, -1, BasicDerivedFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(basicScenarioLogicEClass, BasicScenarioLogic.class, "BasicScenarioLogic", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBasicScenarioLogic_BasicFunction(), this.getBasicFunction(), null, "basicFunction", null, 0, 1, BasicScenarioLogic.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getBasicScenarioLogic_Sceanrio(), theLogical_transformationsPackage.getScenario(), null, "sceanrio", null, 0, 1, BasicScenarioLogic.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(derivedFeatureEClass, DerivedFeature.class, "DerivedFeature", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedFeature_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, DerivedFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(baseFeatureEClass, BaseFeature.class, "BaseFeature", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBaseFeature_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, BaseFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(derivedFeatureGroupEClass, DerivedFeatureGroup.class, "DerivedFeatureGroup", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDerivedFeatureGroup_DerivedFeatures(), this.getDerivedFeature(), null, "derivedFeatures", null, 0, -1, DerivedFeatureGroup.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(featureParameterEClass, FeatureParameter.class, "FeatureParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFeatureParameter_Feature(), theEcorePackage.getEStructuralFeature(), null, "feature", null, 0, 1, FeatureParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(proxyFeatureParameterEClass, ProxyFeatureParameter.class, "ProxyFeatureParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getProxyFeatureParameter_Entity(), theEcore_plus_structurePackage.getEntity(), null, "entity", null, 0, 1, ProxyFeatureParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getProxyFeatureParameter_Varaible(), theCorePackage.getVARIABLE(), null, "varaible", null, 0, 1, ProxyFeatureParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(getAttributeFromEntityDerivedFeatureEClass, GetAttributeFromEntityDerivedFeature.class, "GetAttributeFromEntityDerivedFeature", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGetAttributeFromEntityDerivedFeature_AttributeInEntity(), theEcorePackage.getEAttribute(), null, "attributeInEntity", null, 0, 1, GetAttributeFromEntityDerivedFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGetAttributeFromEntityDerivedFeature_EntityFeature(), theEcorePackage.getEStructuralFeature(), null, "entityFeature", null, 0, 1, GetAttributeFromEntityDerivedFeature.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(attributeFromEntityParameterEClass, AttributeFromEntityParameter.class, "AttributeFromEntityParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAttributeFromEntityParameter_AttributeInEntity(), theEcorePackage.getEAttribute(), null, "attributeInEntity", null, 0, 1, AttributeFromEntityParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAttributeFromEntityParameter_EntityFeature(), theEcorePackage.getEStructuralFeature(), null, "entityFeature", null, 0, 1, AttributeFromEntityParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(proxyAttributeFromEntityParameterEClass, ProxyAttributeFromEntityParameter.class, "ProxyAttributeFromEntityParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getProxyAttributeFromEntityParameter_Variable(), theCorePackage.getVARIABLE(), null, "variable", null, 0, 1, ProxyAttributeFromEntityParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getProxyAttributeFromEntityParameter_Entity(), theEcorePackage.getEClass(), null, "entity", null, 0, 1, ProxyAttributeFromEntityParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getProxyAttributeFromEntityParameter_Cube(), theData_definitionPackage.getCUBE(), null, "cube", null, 0, 1, ProxyAttributeFromEntityParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(customRowCreationApproachEClass, CustomRowCreationApproach.class, "CustomRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCustomRowCreationApproach_DependantVariables(), theEcorePackage.getEAttribute(), null, "dependantVariables", null, 0, -1, CustomRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(filterAndGroupToOneRowCreationApproachEClass, FilterAndGroupToOneRowCreationApproach.class, "FilterAndGroupToOneRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFilterAndGroupToOneRowCreationApproach_FilterFunction(), this.getBooleanFunction(), null, "filterFunction", null, 0, 1, FilterAndGroupToOneRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getFilterAndGroupToOneRowCreationApproach_GroupByVariables(), theEcorePackage.getEAttribute(), null, "groupByVariables", null, 0, -1, FilterAndGroupToOneRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(rowCreationApproachForEntityEClass, RowCreationApproachForEntity.class, "RowCreationApproachForEntity", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRowCreationApproachForEntity_RowCreationApproach(), this.getRowCreationApproach(), null, "rowCreationApproach", null, 0, 1, RowCreationApproachForEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRowCreationApproachForEntity_Entity(), theEcorePackage.getEClass(), null, "entity", null, 0, 1, RowCreationApproachForEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getRowCreationApproachForEntity_Name(), ecorePackage.getEString(), "name", null, 0, 1, RowCreationApproachForEntity.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(oneToOneRowCreationApproachEClass, OneToOneRowCreationApproach.class, "OneToOneRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(filterRowCreationApproachEClass, FilterRowCreationApproach.class, "FilterRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getFilterRowCreationApproach_FilterFunction(), this.getBooleanFunction(), null, "filterFunction", null, 0, 1, FilterRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(rowCreationApproachEClass, RowCreationApproach.class, "RowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRowCreationApproach_DependantCubeColumns(), theEcorePackage.getEStructuralFeature(), null, "dependantCubeColumns", null, 0, -1, RowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRowCreationApproach_CreatedCubeColumns(), theEcorePackage.getEAttribute(), null, "createdCubeColumns", null, 0, -1, RowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getRowCreationApproach_DependantStructItemColumns(), theEcorePackage.getEStructuralFeature(), null, "dependantStructItemColumns", null, 0, -1, RowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(groupByRowCreationApproachEClass, GroupByRowCreationApproach.class, "GroupByRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGroupByRowCreationApproach_GroupByCubeColumnNames(), theEcorePackage.getEAttribute(), null, "groupByCubeColumnNames", null, 0, -1, GroupByRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getGroupByRowCreationApproach_GroupByColumns(), theEcorePackage.getEAttribute(), null, "groupByColumns", null, 0, 1, GroupByRowCreationApproach.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(rowJoinFunctionEClass, RowJoinFunction.class, "RowJoinFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRowJoinFunction_JoinFunction(), this.getBooleanFunction(), null, "joinFunction", null, 0, 1, RowJoinFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(unionRowCreationApproachEClass, UnionRowCreationApproach.class, "UnionRowCreationApproach", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(explodeArrayOfStructsRowFunctionEClass, ExplodeArrayOfStructsRowFunction.class, "ExplodeArrayOfStructsRowFunction", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getExplodeArrayOfStructsRowFunction_ArraySourceVariable(), theEcorePackage.getEStructuralFeature(), null, "arraySourceVariable", null, 0, 1, ExplodeArrayOfStructsRowFunction.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (aggregateFunctionEClass,
		   source,
		   new String[] {
			   "name", "AggregateFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateFunction_FunctionSpec(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "functionSpec"
		   });
		addAnnotation
		  (aggregateFunctionSpecEClass,
		   source,
		   new String[] {
			   "name", "AggregateFunctionSpec",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (basicFunctionEClass,
		   source,
		   new String[] {
			   "name", "BasicFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getBasicFunction_FunctionSpec(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "functionSpec"
		   });
		addAnnotation
		  (basicFunctionSpecEClass,
		   source,
		   new String[] {
			   "name", "BasicFunctionSpec",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getBasicFunctionSpec_IsInfix(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "isInfix"
		   });
		addAnnotation
		  (booleanFunctionEClass,
		   source,
		   new String[] {
			   "name", "BooleanFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (functionEClass,
		   source,
		   new String[] {
			   "name", "Function",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunction_Parameters(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "parameters"
		   });
		addAnnotation
		  (getFunction_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (functionSpecEClass,
		   source,
		   new String[] {
			   "name", "FunctionSpec",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunctionSpec_ParameterSpecs(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "parameterSpecs"
		   });
		addAnnotation
		  (getFunctionSpec_FunctionalName(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "functionalName"
		   });
		addAnnotation
		  (getFunctionSpec_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getFunctionSpec_SqlName(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "sqlName"
		   });
		addAnnotation
		  (functionSpecModuleEClass,
		   source,
		   new String[] {
			   "name", "FunctionSpecModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFunctionSpecModule_FunctionSpecs(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "functionSpecs"
		   });
		addAnnotation
		  (paramaterSpecEClass,
		   source,
		   new String[] {
			   "name", "ParamaterSpec",
			   "kind", "empty"
		   });
		addAnnotation
		  (getParamaterSpec_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (parameterEClass,
		   source,
		   new String[] {
			   "name", "Parameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (memberParameterEClass,
		   source,
		   new String[] {
			   "name", "MemberParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMemberParameter_Param(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "param"
		   });
		addAnnotation
		  (valueParameterEClass,
		   source,
		   new String[] {
			   "name", "ValueParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getValueParameter_Value(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "value"
		   });
		addAnnotation
		  (aggregateDerivedFeatureEClass,
		   source,
		   new String[] {
			   "name", "AggregateColumnFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAggregateDerivedFeature_AggregateFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "aggregateFunction"
		   });
		addAnnotation
		  (basicDerivedFeatureEClass,
		   source,
		   new String[] {
			   "name", "BasicColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (getBasicScenarioLogic_BasicFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "basicFunction"
		   });
		addAnnotation
		  (derivedFeatureEClass,
		   source,
		   new String[] {
			   "name", "ColumnFunction",
			   "kind", "empty"
		   });
		addAnnotation
		  (featureParameterEClass,
		   source,
		   new String[] {
			   "name", "ResolvedCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getFeatureParameter_Feature(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cubeColumn"
		   });
		addAnnotation
		  (proxyFeatureParameterEClass,
		   source,
		   new String[] {
			   "name", "SpeculativeCubeColumnParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getProxyFeatureParameter_Entity(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (customRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "CustomRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (filterAndGroupToOneRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "FilterAndGroupToOneRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFilterAndGroupToOneRowCreationApproach_FilterFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "filterFunction"
		   });
		addAnnotation
		  (rowCreationApproachForEntityEClass,
		   source,
		   new String[] {
			   "name", "RowCreationApproachForCube",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getRowCreationApproachForEntity_RowCreationApproach(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "rowCreationApproach"
		   });
		addAnnotation
		  (getRowCreationApproachForEntity_Entity(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "cube"
		   });
		addAnnotation
		  (getRowCreationApproachForEntity_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (oneToOneRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "OneToOneRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (filterRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "FilterRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getFilterRowCreationApproach_FilterFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "filterFunction"
		   });
		addAnnotation
		  (rowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "RowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getRowCreationApproach_DependantCubeColumns(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "dependantCubeColumns"
		   });
		addAnnotation
		  (getRowCreationApproach_CreatedCubeColumns(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "createdCubeColumns"
		   });
		addAnnotation
		  (getRowCreationApproach_DependantStructItemColumns(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "dependantCubeColumns"
		   });
		addAnnotation
		  (groupByRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "GroupByRowCreationApproach",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getGroupByRowCreationApproach_GroupByCubeColumnNames(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "groupByCubeColumns"
		   });
		addAnnotation
		  (rowJoinFunctionEClass,
		   source,
		   new String[] {
			   "name", "RowJoinFunction",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getRowJoinFunction_JoinFunction(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "joinFunction"
		   });
		addAnnotation
		  (unionRowCreationApproachEClass,
		   source,
		   new String[] {
			   "name", "UnionRowCreationApproach",
			   "kind", "elementOnly"
		   });
	}

} //Ecore_plus_logicPackageImpl
