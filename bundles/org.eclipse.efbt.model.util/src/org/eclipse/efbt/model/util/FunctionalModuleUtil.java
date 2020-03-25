/*******************************************************************************
 * Copyright (c) 2019 Openregtech Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Neil Mackenzie - initial API and implementation
 *  */

package org.eclipse.efbt.model.util;

import java.util.Iterator;
import java.util.List;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;

import domain_model_mapping.CodeCondition;
import domain_model_mapping.ColumnSettingCondition;
import domain_model_mapping.DataSetFilterCondition;
import domain_model_mapping.DomainMappingModule;
import domain_model_mapping.LeafModuleToVersionedSQLViewsModule;
import domain_model_mapping.ScenarioToCodeConditions;
import functional_module.FunctionalModule;
import functional_module.FunctionalModuleSet;
import functional_module.LeafFunctionalModule;
import functional_module.ScenarioSetFunctionalModule;
import functional_module.SerialFunctionalModule;
import functions.BooleanFunction;
import navigation_context.NavigationContext;
import requirements_text.TitledRequirementsSection;
import scenarios.Scenario;
import tags.FunctionalModuleTag;
import tags.ScenarioTag;
import trl_sql_views.VersionedSQLViewsModule;



/**
 * @author Neil Mackenzie
 *
 */
public class FunctionalModuleUtil {

  /**
   * Given a FunctionalModule this method returns a list of the LeafFunctionalModule
   * which it is dependent upon. These will be FunctionalModules prior to this in a
   * SerialTransformationSchemSet, and child FunctionalModules, if the FunctionalModule
   * is a SerialTransformationSchemSet, and also FunctionalModules usedBy the 
   * FunctionalModule. Also it will include recursivley any transformation schemes that
   * those depend upon.
   * 
   * @param FunctionalModule
   * @return
   */
  static List<LeafFunctionalModule>
    getFunctionalModuleDependencies(LeafFunctionalModule FunctionalModule) {
    
    List<LeafFunctionalModule> transformationDependencies =
        new BasicEList<LeafFunctionalModule>();
    List<FunctionalModule> parentsAndYoungerSiblingsOfTranformation =
        new BasicEList<FunctionalModule>();
    addParentsAndYoungerSiblingsOfToList(
        FunctionalModule, parentsAndYoungerSiblingsOfTranformation);
    // traverse the list, add transformations to the list, and all children of
    // transformation schemes
    addChildrenTransformationsOfSchemesToList(
        parentsAndYoungerSiblingsOfTranformation, transformationDependencies);
    transformationDependencies.add(FunctionalModule);
    return transformationDependencies;
  }

  
  /**
   * Aadd the parents and younger siblings of the LeafFunctionalModules in the list
   * and add these to a list called parentsAndYoungerSiblingsOfTranformationScheme.
   * 
   * @param parentsAndYoungerSiblingsOfTranformationScheme
   * @param list
   */
  private static void addChildrenTransformationsOfSchemesToList(
      List<FunctionalModule> parentsAndYoungerSiblingsOfTranformationScheme, List<LeafFunctionalModule> list) {

    for (Iterator iterator = parentsAndYoungerSiblingsOfTranformationScheme.iterator(); iterator.hasNext();) {
      FunctionalModule FunctionalModule = (FunctionalModule) iterator.next();
      if (FunctionalModule instanceof LeafFunctionalModule) {
        list.add((LeafFunctionalModule) FunctionalModule);
      }
      if (FunctionalModule instanceof FunctionalModuleSet) {
        addDecendentFunctionalModulesOfFunctionalModuleToList((FunctionalModuleSet) FunctionalModule, list);
      }

    }

  }

  /**
   * Add the decendents (sub transformation schemes , and their sub transformation schemes recursively)
   *  of the FunctionalModuleSet which are LeafFunctionalModule 
   * to a list of LeafFunctionalModule
   * @param FunctionalModuleSet
   * @param list
   */
  private static void addDecendentFunctionalModulesOfFunctionalModuleToList(
      FunctionalModuleSet FunctionalModuleSet, List<LeafFunctionalModule> list) {

    EList<FunctionalModule> children = FunctionalModuleSet.getSubTransformationsAndSchemes();
    addChildrenTransformationsOfSchemesToList(children, list);

  }

  /**
   * Add parents and younger siblings of a FunctionalModule to a list.
   * 
   * @param FunctionalModule
   * @param list
   */
  private static void addParentsAndYoungerSiblingsOfToList(FunctionalModule FunctionalModule, List<FunctionalModule> list) {
   

    if (FunctionalModule.eContainer() instanceof FunctionalModuleSet) {
      addYoungerSiblingsToList(list, (FunctionalModuleSet) FunctionalModule.eContainer(), FunctionalModule);
      addParentsAndYoungerSiblingsOfToList((FunctionalModule) FunctionalModule.eContainer(), list);
    }
  }

  /**
   * Add younger siblings of a FunctionalModule to a list.
   * 
   * @param list
   * @param parent
   * @param FunctionalModule
   */
  private static void addYoungerSiblingsToList(List<FunctionalModule> list, FunctionalModuleSet parent,
      FunctionalModule FunctionalModule) {

    boolean finishedFlag = false;
    EList<FunctionalModule> children = parent.getSubTransformationsAndSchemes();

    // we are making an assumption here (possibly unsafe) that the iterator gets the
    // contents in the same order as the ordered list.
    for (Iterator iterator = children.iterator(); iterator.hasNext();) {
      FunctionalModule transformationSuperClass = (FunctionalModule) iterator.next();

      if (transformationSuperClass.equals(FunctionalModule))
        finishedFlag = true;
      if (finishedFlag == false)
        list.add((FunctionalModule) transformationSuperClass);
    }

  }
  
  
  /**
   * Ths method sets the transformationSubset attrribute of the call to equal
   * the a FunctionalModuleSet containing the dependent  LeafFunctionalModules of the 
   * calls ReportCellCreationFunctionalModule .
   * 
   * Currently the method has an assumption that there is only one 
   * ReportCellCreationFunctionalModule, which is done for simplicity.
   * 
   * @param call
   */
 /** static void setFunctionalModuleInCall(GetTestDefinition call) {
    EList<ReportCellCreationFunctionalModule> rccTransformations = call.getReportCellCreationTransformations();
    List<LeafFunctionalModule> transformationSubset = null;
    FunctionalModuleSet initialScheme = null;
    for (Iterator iterator = rccTransformations.iterator(); iterator.hasNext();) {
      ReportCellCreationFunctionalModule reportCellCreationTransformation = (ReportCellCreationFunctionalModule) iterator
          .next();
      // for now assume we have only one rcct
      transformationSubset = FunctionalModuleUtil.getFunctionalModuleDependencies(reportCellCreationTransformation);
      initialScheme = getUltimateParentScheme((FunctionalModuleSet) reportCellCreationTransformation.eContainer());

    }
    // copy transformation scheme and delete any transformation Not in the list,
    // then delete all empty schemes.

    FunctionalModuleSet initialSchemeClone = EcoreUtil.copy(initialScheme);
    removeFunctionalModulesFromATranformationSchemeSet(initialSchemeClone, transformationSubset);
    removeEmptySchemesFromScheme(initialSchemeClone);
    call.setFunctionalModule(initialSchemeClone);

  }*/

  /**
   * Returns the ultimate parent FunctionalModuleSet of a FunctionalModuleSet
   * @param FunctionalModuleSet
   * @return
   */
  private static FunctionalModuleSet getUltimateParentScheme(FunctionalModuleSet FunctionalModuleSet) {
   
    if (FunctionalModuleSet.eContainer() != null && FunctionalModuleSet.eContainer() instanceof FunctionalModuleSet) {
      return getUltimateParentScheme((FunctionalModuleSet) FunctionalModuleSet.eContainer());
    } else
      return FunctionalModuleSet;
  }

  /**
   * Removes any empty FunctionalModuleSets from a FunctionalModuleSet
   * @param FunctionalModuleSet
   */
  private static void removeEmptySchemesFromScheme(FunctionalModuleSet FunctionalModuleSet) {
    EList<FunctionalModule> coll1 = FunctionalModuleSet.getSubTransformationsAndSchemes();
    BasicEList<FunctionalModuleSet> tsToDelete = new BasicEList<FunctionalModuleSet>();
    for (Iterator iterator = coll1.iterator(); iterator.hasNext();) {
      FunctionalModule transformationSuperClass = (FunctionalModule) iterator.next();
      if (transformationSuperClass instanceof FunctionalModuleSet) {
        if (((FunctionalModuleSet) transformationSuperClass).getSubTransformationsAndSchemes().isEmpty()) {
          tsToDelete.add((FunctionalModuleSet) transformationSuperClass);

        } else
          removeEmptySchemesFromScheme((FunctionalModuleSet) transformationSuperClass);

      }
    }
    for (Iterator iterator = tsToDelete.iterator(); iterator.hasNext();) {
      FunctionalModuleSet FunctionalModule = (FunctionalModuleSet) iterator.next();
      FunctionalModuleSet.getSubTransformationsAndSchemes().remove(FunctionalModule);

    }

  }

  /**
   * Removes FunctionalModules from a tranformationSchemeSet.
   * 
   * @param FunctionalModuleSet
   * @param transformationSubset
   */
  private static void removeFunctionalModulesFromAFunctionalModuleSet(FunctionalModuleSet FunctionalModuleSet,
      List<LeafFunctionalModule> transformationSubset) {
    EList<FunctionalModule> tranformationSchemes = FunctionalModuleSet.getSubTransformationsAndSchemes();
    BasicEList<LeafFunctionalModule> FunctionalModulesToDelete = new BasicEList<LeafFunctionalModule>();
    for (Iterator iterator = tranformationSchemes.iterator(); iterator.hasNext();) {

      FunctionalModule transformationSuperClass = (FunctionalModule) iterator.next();
      if (transformationSuperClass instanceof LeafFunctionalModule) {
        if (!containsItemWithName(transformationSubset, transformationSuperClass.getName())) {
          FunctionalModulesToDelete.add((LeafFunctionalModule) transformationSuperClass);

        }

      } else if (transformationSuperClass instanceof FunctionalModuleSet) {
        removeFunctionalModulesFromAFunctionalModuleSet((FunctionalModuleSet) transformationSuperClass, transformationSubset);
      }

    }

    for (Iterator iterator = FunctionalModulesToDelete.iterator(); iterator.hasNext();) {
      LeafFunctionalModule transformation = (LeafFunctionalModule) iterator.next();
      FunctionalModuleSet.getSubTransformationsAndSchemes().remove(transformation);
    }

  }

  /**
   * Returns true if the transformationSubset contains a LeafFunctionalModule with
   * a particular name.
   * @param transformationSubset
   * @param name
   * @return
   */
  private static boolean containsItemWithName(List<LeafFunctionalModule> transformationSubset, String name) {
    
    for (Iterator iterator = transformationSubset.iterator(); iterator.hasNext();) {
      LeafFunctionalModule FunctionalModule = (LeafFunctionalModule) iterator.next();
      if (FunctionalModule.getName().equalsIgnoreCase(name))
        return true;

    }
    return false;
  }

 
   /**
    * If the FunctionalModule is a member of a SerialFunctionalModule this method
    * returns the prior FunctionalModules in the serial list.
   * @param FunctionalModule
   * @return
   */
  public static EList<FunctionalModule> getPreviousFunctionalModules(FunctionalModule FunctionalModule) {

    EList<FunctionalModule> list = new BasicEList<FunctionalModule>();
    EObject container = FunctionalModule.eContainer();

    boolean addIt = false;
    if (container != null
        && ((container instanceof SerialFunctionalModule) || 
            container instanceof ScenarioSetFunctionalModule)) 
      //why do we include ScenarioSetFunctionalModule in the if statement, 
      // there may be a good reason but should  check.
    {
      EList<EObject> list2 = container.eContents();
      for (Iterator iterator = list2.iterator(); iterator.hasNext();) {
        EObject eObject = (EObject) iterator.next();

        if (addIt) {
          list.add((FunctionalModule) eObject);
          addIt = false;
        }
        if (eObject == FunctionalModule)
          addIt = true;

      }
    }
    return list;
  }

  /**
   *  If the FunctionalModule is a member of a SerialFunctionalModule this method
    * returns the prior FunctionalModules in the serial list.
   * @param FunctionalModule
   * @return
   */
  public static EList<FunctionalModule> getPreviousFunctionalModule2(FunctionalModule FunctionalModule) {

    EList<FunctionalModule> list = new BasicEList<FunctionalModule>();
    EObject cont = FunctionalModule.eContainer();
    FunctionalModule schemeToAdd = null;
    // the below if statement is different form that in the method getPreviousFunctionalModules
    // need to clarify the exact different and reason for 2 similar methods.
    if (cont != null && cont instanceof FunctionalModuleSet) {
      EList<EObject> list2 = cont.eContents();
      for (Iterator iterator = list2.iterator(); iterator.hasNext();) {
        EObject eObject = (EObject) iterator.next();

        if (eObject == FunctionalModule)
          list.add(schemeToAdd);
        else
          schemeToAdd = (FunctionalModule) eObject;
      }
    }
    return list;
  }
  
  /**
   * Returns true if a FunctionalModule is included in the provided  list of 
   * LeafFunctionalModules
   * @param FunctionalModule
   * @param FunctionalModuleSubset
   * @return
   */
  static boolean isInFunctionalModuleList(LeafFunctionalModule FunctionalModule,
      List<LeafFunctionalModule> FunctionalModuleSubset) {
    boolean returnValue = false;
    for (Iterator iterator = FunctionalModuleSubset.iterator(); iterator.hasNext();) {
      LeafFunctionalModule transformation = (LeafFunctionalModule) iterator.next();
      if (transformation.getName().equals(FunctionalModule.getName()))
        returnValue = true;
    }
    return returnValue;
  }

  /**
   * Get the requirements associated with a scenario, utilising the 
   * DefaultTagGroup.
   * 
   * @param scenario
   * @return
   */
  public static EList<CodeCondition> getCodeConditionsFromDefualtNavigationContext(Scenario scenario)
  {

			EList<CodeCondition> returnlist = new BasicEList<CodeCondition>();
			
			NavigationContext nc = TagUtil.getDefaultNavigationContext(scenario);
			
			DomainMappingModule domainMapping = nc.getDomainModelMapping();
			EList<ScenarioToCodeConditions> scenarioToCodeConditions = domainMapping.getScenarioToCodeConditions();

			for (ScenarioToCodeConditions scenarioToCodeConditions2 : scenarioToCodeConditions) {
				if(scenarioToCodeConditions2.getScenario().equals(scenario))
					returnlist.addAll(scenarioToCodeConditions2.getCodeConditions());
					
			} 
			
			return returnlist;
		}
  
  public static EList<BooleanFunction> getCodeConditionFilterFunctions(Scenario scenario) {
	    
	  EList<BooleanFunction> returnlist = new BasicEList<BooleanFunction>();
		
		NavigationContext nc = TagUtil.getDefaultNavigationContext(scenario);
		
		DomainMappingModule domainMapping = nc.getDomainModelMapping();
		EList<ScenarioToCodeConditions> scenarioToCodeConditions = domainMapping.getScenarioToCodeConditions();

		for (ScenarioToCodeConditions scenarioToCodeConditions2 : scenarioToCodeConditions) {
			EList<CodeCondition> list1;
			if(scenarioToCodeConditions2.getScenario().equals(scenario) )
			{
				EList<CodeCondition> codeConditions = scenarioToCodeConditions2.getCodeConditions();
				
				for (CodeCondition codeCondition : codeConditions) {
					if (codeCondition instanceof DataSetFilterCondition)
					{
						returnlist.add(((DataSetFilterCondition)codeCondition).getFilterConditions());
					}
					if (codeCondition instanceof ColumnSettingCondition)
					{
						returnlist.add(((ColumnSettingCondition)codeCondition).getFilterConditions());
					}
				}
			}
				
		} 
		
		return returnlist;

  }

	public static EList<DataSetFilterCondition> getDataSetFilterCodeConditionsFromDefualtNavigationContext(Scenario scenario) {
	    
		
		EList<DataSetFilterCondition> returnlist = new BasicEList<DataSetFilterCondition>();
		
		NavigationContext nc = TagUtil.getDefaultNavigationContext(scenario);
		
		DomainMappingModule domainMapping = nc.getDomainModelMapping();
		EList<ScenarioToCodeConditions> scenarioToCodeConditions = domainMapping.getScenarioToCodeConditions();

		for (ScenarioToCodeConditions scenarioToCodeConditions2 : scenarioToCodeConditions) {
			if(scenarioToCodeConditions2.getScenario().equals(scenario))
			{
				

				
				if(scenarioToCodeConditions2.getScenario().equals(scenario) )
				{
						EList<CodeCondition> codeConditions = scenarioToCodeConditions2.getCodeConditions();
					
					for (CodeCondition codeCondition : codeConditions) {
						if (codeCondition instanceof DataSetFilterCondition)
						{
							returnlist.add(((DataSetFilterCondition)codeCondition));
						}
					}
				}	
			}
					
		} 
		
		return returnlist; 
	 }

	public static EList<ColumnSettingCondition> getColumnSettingCodeConditionsFromDefualtNavigationContext(Scenario scenario) {
	    
		
       EList<ColumnSettingCondition> returnlist = new BasicEList<ColumnSettingCondition>();
		
		NavigationContext nc = TagUtil.getDefaultNavigationContext(scenario);
		
		DomainMappingModule domainMapping = nc.getDomainModelMapping();
		EList<ScenarioToCodeConditions> scenarioToCodeConditions = domainMapping.getScenarioToCodeConditions();

		for (ScenarioToCodeConditions scenarioToCodeConditions2 : scenarioToCodeConditions) {
			if(scenarioToCodeConditions2.getScenario().equals(scenario))
			{
				

				if(scenarioToCodeConditions2.getScenario().equals(scenario) )
				{
					EList<CodeCondition> codeConditions = scenarioToCodeConditions2.getCodeConditions();
					
					for (CodeCondition codeCondition : codeConditions) {
						if (codeCondition instanceof ColumnSettingCondition)
						{
							returnlist.add(((ColumnSettingCondition)codeCondition));
						}
					}
					
				}	
			}
					
		} 
		
		return returnlist; 
	 }

  
  

}
