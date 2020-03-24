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

import java.io.File;
import java.util.Iterator;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;

import functions.Function;
import navigation_context.NavigationContext;
import requirements_text.TitledRequirementsSection;
import scenarios.Scenario;
import tags.DataSetTransformationTag;
import tags.FunctionTag;
import tags.ScenarioTag;
import tags.Tag;
import tags.TagGroup;
import tags.FunctionalModuleTag;
import transformation.DataSetTransformation;
import functional_module.LeafFunctionalModule;
import functional_module.FunctionalModule;

/**
 * @author Neil Mackenzie
 *
 */
public class TagUtil {

  /**
   * get all the FunctionalModuleTags from the DefualtTagGroup.
   * 
   * @param functionalModule
   * @return
   */
  public static EList<FunctionalModuleTag> getFunctionalModuleTagsFromDefualtTagGroup(
      FunctionalModule functionalModule) {
    EList<FunctionalModuleTag> returnlist = new BasicEList<FunctionalModuleTag>();
    NavigationContext nc = getDefaultNavigationContext(functionalModule);
    TagGroup tg = nc.getTags();
    EList<Tag> tags = tg.getTags();
    for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
      Tag tag = (Tag) iterator.next();

      if (tag instanceof FunctionalModuleTag) {
        if (((FunctionalModuleTag) tag).getTransformation().getName().equals(functionalModule.getName())) {
          returnlist.add((FunctionalModuleTag) tag);
        }

      }

    }

    return returnlist;
  }
  
  public static EList<ScenarioTag> getScenarioTagsFromDefualtTagGroup(
	      Scenario scenario) {
	    EList<ScenarioTag> returnlist = new BasicEList<ScenarioTag>();
	    NavigationContext nc = getDefaultNavigationContext(scenario);
	    TagGroup tg = nc.getTags();
	    EList<Tag> tags = tg.getTags();
	    for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
	      Tag tag = (Tag) iterator.next();

	      if (tag instanceof ScenarioTag) {
	        if (((ScenarioTag) tag).getScenario().getName().equals(scenario.getName())) {
	          returnlist.add((ScenarioTag) tag);
	        }

	      }

	    }

	    return returnlist;
	  }

  /**
   * Get the requirements associated with a Transformationscheme, utilising the 
   * DefaultTagGroup.
   * 
   * @param functionalModule
   * @return
   */
  public static EList<TitledRequirementsSection> getFunctionalModuleRequirementsFromDefualtTagGroup(
      LeafFunctionalModule functionalModule) {
    EList<TitledRequirementsSection> returnlist = new BasicEList<TitledRequirementsSection>();
    EList<FunctionalModuleTag> tagList = getFunctionalModuleTagsFromDefualtTagGroup(functionalModule);
    for (Iterator iterator = tagList.iterator(); iterator.hasNext();) {
      FunctionalModuleTag TransformationTag = (FunctionalModuleTag) iterator.next();
      EList<TitledRequirementsSection> requirements = TransformationTag.getRequirements();
      for (Iterator iterator2 = requirements.iterator(); iterator2.hasNext();) {
        TitledRequirementsSection titledRequirementsSection = (TitledRequirementsSection) iterator2.next();
        returnlist.add(titledRequirementsSection);
      }

    }
    return returnlist;
  }
  
  /**
   * Get the requirements associated with a scenario, utilising the 
   * DefaultTagGroup.
   * 
   * @param functionalModule
   * @return
   */
  public static EList<TitledRequirementsSection> getScenarioRequirementsFromDefualtTagGroup(
		  Scenario scenario) {
    EList<TitledRequirementsSection> returnlist = new BasicEList<TitledRequirementsSection>();
    EList<ScenarioTag> tagList = getScenarioTagsFromDefualtTagGroup(scenario);
    for (Iterator iterator = tagList.iterator(); iterator.hasNext();) {
    	ScenarioTag scenarioTag = (ScenarioTag) iterator.next();
      EList<TitledRequirementsSection> requirements = scenarioTag.getRequirements();
      for (Iterator iterator2 = requirements.iterator(); iterator2.hasNext();) {
        TitledRequirementsSection titledRequirementsSection = (TitledRequirementsSection) iterator2.next();
        returnlist.add(titledRequirementsSection);
      }

    }
    return returnlist;
  }

  /**
   * Get the requirements associated with a DataSetTransformation, utilising the 
   * DefaultTagGroup.
   * 
   * @param dataSetTransformation
   * @return
   */
  public static EList<TitledRequirementsSection> getDataSetTransactionRequirementsFromDefualtTagGroup(
      DataSetTransformation dataSetTransformation) {

    EList<TitledRequirementsSection> returnlist = new BasicEList<TitledRequirementsSection>();
    EList<DataSetTransformationTag> tagList = getDataSetTransformationTagsFromDefaultTagGroup(dataSetTransformation);
    for (Iterator iterator = tagList.iterator(); iterator.hasNext();) {
      DataSetTransformationTag DSTransformationTag = (DataSetTransformationTag) iterator.next();
      EList<TitledRequirementsSection> requirements = DSTransformationTag.getRequirements();
      for (Iterator iterator2 = requirements.iterator(); iterator2.hasNext();) {
        TitledRequirementsSection titledRequirementsSection = (TitledRequirementsSection) iterator2.next();
        returnlist.add(titledRequirementsSection);
      }

    }
    return returnlist;

  }

  /**
   *  Get the requirements associated with a Function , utilising the 
   * DefaultTagGroup.
   * 
   * @param function
   * @return
   */
  public static EList<TitledRequirementsSection> getFunctionRequirementsFromDefualtTagGroup(Function function) {

    EList<TitledRequirementsSection> returnlist = new BasicEList<TitledRequirementsSection>();
    EList<FunctionTag> tagList = getFunctionTagsFromDefaultTagGroup(function);
    for (Iterator iterator = tagList.iterator(); iterator.hasNext();) {
      FunctionTag functionTag = (FunctionTag) iterator.next();
      EList<TitledRequirementsSection> requirements = functionTag.getRequirements();
      for (Iterator iterator2 = requirements.iterator(); iterator2.hasNext();) {
        TitledRequirementsSection titledRequirementsSection = (TitledRequirementsSection) iterator2.next();
        returnlist.add(titledRequirementsSection);
      }

    }
    return returnlist;

  }

  /**
   * Get the DataSetTransformationTags from the DefaultTagGroup.
   * 
   * @param dataSetTransformation
   * @return
   */
  public static EList<DataSetTransformationTag> getDataSetTransformationTagsFromDefaultTagGroup(
      DataSetTransformation dataSetTransformation) {
    EList<DataSetTransformationTag> returnlist = new BasicEList<DataSetTransformationTag>();
    NavigationContext nc = getDefaultNavigationContext(dataSetTransformation);
    TagGroup tg = nc.getTags();
    EList<Tag> tags = tg.getTags();
    for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
      Tag tag = (Tag) iterator.next();

      if (tag instanceof DataSetTransformationTag) {
        if (((DataSetTransformationTag) tag).getDatasetTransformation().getName()
            .equals(dataSetTransformation.getName()))
          returnlist.add((DataSetTransformationTag) tag);

      }

    }

    return returnlist;

  }

  /**
   * Get the FunctionTags from the DefaultTagGroup.
   * @param function
   * @return
   */
  public static EList<FunctionTag> getFunctionTagsFromDefaultTagGroup(Function function) {
   
    EList<FunctionTag> returnlist = new BasicEList<FunctionTag>();
    NavigationContext nc = getDefaultNavigationContext(function);
    TagGroup tg = nc.getTags();
    EList<Tag> tags = tg.getTags();
    for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
      Tag tag = (Tag) iterator.next();

      if (tag instanceof FunctionTag) {
        if (((FunctionTag) tag).getFunction().getName().equals(function.getName()))
          returnlist.add((FunctionTag) tag);

      }

    }

    return returnlist;

  }

  /**
   * Returns the DefaultNavigationContext
   * @param o
   * @return
   */
  public static NavigationContext getDefaultNavigationContext(EObject o) {
    ResourceSet rs = o.eResource().getResourceSet();
    String tagsXMLFile = o.eResource().getURI().trimSegments(2)
        + "/extra/defaultNavigationContext.navigation_context";
    File file = new File(tagsXMLFile);
    URI uri = file.isFile() ? URI.createFileURI(file.getAbsolutePath()) : URI.createURI(tagsXMLFile);
    Resource resource = rs.getResource(uri, true);
    return (NavigationContext) resource.getContents().get(0);

  }

}
