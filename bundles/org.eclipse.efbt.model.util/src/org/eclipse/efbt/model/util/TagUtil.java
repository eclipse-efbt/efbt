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
import tags.DataSetTransformationTag;
import tags.FunctionTag;
import tags.Tag;
import tags.TagGroup;
import tags.TransformationSchemeTag;
import transformation.DataSetTransformation;
import functional_module.LeafTransformationScheme;
import functional_module.TransformationScheme;

/**
 * @author Neil Mackenzie
 *
 */
public class TagUtil {

  /**
   * get all the TransformationSchemeTags from the DefualtTagGroup.
   * 
   * @param transformationScheme
   * @return
   */
  public static EList<TransformationSchemeTag> getTransformationSchemeTagsFromDefualtTagGroup(
      TransformationScheme transformationScheme) {
    EList<TransformationSchemeTag> returnlist = new BasicEList<TransformationSchemeTag>();
    NavigationContext nc = getDefaultNavigationContext(transformationScheme);
    TagGroup tg = nc.getTags();
    EList<Tag> tags = tg.getTags();
    for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
      Tag tag = (Tag) iterator.next();

      if (tag instanceof TransformationSchemeTag) {
        if (((TransformationSchemeTag) tag).getTransformation().getName().equals(transformationScheme.getName())) {
          returnlist.add((TransformationSchemeTag) tag);
        }

      }

    }

    return returnlist;
  }

  /**
   * Get the requirements associated with a Transformationscheme, utilising the 
   * DefaultTagGroup.
   * 
   * @param transformationScheme
   * @return
   */
  public static EList<TitledRequirementsSection> getTransformationSchemeRequirementsFromDefualtTagGroup(
      LeafTransformationScheme transformationScheme) {
    EList<TitledRequirementsSection> returnlist = new BasicEList<TitledRequirementsSection>();
    EList<TransformationSchemeTag> tagList = getTransformationSchemeTagsFromDefualtTagGroup(transformationScheme);
    for (Iterator iterator = tagList.iterator(); iterator.hasNext();) {
      TransformationSchemeTag TransformationTag = (TransformationSchemeTag) iterator.next();
      EList<TitledRequirementsSection> requirements = TransformationTag.getRequirements();
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
        + "/navigation_context/defaultNavigationContext.navigation_context";
    File file = new File(tagsXMLFile);
    URI uri = file.isFile() ? URI.createFileURI(file.getAbsolutePath()) : URI.createURI(tagsXMLFile);
    Resource resource = rs.getResource(uri, true);
    return (NavigationContext) resource.getContents().get(0);

  }

}
