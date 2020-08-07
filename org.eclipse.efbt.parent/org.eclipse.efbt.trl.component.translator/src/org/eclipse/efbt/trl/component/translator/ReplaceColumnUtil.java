/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.trl.component.translator;

import java.util.Iterator;

import org.eclipse.efbt.almengine.model.cubes.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.bird.core.VARIABLE;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.IgnoreColumn;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.JoinView;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.RenameAs;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.SQLView;



/**
 * @author Neil Mackenzie
 *
 */
public class ReplaceColumnUtil {

  /**
   * Gets the replaced a column in a JoinView
   * 
   * @param joinView
   * @param view
   * @param item
   * @return
   */
  public static VARIABLE replaceColumn(JoinView joinView, SQLView view, VARIABLE item) {
    FreeBirdToolsCube cube = view.getCube();
    return replaceColumn(joinView, cube, item);
  }

  /**
   * @param joinView
   * @param cube
   * @param item
   * @return
   */
  public static VARIABLE replaceColumn(JoinView joinView, FreeBirdToolsCube cube, VARIABLE item) {
    VARIABLE returnDimension = item;
    if (((JoinView) joinView).getIgnores() != null) {
      Iterator<IgnoreColumn> iter = ((JoinView) joinView).getIgnores().iterator();
      while (iter.hasNext()) {

        IgnoreColumn ignore = iter.next();
        FreeBirdToolsCube inputCube = ignore.getCube();
        VARIABLE inputcol = ignore.getColumnToIgnore();
        if (cube.equals(inputCube) && inputcol.equals(item))
          returnDimension = null;

      }
    }

    if (((JoinView) joinView).getRenames() != null && returnDimension != null) {
      Iterator<RenameAs> iter = ((JoinView) joinView).getRenames().iterator();
      while (iter.hasNext()) {

        RenameAs rename = iter.next();
        FreeBirdToolsCube inputCube = rename.getFromCube();
        VARIABLE inputcol = rename.getFromVariable();
        if (cube.equals(inputCube) && inputcol.equals(item))
          returnDimension = (VARIABLE) rename.getToVariable();

      }
    }
    return returnDimension;
  }

}
