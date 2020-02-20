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
 *  Neil Mackenzie - initial API and implementation */
package customfuncs

object CustomFunctions {
  
  def trippleA_NotionaAmount(s1:String, d : Double) : Double=
  {
    if (s1.equals("AAA")) d
    else 0
  }
  
  
}