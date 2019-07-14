/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions.Attribute

object SlothUtils {

  def attrExist(attr: Attribute, attrSet: Seq[Attribute]): Boolean = {
    attrSet.exists(thisAttr => thisAttr.semanticEquals(attr))
  }

  def attrDiff(attrSetA: Seq[Attribute], attrSetB: Seq[Attribute]):
  Seq[Attribute] = {
    val retSet = new ListBuffer[Attribute]
    attrSetA.foreach(attrA => {
      if (!attrExist(attrA, attrSetB)) retSet += attrA
    })

    retSet
  }

  def attrIntersect(attrSetA: Seq[Attribute], attrSetB: Seq[Attribute]):
  Seq[Attribute] = {
    val retSet = new ListBuffer[Attribute]
    attrSetA.foreach(attrA => {
      if (attrExist(attrA, attrSetB)) retSet += attrA
    })

    retSet
  }

  def attrUnion(attrSetA: Seq[Attribute], attrSetB: Seq[Attribute]):
  Seq[Attribute] = {
    val retSet = new ListBuffer[Attribute]
    attrSetA.foreach(attrA => {
      if (!attrExist(attrA, retSet)) retSet += attrA
    })

    attrSetB.foreach(attrB => {
      if (!attrExist(attrB, retSet)) retSet += attrB
    })

    retSet
  }
}
