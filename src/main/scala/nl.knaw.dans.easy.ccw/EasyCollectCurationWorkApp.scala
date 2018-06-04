/**
 * Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.ccw

import better.files.File
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.Try

class EasyCollectCurationWorkApp(commonCurationArea: File, managerCurationArea: DatamanagerId => File, datamanagerProperties: PropertiesConfiguration) extends DebugEnhancedLogging {

  private def isCurated(depositProperties: PropertiesConfiguration): Boolean = {
    depositProperties.getBoolean("curation.performed")
  }

  private def requiredPropertiesPresent(datamanager: DatamanagerId, deposit: File, depositProperties: PropertiesConfiguration, keys: List[String]): Boolean = {
    val invalidKeys = keys.filter(key => depositProperties.getString(key, "").isEmpty)
    invalidKeys.foreach(key => logger.error(s"Deposit ${ deposit.name }, curated by $datamanager, has no value for property '$key'"))
    invalidKeys.isEmpty
  }

  private def correctState(datamanager: DatamanagerId, deposit: File, state: String): Boolean = {
    if (state != "SUBMITTED" && state != "REJECTED") {
      logger.error(s"Deposit ${ deposit.name }, curated by $datamanager, is curated, but is in state $state")
      false
    }
    else
      true
  }

  private def correctProperties(datamanager: DatamanagerId, deposit: File, depositProperties: PropertiesConfiguration, state: String): Boolean = {
    val propertiesToCheck = List("state.description", "curation.datamanager.userId", "curation.datamanager.email")
    val isCorrectState = correctState(datamanager, deposit, state)
    requiredPropertiesPresent(datamanager, deposit, depositProperties, propertiesToCheck) && isCorrectState
  }

  private def collectDeposit(datamanager: DatamanagerId, deposit: File): Unit = {
    val depositProperties = new PropertiesConfiguration(deposit / "deposit.properties" toJava)
    val state = depositProperties.getString("state.label", "")

    if (isCurated(depositProperties) && correctProperties(datamanager, deposit, depositProperties, state)) {
      deposit moveTo commonCurationArea / deposit.name
      logger.info(s"Deposit ${ deposit.name } ($state), curated by $datamanager, has been moved to common curation area")
    }
  }

  private def collectDatamanagersCuratedDeposits(datamanager: DatamanagerId, dir: File): Unit = {
    dir.list.foreach(deposit => collectDeposit(datamanager, deposit))
  }

  def getDatamanagers: List[DatamanagerId] = {
    datamanagerProperties.getKeys.asScala.toList.map(key => key.substring(0, key.indexOf('.'))).distinct
  }

  def run(): Try[Unit] = Try {
    logger.info(s"-- Collection of curated deposits started --")
    getDatamanagers.foreach(datamanager => {
      val curationDirectory = managerCurationArea(datamanager)
      if (curationDirectory.exists)
        collectDatamanagersCuratedDeposits(datamanager, curationDirectory)
    })
    logger.info(s"-- Collection of curated deposits completed --")

  }
}
