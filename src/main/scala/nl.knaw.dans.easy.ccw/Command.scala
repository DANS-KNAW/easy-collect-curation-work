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
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.language.reflectiveCalls

object Command extends App with DebugEnhancedLogging {

  val configuration = Configuration(File(System.getProperty("app.home")))
  val commonCurationArea = File(configuration.properties.getString("curation.common.directory"))
  def managerCurationArea(datamanager: DatamanagerId) = File(configuration.properties.getString("curation.personal.directory").replace("$unix-user", datamanager))
  val datamanagerProperties = configuration.datamanagers

  val app = new EasyCollectCurationWorkApp(commonCurationArea, managerCurationArea, datamanagerProperties)
  app.run()
    .doIfSuccess { _ => Console.err.println(s"Collection of curated deposits completed") }
    .doIfFailure { case e => logger.error(e.getMessage, e) }
    .doIfFailure { case e => Console.err.println(s"FAILED: ${ e.getMessage }") }
}
