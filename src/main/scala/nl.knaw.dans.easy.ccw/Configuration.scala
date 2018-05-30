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
import better.files.File.root
import org.apache.commons.configuration.PropertiesConfiguration

import scala.language.postfixOps

case class Configuration(version: String, properties: PropertiesConfiguration, datamanagers: PropertiesConfiguration)

object Configuration {

  def apply(home: File): Configuration = {
    val cfgPath = Seq(
      root / "etc" / "opt" / "dans.knaw.nl" / "easy-manage-curation-work",
      home / "cfg"
    )
      .find(_.exists)
      .getOrElse { throw new IllegalStateException("No configuration directory found") }

    new Configuration(
      version = (home / "bin" / "version").contentAsString,
      properties = new PropertiesConfiguration() {
        setDelimiterParsingDisabled(true)
        load(cfgPath / "application.properties" toJava)
      },
      datamanagers = new PropertiesConfiguration() {
        setDelimiterParsingDisabled(true)
        load(cfgPath / "datamanager.properties" toJava)
      }
    )
  }
}
