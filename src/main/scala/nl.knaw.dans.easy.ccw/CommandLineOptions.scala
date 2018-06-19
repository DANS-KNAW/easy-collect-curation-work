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

import org.rogach.scallop.ScallopConf

class CommandLineOptions(args: Array[String], version: String) extends ScallopConf(args) {
  appendDefaultToDescription = true
  editBuilder(_.setHelpWidth(110))
  printedName = "easy-collect-curation-work"
  private val SUBCOMMAND_SEPARATOR = "---\n"
  val description = "Job that collects the curated deposits from the data managers' personal curation areas"
  val synopsis: String =
  s"  $printedName "

  version(s"$printedName v$version")
  banner(
    s"""
       |$description
       |
       |Usage:
       |
       |$synopsis
       |
       |Options:
       |""".stripMargin)

  verify()
}
