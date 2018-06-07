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
import org.apache.commons.configuration.PropertiesConfiguration

import scala.language.postfixOps
import scala.util.Success

class CollectSpec extends TestSupportFixture {

  val resourceDir = File(getClass.getResource("/"))
  val configuration = new Configuration("version x.y.z",
    new PropertiesConfiguration() {},
    new PropertiesConfiguration() {
      setDelimiterParsingDisabled(true)
      load(resourceDir / "debug-config" / "datamanager.properties" toJava)
    })
  val datamanagerProperties = configuration.datamanagers
  val commonCurationArea = testDir / "easy-common-curation-area"
  val datamanagerCurationAreas = testDir / "datamanager-curation-areas"
  val managerCurationDir = datamanagerCurationAreas / "$unix-user/curation-area" toString
  def managerCurationArea(datamanager: DatamanagerId) = File(managerCurationDir.replace("$unix-user", datamanager))
  val jannekesCurationArea = datamanagerCurationAreas / "janneke/curation-area"
  val testLog = testDir / ".." / "test.log"

  val collector = new EasyCollectCurationWorkApp(commonCurationArea, managerCurationArea, datamanagerProperties)

  override def beforeEach(): Unit = {
    testLog.clear()
    commonCurationArea.delete(swallowIOExceptions = true)
    jannekesCurationArea.delete(swallowIOExceptions = true)
    File(getClass.getResource("/easy-common-curation-area")) copyTo commonCurationArea
    File(getClass.getResource("/datamanager-curation-areas/janneke/curation-area")) copyTo jannekesCurationArea
    commonCurationArea.toJava should exist
    jannekesCurationArea.toJava should exist
  }

  "collect" should "succeed" in {
    collector.run() shouldBe a[Success[_]]
  }

  it should "find two datamanager user ids" in {
    collector.getDatamanagers should have size 2
  }

  it should "list correct error messages to the log file" in {
    collector.run() shouldBe a[Success[_]]
    testLog.contentAsString should include("ERROR Deposit 48bc40f9-12d7-42c6-808a-8eac77bfc726, curated by janneke, is curated, but is in state DRAFT")
    testLog.contentAsString should include("ERROR Deposit 48bc40f9-12d7-42c6-808a-8eac77bfc726, curated by janneke, has no value for property 'curation.datamanager.userId'")
    testLog.contentAsString should include("ERROR Deposit 48bc40f9-12d7-42c6-808a-8eac77bfc726, curated by janneke, has no value for property 'curation.datamanager.userId'")
    testLog.contentAsString should include("ERROR Deposit 48bc40f9-12d7-42c6-808a-8eac77bfc726, curated by janneke, has no value for property 'state.description'")
  }

  it should "list messages about collected deposits to the log file" in {
    collector.run() shouldBe a[Success[_]]
    testLog.contentAsString should include("INFO  Deposit 38bc40f9-12d7-42c6-808a-8eac77bfc726 (SUBMITTED), curated by janneke, has been moved to common curation area")
  }

  it should "have no deposits curated by datamanager jip in the log file" in {
    collector.run() shouldBe a[Success[_]]
    testLog.contentAsString should not include (s"jip")
  }

  it should "have moved one deposit from a datamager curation area to the common curation areas" in {
    jannekesCurationArea.list should have size 3
    commonCurationArea.list should have size 1
    collector.run() shouldBe a[Success[_]]
    jannekesCurationArea.list should have size 2
    commonCurationArea.list should have size 2
  }
}
