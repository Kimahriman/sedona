/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.sql

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SQLContext


trait TestBaseScala extends FunSpec with BeforeAndAfterAll {

  val warehouseLocation = System.getProperty("user.dir") + "/target/"

  val defaultConf = new SparkConf()
    .set(SQLConf.CODEGEN_FALLBACK.key, "false")
    .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
    // .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
    .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConstantFolding.ruleName)
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    .set("spark.sql.warehouse.dir", warehouseLocation)
    // We need to be explicit about broadcasting in tests.
    .set("sedona.join.autoBroadcastJoinThreshold", "-1")

  private val _baseSession = SparkSession.builder()
    .config(defaultConf)
    .master("local[*]")
    .appName("sedonasqlScalaTest")
    .getOrCreate()

  private var _spark: SparkSession = _

  protected implicit def sparkSession = _spark

  // Helper implicits that use the active SparkSession
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = sparkSession.sqlContext
  }

  val resourceFolder = System.getProperty("user.dir") + "/../core/src/test/resources/"
  val mixedWkbGeometryInputLocation = resourceFolder + "county_small_wkb.tsv"
  val mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"
  val shapefileWithMissingsTrailingInputLocation = resourceFolder + "shapefiles/missing"
  val geojsonInputLocation = resourceFolder + "testPolygon.json"
  val arealmPointInputLocation = resourceFolder + "arealm.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val csvPolygon1InputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope1.csv"
  val csvPolygon2InputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope2.csv"
  val csvPolygon1RandomInputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope1_random.csv"
  val csvPolygon2RandomInputLocation = resourceFolder + "equalitycheckfiles/testequals_envelope2_random.csv"
  val overlapPolygonInputLocation = resourceFolder + "testenvelope_overlap.csv"
  val unionPolygonInputLocation = resourceFolder + "testunion.csv"
  val intersectionPolygonInputLocation = resourceFolder + "test_intersection_aggregate.tsv"
  val intersectionPolygonNoIntersectionInputLocation = resourceFolder + "test_intersection_aggregate_no_intersection.tsv"
  val csvPoint1InputLocation = resourceFolder + "equalitycheckfiles/testequals_point1.csv"
  val csvPoint2InputLocation = resourceFolder + "equalitycheckfiles/testequals_point2.csv"
  val geojsonIdInputLocation = resourceFolder + "testContainsId.json"
  val smallAreasLocation: String = resourceFolder + "small/areas.csv"
  val smallPointsLocation: String = resourceFolder + "small/points.csv"
  val spatialJoinLeftInputLocation: String = resourceFolder + "spatial-predicates-test-data.tsv"
  val spatialJoinRightInputLocation: String = resourceFolder + "spatial-join-query-window.tsv"

  def sparkConf: SparkConf = new SparkConf(false)

  override def beforeAll(): Unit = {
    _spark = _baseSession.newSession()
    sparkConf.getAll.foreach { case (key, value) =>
      _spark.conf.set(key, value)
    }
    SedonaSQLRegistrator.registerAll(sparkSession)
  }

  override def afterAll(): Unit = {
    SedonaSQLRegistrator.dropAll(sparkSession)
    _spark = null
  }

  def loadCsv(path: String): DataFrame = {
    sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(path)
  }

  lazy val buildPointDf = loadCsv(csvPointInputLocation).selectExpr("ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20))) as pointshape")
  lazy val buildPolygonDf = loadCsv(csvPolygonInputLocation).selectExpr("ST_PolygonFromEnvelope(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20)), cast(_c2 as Decimal(24,20)), cast(_c3 as Decimal(24,20))) as polygonshape")
}
