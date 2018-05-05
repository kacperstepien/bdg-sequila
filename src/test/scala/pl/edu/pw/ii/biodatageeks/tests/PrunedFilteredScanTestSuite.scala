package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}



import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.scalatest.{BeforeAndAfter, FunSuite}

class PrunedFilteredScanTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {
  val bamPath = getClass.getResource("/prunedFilteredScanTest").getPath
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameBAM = "reads"

  before{

    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(directory "${bamPath}")
         |
      """.stripMargin)
  }

  test("BAM - PrunedFilteredScan all"){
    assert(spark
      .sql(s"SELECT * FROM ${tableNameBAM}")
      .count === 12688L)
  }

  test("BAM - PrunedFilteredScan pruned"){
    assert(spark
      .sql(s"SELECT * FROM ${tableNameBAM} where sampleId like 'NA12880%'")
      .count === 6344L)
  }



  after{
    spark.sql(s"DROP TABLE IF EXISTS  ${tableNameBAM}")
    writer.flush()
  }
}
