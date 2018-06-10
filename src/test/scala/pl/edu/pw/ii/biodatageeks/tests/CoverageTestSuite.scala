package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.datasources.BAM.BAMRecord
import org.biodatageeks.preprocessing.coverage.CoverageStrategy
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.biodatageeks.coverage.CoverageReadBAMFunctions._
import org.biodatageeks.coverage.CoverageReadADAMFunctions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructType}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.coverage.CoverageFunctionsSlimHist._
class CoverageTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{
  def setNullableStateForAllColumns( df: DataFrame, nullable: Boolean) : DataFrame = {
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(s=> { println(s.dataType.json); if (s.dataType==ArrayType) s.copy(nullable = nullable, dataType = ArrayType(IntegerType,containsNull=true)) else s.copy(nullable = nullable)} )))
  }

    val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
    val adamPath = getClass.getResource("/NA12878.slice.adam").getPath
    val metricsListener = new MetricsListener(new RecordedMetrics())
    val writer = new PrintWriter(new OutputStreamWriter(System.out))
    val tableNameBAM = "reads"
    val tableNameADAM = "readsADAM"
    before{

      Metrics.initialize(sc)
      sc.addSparkListener(metricsListener)
      System.setSecurityManager(null)
      spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
      spark.sql(
        s"""
           |CREATE TABLE ${tableNameBAM}
           |USING org.biodatageeks.datasources.BAM.BAMDataSource
           |OPTIONS(path "${bamPath}")
           |
      """.stripMargin)

      spark.sql(s"DROP TABLE IF EXISTS ${tableNameADAM}")
      spark.sql(
        s"""
           |CREATE TABLE ${tableNameADAM}
           |USING org.biodatageeks.datasources.ADAM.ADAMDataSource
           |OPTIONS(path "${adamPath}")
           |
      """.stripMargin)

    }
  test("BAM - coverage table-valued function"){
    val session: SparkSession = SequilaSession(spark)

    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
    //session.sparkContext.setLogLevel("INFO")
    assert(session.sql(s"SELECT * FROM coverage('${tableNameBAM}') WHERE position=20204").first().getInt(3)===1019)
    session.sql(s"SELECT * FROM coverage_hist('${tableNameBAM}') WHERE position=20204").show()

    //covDF.orderBy(desc("coverageTotal")).show()

    //coverage.unionAll(covDF).except(coverage.intersect(covDF)).show()
    //println(covDF.count)
    //covDF.as[CoverageRecordSlimHist].printSchema()
    //coverage.except(covDF).explain
    //covDF.except(coverage).explain
    //coverage.except(covDF).show
    //covDF.except(coverage).show


    assert(coverage.except(covDF).count==0 &&covDF.except(coverage).count==0)
  }

}
