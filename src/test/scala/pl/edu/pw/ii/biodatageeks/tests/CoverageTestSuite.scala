package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.DataFrame
import org.biodatageeks.coverage.{CoverageHistParam, CoverageHistType, CoverageRecordSlimHist}
import org.biodatageeks.datasources.ADAM.ADAMRecord
import org.biodatageeks.datasources.BAM.BAMRecord
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

  def setSchema( df: DataFrame, schema: StructType) : DataFrame = {
    df.sqlContext.createDataFrame(df.rdd, schema)
  }

  /*
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameADAM = "readsADAM"
  before{

    spark.sql(s"DROP TABLE IF EXISTS ${tableNameADAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameADAM}
         |USING org.biodatageeks.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "${adamPath}")
         |
      """.stripMargin)


  }


  test("ADAM - Row count BAMDataSource"){
    spark
      .sql(s"SELECT contigName,start,mapq,cigar FROM ${tableNameADAM}").show
  }
*/
  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
  val adamPath = getClass.getResource("/NA12878.slice.adam").getPath
  val covPath = getClass.getResource("/NA12878.slice.parquet").getPath

  test ("BAMDatasource to dataset"){

    import spark.implicits._
    val dataset = spark
      .read
      .format("org.biodatageeks.datasources.BAM.BAMDataSource")
      .load(bamPath)
      .as[BAMRecord]

    //dataset.foreach(rec => println(rec.start + " " + rec.contigName + " "+ rec.mapq +  " " + rec.cigar))
    val covDF = spark.read.load(covPath).as[CoverageRecordSlimHist]
    val coverage = setSchema(dataset.baseCoverageHistDataset(None, None,CoverageHistParam(CoverageHistType.MAPQ,Array(10,20,30,40))).toDF,covDF.schema).as[CoverageRecordSlimHist]

    //setSchema(coverage.toDF,covDF.schema).as[CoverageRecordSlimHist].printSchema()
    //coverage.orderBy(desc("coverageTotal")).show()
    //println(coverage.count)
    //coverage.as[CoverageRecordSlimHist].printSchema()
    //setNullableStateForAllColumns(coverage.toDF(),true).as[CoverageRecordSlimHist].printSchema()

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

  test ("ADAMDatasource to dataset"){

    import spark.implicits._
    val dataset = spark
      .read
      .format("org.biodatageeks.datasources.ADAM.ADAMDataSource")
      .load(adamPath)
      .as[ADAMRecord].map { rec => {
      if (!rec.readMapped)
        ADAMRecord(rec.readInFragment,
          null,
          rec.start+1,
          rec.oldPosition,
          rec.end,
          rec.mapq,
          rec.readName,
          rec.sequence,
          rec.qual,
          rec.cigar,
          rec.oldCigar,
          rec.basesTrimmedFromStart,
          rec.basesTrimmedFromEnd,
          rec.readPaired,
          rec.properPair,
          rec.readMapped,
          rec.mateMapped,
          rec.failedVendorQualityChecks,
          rec.duplicateRead,
          rec.readNegativeStrand,
          rec.mateNegativeStrand,
          rec.primaryAlignment,
          rec.secondaryAlignment,
          rec.supplementaryAlignment,
          rec.mismatchingPositions,
          rec.origQual,
          rec.attributes,
          rec.recordGroupName,
          rec.recordGroupSample,
          rec.mateAlignmentStart,
          rec.mateContigName,
          rec.inferredInsertSize
        ) else
        ADAMRecord(rec.readInFragment,
          rec.contigName,
          rec.start+1,
          rec.oldPosition,
          rec.end,
          rec.mapq,
          rec.readName,
          rec.sequence,
          rec.qual,
          rec.cigar,
          rec.oldCigar,
          rec.basesTrimmedFromStart,
          rec.basesTrimmedFromEnd,
          rec.readPaired,
          rec.properPair,
          rec.readMapped,
          rec.mateMapped,
          rec.failedVendorQualityChecks,
          rec.duplicateRead,
          rec.readNegativeStrand,
          rec.mateNegativeStrand,
          rec.primaryAlignment,
          rec.secondaryAlignment,
          rec.supplementaryAlignment,
          rec.mismatchingPositions,
          rec.origQual,
          rec.attributes,
          rec.recordGroupName,
          rec.recordGroupSample,
          rec.mateAlignmentStart,
          rec.mateContigName,
          rec.inferredInsertSize
        )
      }
    }
    //dataset.foreach(rec => println(rec.start + " " + rec.contigName+ " "+ rec.mapq + " " + rec.cigar))
    //spark.sql("select contigName from ADAM").show
    //dataset.printSchema
    //dataset.select(col("contigName"),col("start"),col("mapq"),col("cigar")).show()
    //dataset.show()
    val covDF = spark.read.load(covPath).as[CoverageRecordSlimHist]
    val coverage = setSchema(dataset.baseCoverageHistDataset(None, None,CoverageHistParam(CoverageHistType.MAPQ,Array(10,20,30,40))).toDF,covDF.schema).as[CoverageRecordSlimHist]

    //setSchema(coverage.toDF,covDF.schema).as[CoverageRecordSlimHist].printSchema()
    //coverage.orderBy(desc("coverageTotal")).show()
    //println(coverage.count)
    //coverage.as[CoverageRecordSlimHist].printSchema()
    //coverage.printSchema()
    //setNullableStateForAllColumns(coverage.toDF(),true).as[CoverageRecordSlimHist].printSchema()
    //covDF.orderBy(desc("coverageTotal")).show()
    //coverage.unionAll(covDF).except(coverage.intersect(covDF)).show()
    //println(covDF.count)
    //covDF.as[CoverageRecordSlimHist].printSchema()
    //coverage.except(covDF).explain
    //covDF.except(coverage).explain
    //coverage.except(covDF).show
    //covDF.except(coverage).show
    //var fileName = new SimpleDateFormat("'/home/kacper/coverage_ADAM_'yyyyMMddHHmm'.parquet'").format(new Date())
    //coverage.saveCoverageAsParquet(fileName,sort=false)

    assert(coverage.except(covDF).count==0 &&covDF.except(coverage).count==0)
  }
}
