package org.biodatageeks.coverage

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.biodatageeks.datasources.BAM.BAMRecord
import org.biodatageeks.coverage.CoverageReadBAMFunctions._
import org.biodatageeks.coverage.CoverageFunctionsSlimHist._
import org.biodatageeks.coverage.CoverageHistType.{CoverageHistType, Value}
object Static {
  def  coverage(spark: SparkSession,inputPath:String,outputPath:String,histTypeName:String,bucketsDouble:Array[Double]/*,bucketsString:Array[String]*/ )= {
    import spark.implicits._
    val dataset = spark
    .read
    .format("org.biodatageeks.datasources.BAM.BAMDataSource")
    .load(inputPath)
    .as[BAMRecord]

     val histType:CoverageHistType = histTypeName.toUpperCase match {
      case "MAPQ" =>
        CoverageHistType.MAPQ
      case "SAMPLEID" =>
        CoverageHistType.SAMPLEID
      case "CONTIGNAME" =>
        CoverageHistType.CONTIGNAME
      case "START" =>
        CoverageHistType.START
      case "END" =>
        CoverageHistType.END
      case "CIGAR" =>
        CoverageHistType.CIGAR
      case "BASEQ" =>
        CoverageHistType.BASEQ
    }


    val bucketsString = Array[String]()
    val coverage = dataset.baseCoverageHistDataset(None, None,CoverageHistParam(histType,bucketsDouble,bucketsString))
    coverage.saveCoverageAsParquet(outputPath,sort=false)
  }
}
