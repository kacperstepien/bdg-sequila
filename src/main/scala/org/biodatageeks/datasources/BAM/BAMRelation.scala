package org.biodatageeks.datasources.BAM

import java.io.File

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}


case class BAMRecord(sampleId: String,
                     contigName:String,
                     start:Int,
                     end:Int,
                     cigar:String,
                     mapq:Int,
                     baseq: String,
                     reference:String,
                     flags:Int,
                     materefind:Int)

class BAMRelation (path:String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan  {

  val spark = sqlContext
    .sparkSession
  spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

  spark
    .sparkContext
    .hadoopConfiguration
    .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

  override def schema: org.apache.spark.sql.types.StructType = {
    StructType(
      Seq(
        new StructField("sampleId", StringType),
        new StructField("contigName", StringType),
        new StructField("start", IntegerType),
        new StructField("end", IntegerType),
        new StructField("cigar", StringType),
        new StructField("mapq", IntegerType),
        new StructField("baseq", StringType),
        new StructField("reference", StringType),
        new StructField("flags", IntegerType),
        new StructField("materefind", IntegerType)

      )
    )
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val input = if ( path.endsWith(".bam")) path else getInput(filters)

    val alignments = spark
      .sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](input)
    val alignmentsWithFileName = alignments.asInstanceOf[NewHadoopRDD[LongWritable, SAMRecordWritable]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val file = inputSplit.asInstanceOf[FileVirtualSplit]
        iterator.map(tup => (file.getPath.getName.split('.')(0), tup._2))
      }
      )
    val sampleAlignments = alignmentsWithFileName
      .map(r => (r._1, r._2.get()))
      .map { case (sampleId, r) =>
        BAMRecord(sampleId,r.getContig, r.getStart, r.getEnd, r.getCigar.toString,
          r.getMappingQuality, r.getBaseQualityString, r.getReferenceName,
          r.getFlags, r.getMateReferenceIndex)
      }


    //     val alignments = spark
    //      .sparkContext.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path)
    //      .map(_._2.get)
    //      .map(r => BAMRecord(r.getContig, r.getStart, r.getEnd,r.getCigar.toString,
    //        r.getMappingQuality, r.getBaseQualityString, r.getReferenceName,
    //        r.getFlags, r.getMateReferenceIndex))

    val readsTable = spark
      .sqlContext
      .createDataFrame(sampleAlignments)
    readsTable
      .rdd
  }

  private def getRecursiveListOfFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }

  private def getInput(filters: Array[Filter]) :String = {
    var directories = getRecursiveListOfFiles(new File(path)).filter(_.isDirectory)

    filters.foreach { f =>
      f match {
        case EqualTo(attr, value) => directories = directories filter {dir => attr == "sampleId" && dir.getName == value}
        case StringStartsWith(attr, value) => directories = directories filter {dir => attr == "sampleId" && dir.getName.startsWith(value)}
        case StringEndsWith(attr, value) => directories = directories filter {dir => attr == "sampleId" && dir.getName.endsWith(value)}
        case StringContains(attr, value) => directories = directories filter {dir => attr == "sampleId" && dir.getName.contains(value)}
        case _ =>
      }
    }
    directories.map(_.getPath).flatMap(x=>getRecursiveListOfFiles(new File(x)))
      .filter(!_.isDirectory)
      .map(_.getPath)
      .mkString(",")
  }

}