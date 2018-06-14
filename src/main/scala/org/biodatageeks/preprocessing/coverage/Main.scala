package org.biodatageeks.preprocessing.coverage

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import au.com.bytecode.opencsv.CSVWriter
import htsjdk.samtools.SAMFlag
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture, KMeans, LDA}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.rogach.scallop.ScallopConf

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
//import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.functions._

import scala.util.control.Breaks._
object Main {

  val fileList = List(
    "/home/kacper/Pobrane/n1_10M.bam",
    "/home/kacper/Pobrane/c1_10M.bam",
  "/home/kacper/Pobrane/n2_10M.bam",
  "/home/kacper/Pobrane/c2_10M.bam",
  "/home/kacper/Pobrane/n3_10M.bam",
  "/home/kacper/Pobrane/c3_10M.bam",
  "/home/kacper/Pobrane/n4_10M.bam",
  "/home/kacper/Pobrane/c4_10M.bam",
    "/home/kacper/Pobrane/n5_10M.bam",
    "/home/kacper/Pobrane/c5_10M.bam",
    "/home/kacper/Pobrane/n6_10M.bam",
    "/home/kacper/Pobrane/c6_10M.bam",
    "/home/kacper/Pobrane/n7_10M.bam",
    "/home/kacper/Pobrane/c7_10M.bam"
  )


  //val bamPath = "/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam"
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  //val tableNameBAM = "reads"

  //val (geneStart,geneEnd) = (17563439,17590994)
  //val chr = "22"
  //val (geneStart,geneEnd) = (17418697,17419828) //153 kolumny, wszędzie coś jest
  //val (geneStart,geneEnd) = (17424388,17424775) //null
  //val (geneStart,geneEnd) = (17518676,17519029) // 2 rekordy po 49
  //val (geneStart,geneEnd) = (17563439,17590994)
  //val (geneStart,geneEnd) = (17764180,17764259) //null
  //val (geneStart,geneEnd) = (17980868,17980961) //null
  //val (geneStart,geneEnd) = (10736171,10736283) //null pointer
  val flag = 0x10
  class RunConf(args:Array[String]) extends ScallopConf(args){

    val start = opt[Int](required = true)
    val end = opt[Int](required = true)
    val chr = opt[String](required = true)
    val strand = opt[String](required = true)
    val bamList= opt[String](required = true)
    verify()
  }

  def main(args: Array[String]) {
    val runConf = new RunConf(args)

    val chr = runConf.chr.apply()
    val (geneStart,geneEnd) = (runConf.start.apply(),runConf.end.apply())

    var strandSql = ""

    runConf.strand.apply() match {
      case "+" => strandSql = s" and flags & ${flag} = 0"
      case "-" => strandSql = s" and flags & ${flag} <> 0"
    }

    var buf = new ListBuffer[String]
    val filename = runConf.bamList.apply()
    for (line <- Source.fromFile(filename).getLines) {
      println(line)
      buf = buf :+ line;
    }
    val fileList = buf.toList/*.take(2)*/;

    val fileNames = fileList.map(_.split("/").last)
    val filePostfix = runConf.chr.apply()+"_"+runConf.start.apply().toString +"-"+runConf.end.apply().toString+"_"+runConf.strand.apply()
    val spark = SparkSession
      .builder()
      .appName("CoverageClustering")
      .config("spark.master", "local")
      .config("spark.sql.catalogImplementation","hive")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    System.setSecurityManager(null)
    var index = 0
    val session: SparkSession = SequilaSession(spark)
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
    var dataset:DataFrame = null
    var columns:Array[String] = Array()
    var lineCounts:Array[Int] = Array()
    var positionCounts:Array[Int] = Array()
    var coverageSums:Array[Int] = Array()
    var allColumns:Array[String] = Array()
    breakable {
      for (bamPath <- fileList) {
        index = index+1
        val tableNameBAM = "reads"+index.toString
        val tableNameBAMSelect = "reads"+index.toString+"select"
        val columnName = "coverage"+index.toString
        allColumns = allColumns :+ columnName
        println(bamPath)
        session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
        session.sql(
          s"""
             |CREATE TABLE ${tableNameBAM}
             |USING org.biodatageeks.datasources.BAM.BAMDataSource
             |OPTIONS(path "${bamPath}")
             |
          """.stripMargin)
        //session.sql(s"SELECT start,count(*) from ${tableNameBAM} where contigName='22' group by start").show

        session.sql(s"DROP TABLE IF EXISTS ${tableNameBAMSelect}")

        //session.sql(s"SELECT flags,flags & ${flag} from ${tableNameBAM} WHERE start <= ${geneEnd}").show
        session.sql(s"create table ${tableNameBAMSelect} as SELECT * from ${tableNameBAM} WHERE start <= ${geneEnd} and end >=${geneStart} and contigName=${chr}"+strandSql)
        lineCounts=lineCounts:+ session.sql(s"SELECT count(*) from ${tableNameBAMSelect}").first().getLong(0).toInt
        if (session.sql(s"SELECT * from ${tableNameBAMSelect}").count()>0) {
          //println("session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil")

          //session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAM}') where position >= ${geneStart} and position <= ${geneEnd}").show
          session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
          //println("session.sql(s\"SELECT * FROM coverage_hist('${tableNameBAM}')\").show")
          ////session.sql(s"SELECT * FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").show

          positionCounts=positionCounts:+ session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").first().getLong(0).toInt
          coverageSums=coverageSums:+session.sql(s"SELECT sum(coverageTotal) FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").first().getLong(0).toInt
          //session.sql(s"SELECT max(position),min(position) FROM coverage_hist('${tableNameBAM}')").show
          //print(session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").select("coverage").collect().map(_(0)).toList)

          if (index == 1 || dataset == null) {
            dataset = session
              .sql(s"SELECT * FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}")
              .as[CoverageRecordHist]
              .withColumnRenamed("coverageTotal",columnName)
          } else {
            dataset = dataset.join(session
              .sql(s"SELECT * FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}")
              .as[CoverageRecordHist]
              .withColumnRenamed("coverageTotal",columnName),
              Seq("contigName","position"),"FULL_OUTER"
            ).toDF
          }
          //session.sql(s"DROP TABLE IF EXISTS ${tableNameBAMSelect}")
          columns = columns :+ columnName
        } else {
          positionCounts=positionCounts:+0
          coverageSums=coverageSums:+0
          session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
          session.sql(s"DROP TABLE IF EXISTS ${tableNameBAMSelect}")
        }
      }
    }
    if (dataset != null) {
      dataset = dataset.orderBy("position")

      val positions = dataset.select("position").map(x =>
          x.getInt(0)
      ).collect

      //positions.foreach(println)

      dataset.show
      if (dataset.count()>0) {
        dataset = dataset.select(columns.head, columns.tail: _*)
        dataset.groupBy().sum().show
        val count = dataset.count()
        dataset.show()

        var rowSeq: Seq[(Double, Vector)] = Seq()
        var idx = 0
        for (columnName <- allColumns) {
          idx = idx + 1
          if (columns contains columnName) {
            rowSeq = rowSeq.:+(idx.toDouble,
              Vectors.dense(
                dataset.select(columnName).map(x =>
                  if (x.isNullAt(0)) {
                    0.0
                  }
                  else {
                    x.getInt(0).toDouble
                  }
                ).collect
              ))
          } else {
            rowSeq = rowSeq.:+(idx.toDouble,
              Vectors.zeros(count.toInt)
            )
          }
        }


        val coverageFile = new BufferedWriter(new FileWriter("coverage"+filePostfix+".csv")) //replace the path with the desired path and filename with the desired filename

        var csvWriter = new CSVWriter(coverageFile)

        var csvFields = "Sample"+: positions.map(x=>x.toString)

        var listOfRecords = new ListBuffer[Array[String]]()

        listOfRecords += csvFields

        for (i <- 0 to (rowSeq.size-1)) {
          listOfRecords += fileNames.apply(i)+:rowSeq.apply(i)._2.toArray.map(x=>x.toInt.toString)
        }

        csvWriter.writeAll(listOfRecords.toList)

        coverageFile.close()



        dataset = sc.parallelize(rowSeq).toDF("label", "features")
        dataset.show()
        //val fileName = new SimpleDateFormat("'preprocessed'yyyyMMddHHmm'.parquet'").format(new Date())
        //dataset.write.format("parquet").save("/home/kacper/"+fileName)
        // Trains a k-means model.

        var predictionArray: Array[Array[Int]] = Array()
        var clusterCounts:Array[Int] = Array()
        for (idx <- 2 to (fileList.size)) {
          clusterCounts = clusterCounts:+idx
          val kmeans = new KMeans().setK(idx).setSeed(1L)
          val modelKMEANS = kmeans.fit(dataset)

          // Make predictions
          val predictions = modelKMEANS.transform(dataset)

          predictions.show

          predictionArray = predictionArray:+predictions.select("prediction").map(x =>
            x.getInt(0)
          ).collect

        }

        val clusteringFile = new BufferedWriter(new FileWriter("clustering"+filePostfix+".csv")) //replace the path with the desired path and filename with the desired filename

        csvWriter = new CSVWriter(clusteringFile)

        csvFields = Array("Sample","Lines","Positions","CoverageSum")++clusterCounts.map(x=>x.toString)

        listOfRecords = new ListBuffer[Array[String]]()

        listOfRecords += csvFields

        for (i <- 0 to (fileNames.size-1)) {
          listOfRecords += Array(
            fileNames.apply(i),
            lineCounts.apply(i).toString,
            positionCounts.apply(i).toString,
            coverageSums.apply(i).toString)++
            predictionArray.map(x=>x.apply(i).toString)
        }

        csvWriter.writeAll(listOfRecords.toList)

        clusteringFile.close()

      }
    }
    sc.stop()
    spark.stop()
}

}
