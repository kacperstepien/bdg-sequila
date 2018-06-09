package org.biodatageeks.preprocessing.coverage

import java.io.{OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture, KMeans, LDA}
import org.apache.spark.ml.linalg.{Vector, Vectors}
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
    "/home/kacper/Pobrane/c7_10M.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00100.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00101.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00102.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00103.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00107.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00108.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00109.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00110.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00111.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00113.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00114.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00116.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00117.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00119.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00120.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00121.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00123.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00124.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00125.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00127.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00131.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00133.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00136.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00137.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00138.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00139.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00140.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00145.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
    "/home/kacper/Pobrane/exome/FIRST_300.HG00146.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam"
  )


  //val bamPath = "/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam"
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  //val tableNameBAM = "reads"

  //val (geneStart,geneEnd) = (17563439,17590994)
  val chr = "22"
  val (geneStart,geneEnd) = (17418697,17419828) //153 kolumny, wszędzie coś jest
  //val (geneStart,geneEnd) = (17424388,17424775) //null
  //val (geneStart,geneEnd) = (17518676,17519029) // 2 rekordy po 49
  //val (geneStart,geneEnd) = (17563439,17590994)
  //val (geneStart,geneEnd) = (17764180,17764259) //null
  //val (geneStart,geneEnd) = (17980868,17980961) //null
  //val (geneStart,geneEnd) = (10736171,10736283) //null pointer

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ExtraStrategiesGenApp")
      .config("spark.master", "local")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "8g")
      .config("spark.sql.catalogImplementation","hive")
      //.config("spark.memory.fraction","0.6")
      //.config("spark.executor.extrajavaoptions","Xmx1024m")
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
        session.sql(s"SELECT start,count(*) from ${tableNameBAM} where contigName='22' group by start").show

        session.sql(s"DROP TABLE IF EXISTS ${tableNameBAMSelect}")
        session.sql(s"create table ${tableNameBAMSelect} as SELECT * from ${tableNameBAM} WHERE start <= ${geneEnd} and end >=${geneStart} and contigName=${chr}")

        if (session.sql(s"SELECT * from ${tableNameBAMSelect}").count()>0) {
          //println("session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil")

          //session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAM}') where position >= ${geneStart} and position <= ${geneEnd}").show
          session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
          //println("session.sql(s\"SELECT * FROM coverage_hist('${tableNameBAM}')\").show")
          ////session.sql(s"SELECT * FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").show

          session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").show
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
          columns = columns :+ columnName
        } else {
          session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
          session.sql(s"DROP TABLE IF EXISTS ${tableNameBAMSelect}")
        }
      }
    }

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
      dataset = sc.parallelize(rowSeq).toDF("label", "features")
      dataset.show(false)
      val fileName = new SimpleDateFormat("'preprocessed'yyyyMMddHHmm'.parquet'").format(new Date())
      dataset.write.format("parquet").save("/home/kacper/"+fileName)

      dataset.foreach(f => println(f.getAs[Vector](1).size))
      // Trains a k-means model.


      for (idx <- 2 to (fileList.size / 2)) {

        val kmeans = new KMeans().setK(idx).setSeed(1L)
        val modelKMEANS = kmeans.fit(dataset)

        // Make predictions
        val predictions = modelKMEANS.transform(dataset)

        predictions.show
      }

      //// Evaluate clustering by computing Silhouette score
      //val evaluator = new ClusteringEvaluator()

      //val silhouette = evaluator.evaluate(predictions)
      //println(s"Silhouette with squared euclidean distance = $silhouette")

      // Shows the result.

      /*val lda = new LDA().setK(2).setMaxIter(10)
      val modelLDA = lda.fit(dataset)

      val ll = modelLDA.logLikelihood(dataset)
      val lp = modelLDA.logPerplexity(dataset)
      println(s"The lower bound on the log likelihood of the entire corpus: $ll")
      println(s"The upper bound on perplexity: $lp")

      // Describe topics.
      val topics = modelLDA.describeTopics(3)
      println("The topics described by their top-weighted terms:")
      topics.show(false)

      // Shows the result.
      val transformedLDA = modelLDA.transform(dataset)
      transformedLDA.show(false)*/
/*
      val bkm = new BisectingKMeans().setK(2).setSeed(1)
      val modelBKM = bkm.fit(dataset)

      // Evaluate clustering.
      val cost = modelBKM.computeCost(dataset)
      println(s"Within Set Sum of Squared Errors = $cost")

      val transformedBKM = modelBKM.transform(dataset)
      transformedBKM.select("label","prediction").show(false)

      val gmm = new GaussianMixture()
        .setK(2)
      val modelGMM = gmm.fit(dataset)

      // output parameters of mixture model model
      for (i <- 0 until modelGMM.getK) {
        println(s"Gaussian $i:\nweight=${modelGMM.weights(i)}\n" +
          s"mu=${modelGMM.gaussians(i).mean}\nsigma=\n${modelGMM.gaussians(i).cov}\n")
      }

      val transformedGMM = modelGMM.transform(dataset)
      transformedGMM.select("label","prediction").show(false)*/
    }
    sc.stop()
    spark.stop()
}

}
