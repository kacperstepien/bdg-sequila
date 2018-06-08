package org.biodatageeks.preprocessing.coverage

import java.io.{OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{Vectors,Vector}
//import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.linalg.VectorUDT

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
    "/home/kacper/Pobrane/c7_10M.bam"//,
    /*"/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam",
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
    "/home/kacper/Pobrane/exome/FIRST_300.HG00146.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam"*/
  )


  //val bamPath = "/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam"
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  //val tableNameBAM = "reads"

  //val (geneStart,geneEnd) = (17563439,17590994)
  val chr = "22"
  val (geneStart,geneEnd) = (17563439,17590994)
  //val (geneStart,geneEnd) = (17764180,17764259)

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
    breakable {
      for (bamPath <- fileList) {
        index = index+1
        val tableNameBAM = "reads"+index.toString
        val tableNameBAMSelect = "reads"+index.toString+"select"
        val columnName = "coverage"+index.toString
        println(bamPath)
        session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
        session.sql(
          s"""
             |CREATE TABLE ${tableNameBAM}
             |USING org.biodatageeks.datasources.BAM.BAMDataSource
             |OPTIONS(path "${bamPath}")
             |
          """.stripMargin)
        session.sql(s"SELECT * from ${tableNameBAM}")
        session.sql(s"DROP TABLE IF EXISTS ${tableNameBAMSelect}")
        session.sql(s"create table ${tableNameBAMSelect} as SELECT * from ${tableNameBAM} WHERE start <= ${geneEnd} and end >=${geneStart} and contigName=${chr}").show

        if (session.sql(s"SELECT * from ${tableNameBAMSelect}").count()>0) {
          //println("session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil")

          //session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAM}') where position >= ${geneStart} and position <= ${geneEnd}").show
          session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
          //println("session.sql(s\"SELECT * FROM coverage_hist('${tableNameBAM}')\").show")
          ////session.sql(s"SELECT * FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").show

          session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").show
          //session.sql(s"SELECT max(position),min(position) FROM coverage_hist('${tableNameBAM}')").show
          //print(session.sql(s"SELECT count(*) FROM coverage_hist('${tableNameBAMSelect}') where position >= ${geneStart} and position <= ${geneEnd} and contigName=${chr}").select("coverage").collect().map(_(0)).toList)

          if (index == 1) {
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
    dataset = dataset.select(columns.head,columns.tail:_* )
    dataset.show()
    var rowSeq:Seq[(Double,Vector)] = Seq()
    var idx = 0
    for (columnName<-columns) {
      idx = idx+1
      rowSeq = rowSeq.:+(idx.toDouble,
        Vectors.dense(
          dataset.select(columnName).map( x=>
            if (x.isNullAt(0)) {
              0.0 }
            else {
              x.getInt(0).toDouble
            }
          ).collect
        ))

    }
    dataset = sc.parallelize(rowSeq).toDF("label","features")
    dataset.show
    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Make predictions
    val predictions = model.transform(dataset)

    predictions.show

    //// Evaluate clustering by computing Silhouette score
    //val evaluator = new ClusteringEvaluator()

    //val silhouette = evaluator.evaluate(predictions)
    //println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    sc.stop()
    spark.stop()
}

}
