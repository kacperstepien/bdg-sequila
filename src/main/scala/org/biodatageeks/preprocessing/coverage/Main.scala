package org.biodatageeks.preprocessing.coverage

import java.io.{OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.VectorUDT

import scala.util.control.Breaks._
object Main {

  val fileList = List(
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
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ExtraStrategiesGenApp")
      .config("spark.master", "local")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    System.setSecurityManager(null)
    var index = 0
    val session: SparkSession = SequilaSession(spark)
    breakable {
      for (bamPath <- fileList) {
        index = index+1
        val tableNameBAM = "reads"+index.toString

        session.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
        session.sql(
          s"""
             |CREATE TABLE ${tableNameBAM}
             |USING org.biodatageeks.datasources.BAM.BAMDataSource
             |OPTIONS(path "${bamPath}")
             |
          """.stripMargin)


        session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
        session.sql(s"SELECT * FROM coverage_hist('${tableNameBAM}')").show
        //print(session.sql(s"SELECT * FROM coverage_hist('${tableNameBAM}')").select("coverage").collect().map(_(0)).toList)
        if (index == 2)
          break
      }
    }
    val dataset1 = session.sql(s"SELECT * FROM coverage_hist('reads1')").as[CoverageRecordHist]
    val dataset2 = session.sql(s"SELECT * FROM coverage_hist('reads2')").as[CoverageRecordHist]
    val dataset3 =dataset1.withColumnRenamed("coverageTotal","coverageTotal1")
      .join(dataset2.withColumnRenamed("coverageTotal","coverageTotal2"),Seq("contigName","position")).toDF()

    val df = dataset3.select("coverageTotal1","coverageTotal2")

    val row1 = df.select("coverageTotal1").map(_.getInt(0)).collect
    val row2 = df.select("coverageTotal1").map(_.getInt(0)).collect

    val d=sc.parallelize(Seq(
      (1.0, Vectors.dense(row1.map(x => x.toDouble))),
      (2.0, Vectors.dense(row2.map(x => x.toDouble)))
    )).toDF("label","features")
    d.show
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(d)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(d)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
}

}
