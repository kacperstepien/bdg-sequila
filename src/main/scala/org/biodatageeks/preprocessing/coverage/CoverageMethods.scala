package org.biodatageeks.preprocessing.coverage

import breeze.numerics.{exp, log}
import htsjdk.samtools.{Cigar, CigarOperator, SAMUtils, TextCigarCodec}
import org.apache.commons.math3.stat.StatUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.datasources.BAM.BAMRecord
import org.biodatageeks.preprocessing.coverage.CoverageHistType.CoverageHistType
import org.biodatageeks.preprocessing.coverage.NormalizationType.NormalizationType
//import org.biodatageeks.preprocessing.coverage.NormalizationType.NormalizationType

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class CoverageRecord(sampleId:String,
                          contigName:String,
                              position:Int,
                              coverage:Int)

case class CoverageRecordHist(sampleId:String,
                              contigName:String,
                                  position:Int,
                                  coverage:Array[Int],
                                  coverageTotal:Int)

case class PartitionCoverage(covMap: mutable.HashMap[(String, Int), Array[Int]],
                             maxCigarLength: Int,
                             outputSize: Int,
                             chrMinMax: Array[(String,Int)] )

case class PartitionCoverageHist(covMap: mutable.HashMap[(String, Int), (Array[Array[Int]],Array[Int])],
                                 maxCigarLength: Int,
                                 outputSize: Int,
                                 chrMinMax: Array[(String,Int)]
                                )
object CoverageHistType extends Enumeration{
  type CoverageHistType = Value
  val MAPQ = Value
}
case class CoverageHistParam(
                              histType : CoverageHistType,
                              buckets: Array[Double]
                            )

class CoverageReadFunctions(covReadRDD:RDD[BAMRecord]) extends Serializable {

  def baseCoverage(minMapq: Option[Int], numTasks: Option[Int] = None, sorted: Boolean):RDD[CoverageRecord] ={
    val sampleId = covReadRDD
      .first()
      .sampleId
    lazy val cov =numTasks match {
        case Some(n) => covReadRDD.repartition(n)
        case _ => covReadRDD
      }

      lazy val covQual = minMapq match {
        case Some(qual) => cov //FIXME add filtering
        case _ => cov
      }
        lazy val partCov = {
          sorted match {
            case true => covQual//.instrument()
            case _ => covQual.sortBy(r => (r.contigName, r.start))
          }
        }.mapPartitions { partIterator =>
          val covMap = new mutable.HashMap[(String, Int), Array[Int]]()
          val numSubArrays = 10000
          val subArraySize = 250000000 / numSubArrays
          val chrMinMax = new ArrayBuffer[(String, Int)]()
          var maxCigarLength = 0
          var lastChr = "NA"
          var lastPosition = 0
          var outputSize = 0

          for (cr <- partIterator) {
            val cigar = TextCigarCodec.decode(cr.cigar)
            val cigIterator = cigar.iterator()
            var position = cr.start
            val cigarStartPosition = position
            while (cigIterator.hasNext) {
              val cigarElement = cigIterator.next()
              val cigarOpLength = cigarElement.getLength
              val cigarOp = cigarElement.getOperator
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                var currPosition = 0
                while (currPosition < cigarOpLength) {
                  val subIndex = position % numSubArrays
                  val index = position / numSubArrays

                  if (!covMap.keySet.contains(cr.contigName, index)) {
                    covMap += (cr.contigName, index) -> Array.fill[Int](subArraySize)(0)
                  }
                  covMap(cr.contigName, index)(subIndex) += 1
                  position += 1
                  currPosition += 1

                  /*add first*/
                  if (outputSize == 0) chrMinMax.append((cr.contigName, position))
                  if (covMap(cr.contigName, index)(subIndex) == 1) outputSize += 1

                }
              }
              else if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
            }
            val currLength = position - cigarStartPosition
            if (maxCigarLength < currLength) maxCigarLength = currLength
            lastPosition = position
            lastChr = cr.contigName
          }
          chrMinMax.append((lastChr, lastPosition))
          Array(PartitionCoverage(covMap, maxCigarLength, outputSize, chrMinMax.toArray)).iterator
        }.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val maxCigarLengthGlobal = partCov.map(r => r.maxCigarLength).reduce((a, b) => scala.math.max(a, b))
        lazy val combOutput = partCov.mapPartitions { partIterator =>
          /*split for reduction basing on position and max cigar length across all partitions - for gap alignments*/
          val partitionCoverageArray = (partIterator.toArray)
          val partitionCoverage = partitionCoverageArray(0)
          val chrMinMax = partitionCoverage.chrMinMax
          lazy val output = new Array[Array[CoverageRecord]](2)
          lazy val outputArray = new Array[CoverageRecord](partitionCoverage.outputSize)
          lazy val outputArraytoReduce = new ArrayBuffer[CoverageRecord]()
          val covMap = partitionCoverage.covMap
          var cnt = 0
          for (key <- covMap.keys) {
            var locCnt = 0
            for (value <- covMap.get(key).get) {
              if (value > 0) {
                val position = key._2 * 10000 + locCnt
                if (key._1 == chrMinMax.head._1 && position <= chrMinMax.head._2 + maxCigarLengthGlobal ||
                  key._1 == chrMinMax.last._1 && position >= chrMinMax.last._2 - maxCigarLengthGlobal)
                  outputArraytoReduce.append(CoverageRecord(sampleId,key._1, position, value))
                else
                  outputArray(cnt) = (CoverageRecord(sampleId,key._1, position, value))
                cnt += 1
              }
              locCnt += 1
            }
          } /*only records from the beginning and end of the partition for reduction the rest pass-through */
          output(0) = outputArray.filter(r => r != null)
          output(1) = outputArraytoReduce.toArray
          Iterator(output)
        }
        //partCov.unpersist()
        lazy val covReduced = combOutput.flatMap(r => r.array(1)).map(r => ((r.contigName, r.position), r))
          .reduceByKey((a, b) => CoverageRecord(sampleId,a.contigName, a.position, a.coverage + b.coverage))
          .map(_._2)
        partCov.unpersist()
        combOutput.flatMap(r => (r.array(0)))
          .union(covReduced)

  }
  def baseCoverageHist(minMapq: Option[Int], numTasks: Option[Int] = None, coverageHistParam: CoverageHistParam) /*: RDD[CoverageRecordSlimHist]*/ = {
    val sampleId = covReadRDD
      .first()
      .sampleId
    lazy val cov =numTasks match {
      case Some(n) => covReadRDD.repartition(n)
      case _ => covReadRDD
    }

    lazy val covQual = minMapq match {
      case Some(qual) => cov //FIXME add filtering
      case _ => cov
    }
    lazy val partCov = covQual
      .sortBy(r => (r.contigName, r.start))
      .mapPartitions { partIterator =>
        val covMap = new mutable.HashMap[(String, Int), (Array[Array[Int]], Array[Int])]()
        val numSubArrays = 10000
        val subArraySize = 250000000 / numSubArrays
        val chrMinMax = new ArrayBuffer[(String, Int)]()
        var maxCigarLength = 0
        var lastChr = "NA"
        var lastPosition = 0
        var outputSize = 0
        for (cr <- partIterator) {
          val cigar = TextCigarCodec.decode(cr.cigar)
          val cigInterator = cigar.getCigarElements.iterator()
          var position = cr.start
          val cigarStartPosition = position
          while (cigInterator.hasNext) {
            val cigarElement = cigInterator.next()
            val cigarOpLength = cigarElement.getLength
            val cigarOp = cigarElement.getOperator
            cigarOp match {
              case CigarOperator.M | CigarOperator.X | CigarOperator.EQ =>
                var currPosition = 0
                while (currPosition < cigarOpLength) {
                  val subIndex = position % numSubArrays
                  val index = position / numSubArrays

                  if (!covMap.keySet.contains(cr.contigName, index)) {
                    covMap += (cr.contigName, index) -> (Array.ofDim[Int](numSubArrays,coverageHistParam.buckets.length),Array.fill[Int](numSubArrays)(0) )
                  }
                  val params = coverageHistParam.buckets.sortBy(r=>r)
                  if(coverageHistParam.histType == CoverageHistType.MAPQ) {
                    breakable {
                      for (i <- 0 until params.length) {
                        if ( i < params.length-1  && cr.mapq >= params(i) && cr.mapq < params(i+1)) {
                          covMap(cr.contigName, index)._1(subIndex)(i) += 1
                          break
                        }
                      }

                    }
                    if (cr.mapq >= params.last) covMap(cr.contigName, index)._1(subIndex)(params.length-1) += 1
                  }
                  else throw new Exception("Unsupported histogram parameter")


                  covMap(cr.contigName, index)._2(subIndex) += 1

                  position += 1
                  currPosition += 1
                  /*add first*/
                  if (outputSize == 0) chrMinMax.append((cr.contigName, position))
                  if (covMap(cr.contigName, index)._2(subIndex) == 1) outputSize += 1

                }
              case CigarOperator.N | CigarOperator.D => position += cigarOpLength
              case _ => None
            }
          }
          val currLength = position - cigarStartPosition
          if (maxCigarLength < currLength) maxCigarLength = currLength
          lastPosition = position
          lastChr = cr.contigName
        }
        chrMinMax.append((lastChr, lastPosition))
        Array(PartitionCoverageHist(covMap, maxCigarLength, outputSize, chrMinMax.toArray)).iterator
      }.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val maxCigarLengthGlobal = partCov.map(r => r.maxCigarLength).reduce((a, b) => scala.math.max(a, b))
    lazy val combOutput = partCov.mapPartitions { partIterator =>
      /*split for reduction basing on position and max cigar length across all partitions - for gap alignments*/
      val partitionCoverageArray = (partIterator.toArray)
      val partitionCoverage = partitionCoverageArray(0)
      val chrMinMax = partitionCoverage.chrMinMax
      lazy val output = new Array[Array[CoverageRecordHist]](2)
      lazy val outputArray = new Array[CoverageRecordHist](partitionCoverage.outputSize)
      lazy val outputArraytoReduce = new ArrayBuffer[CoverageRecordHist]()
      val covMap = partitionCoverage.covMap
      var cnt = 0
      for (key <- covMap.keys) {
        var locCnt = 0
        val covs = covMap.get(key).get
        for (i<-0 until covs._1.length) {
          if (covs._2(i) > 0) {
            val position = key._2 * 10000 + locCnt
            if(key._1 == chrMinMax.head._1 && position <= chrMinMax.head._2 + maxCigarLengthGlobal ||
              key._1 == chrMinMax.last._1 && position >= chrMinMax.last._2 - maxCigarLengthGlobal )
              outputArraytoReduce.append(CoverageRecordHist(sampleId,key._1,position,covs._1(i),covs._2(i)))
            else
              outputArray(cnt) = CoverageRecordHist(sampleId,key._1,position ,covs._1(i),covs._2(i))
            cnt += 1
          }
          locCnt += 1
        }
      } /*only records from the beginning and end of the partition for reduction the rest pass-through */
      output(0) = outputArray.filter(r=> r!=null )
      output(1) = outputArraytoReduce.toArray
      Iterator(output)
    }
    //partCov.unpersist()
    lazy val covReduced =  combOutput.flatMap(r=>r.array(1)).map(r=>((r.contigName,r.position),r))
      .reduceByKey((a,b)=>CoverageRecordHist(sampleId,a.contigName,a.position,sumArrays(a.coverage,b.coverage),a.coverageTotal+b.coverageTotal)).map(_._2)
    partCov.unpersist()
    combOutput.flatMap(r => (r.array(0)))
      .union(covReduced)
  }
  private def sumArrays(a:Array[Int], b:Array[Int]) ={
    val out = new Array[Int](a.length)
    for(i<- 0 until a.length){
      out(i) = a(i) + b(i)
    }
    out
  }
}

object CoverageReadFunctions {

  implicit def addCoverageReadFunctions(rdd: RDD[BAMRecord]) = {
    new CoverageReadFunctions(rdd)

  }
}

object NormalizationType extends Enumeration{
  type NormalizationType = Value
  val DESeq2 = Value
}

object CountNormalization {

  def estimateNormFactors(covRDD: RDD[CoverageRecord], normType: NormalizationType) = {

    normType match {
      case NormalizationType.DESeq2 => Some(deseq2Factor(covRDD) )
      case _ => None

    }

  }

  def normalize(covRdd:RDD[CoverageRecord], normType: NormalizationType, numTasks:Option[Int]= None) = {
    val factors = covRdd
      .sparkContext
      .broadcast{
        val factorsMap = new mutable.HashMap[String,Double]()
        val rdd = normType match {
          case NormalizationType.DESeq2 => deseq2Factor(covRdd, numTasks)
          case _ => deseq2Factor(covRdd, numTasks) //FIXME:Change to None and add pattern matching
        }
        rdd.foreach{case (sampleId,factor) => factorsMap(sampleId) = factor}
        factorsMap
      }

    covRdd.map{
      case r:CoverageRecord => {
        r.copy(coverage = (r.coverage/factors.value(r.sampleId).toInt ) )
      }
    }

  }

  private def deseq2Factor(covRDD: RDD[CoverageRecord], numTasks:Option[Int] = None) = {

    //val logMean = StatUtils.mean(counts.map(r=>log10(r)))
    val sortedRdd = {
      numTasks match {
        case Some(n) => covRDD.repartition(n)
        case _ => covRDD
      }
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)
      .map { case c: CoverageRecord => ((c.contigName, c.position), Array((c.sampleId, c.coverage))) }
      .reduceByKey((a, b) => a ++ b)
      .sortByKey()
      .mapValues(r => r.sortBy(r => r._1))


    val logMeanRdd = sortedRdd
      .map{case ((chr,position),covArray) =>  ( (chr,position),StatUtils.mean(covArray.map(k => log(k._2) ) ) ) }

    val distinctSamplesSorted = covRDD.map { case c: CoverageRecord => c.sampleId }
      .distinct()
      .collect()
      .sortBy(r => r)

    val samplesRDD = for(s<-distinctSamplesSorted) yield {
      covRDD
        .filter{c:CoverageRecord=>c.sampleId == s}
        .map(r=>((r.contigName,r.position),(r.coverage) ) )
    }

    val factors = for(i <- 0 to samplesRDD.length - 1) yield {
      exp(
        medianFromRDD(
          samplesRDD(i)
            .join(logMeanRdd)
            .mapValues{case (cnt,logMean)=> log(cnt) - logMean }
            .map(r=>r._2)
        )
      )
    }
    val factorsWithSamples = for(i<-0 to factors.length-1) yield {
      (distinctSamplesSorted(i),factors(i))
    }
    factorsWithSamples
  }

  def medianFromRDD(rdd:RDD[Double]) = {

    val sorted = rdd.sortBy(identity).zipWithIndex().map {
      case (v, idx) => (idx, v)
    }

    val count = sorted.count()

    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
    }
    else sorted.lookup(count / 2).head.toDouble

    median
  }

}

class CoverageFunctions(coverageRDD:RDD[CoverageRecord]) extends Serializable {

    val sqlContext = new SQLContext(coverageRDD.sparkContext)

    def normalize(normType:NormalizationType = NormalizationType.DESeq2, numTasks: Option[Int] = None )={
      CountNormalization.normalize(coverageRDD, normType, numTasks)
    }
}

object CoverageFunctions {
  implicit def addCoverageFunctions(rdd: RDD[CoverageRecord]) = new
      CoverageFunctions(rdd)
}