package org.biodatageeks.preprocessing.coverage

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.datasources.BAM.BAMRecord
import org.biodatageeks.preprocessing.coverage.CoverageReadFunctions._

import scala.collection.mutable


class CoverageStrategy(spark: SparkSession) extends Strategy with Serializable  {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case Coverage(tableName,output) => CoveragePlan(plan,spark,tableName,output) :: Nil
    case CoverageHist(tableName,output) => CoverageHistPlan(plan,spark,tableName,output) :: Nil
    case _ => Nil
  }

}

case class CoveragePlan(plan: LogicalPlan, spark: SparkSession, table:String, output: Seq[Attribute]) extends SparkPlan with Serializable {
  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    import spark.implicits._
    val ds = spark.sql(s"select * FROM ${table}")
      .as[BAMRecord]
      .filter(r=>r.contigName != null)
    val schema = plan.schema
    val cov = ds.rdd.baseCoverage(None,Some(4),sorted=false)
      cov
      .mapPartitions(p=>{
       val proj =  UnsafeProjection.create(schema)
       p.map(r=>   proj.apply(InternalRow.fromSeq(Seq(UTF8String.fromString(r.sampleId),
          UTF8String.fromString(r.contigName),r.position,r.coverage))))
      })
  }


  def children: Seq[SparkPlan] = Nil
}

case class CoverageHistPlan(plan: LogicalPlan, spark: SparkSession, table:String, output: Seq[Attribute]) extends SparkPlan with Serializable {

  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    import spark.implicits._
    val ds = spark.sql(s"select * FROM ${table}")
      .as[BAMRecord]
      .filter(r=>r.contigName != null)
    val schema = plan.schema
    val params = CoverageHistParam(CoverageHistType.MAPQ,Array(0,1,2,3,50))
    val cov = ds.rdd.baseCoverageHist(Some(0),None,params)
//    val emptyIntArray =
//      ExpressionEncoder[Array[Int]]().resolveAndBind().toRow(Array.emptyIntArray).getArray(0)
    cov
      .mapPartitions(p=>{
        val proj =  UnsafeProjection.create(schema)
        val exprEnc =  ExpressionEncoder[Array[Int]]().resolveAndBind()
        p.map(r=>   proj.apply(InternalRow.fromSeq(
          Seq(
            UTF8String.fromString(r.sampleId),
            UTF8String.fromString(r.contigName),
            r.position,
           exprEnc.toRow(r.coverage).getArray(0),
            //UTF8String.fromString(r.coverage.mkString(",") ),
            r.coverageTotal) ) ) )
      })
    //spark.sparkContext.emptyRDD[InternalRow]
  }
  def children: Seq[SparkPlan] = Nil
}