package org.apache.spark.sql


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.Locale

import org.apache.spark.sql.ResolveTableValuedFunctionsSeq.tvf
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, TypeCoercion, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, _}


/**
  * Rule that resolves table-valued function references.
  */
object ResolveTableValuedFunctionsSeq extends Rule[LogicalPlan] {
  /**
    * List of argument names and their types, used to declare a function.
    */
  private case class ArgumentList(args: (String, DataType)*) {
    /**
      * Try to cast the expressions to satisfy the expected types of this argument list. If there
      * are any types that cannot be casted, then None is returned.
      */
    def implicitCast(values: Seq[Expression]): Option[Seq[Expression]] = {
      if (args.length == values.length) {
        val casted = values.zip(args).map { case (value, (_, expectedType)) =>
          TypeCoercion.ImplicitTypeCasts.implicitCast(value, expectedType)
        }
        if (casted.forall(_.isDefined)) {
          return Some(casted.map(_.get))
        }
      }
      None
    }

    override def toString: String = {
      args.map { a =>
        s"${a._1}: ${a._2.typeName}"
      }.mkString(", ")
    }
  }

  /**
    * A TVF maps argument lists to resolver functions that accept those arguments. Using a map
    * here allows for function overloading.
    */
  private type TVF = Map[ArgumentList, Seq[Any] => LogicalPlan]

  /**
    * TVF builder.
    */
  private def tvf(args: (String, DataType)*)(pf: PartialFunction[Seq[Any], LogicalPlan])
  : (ArgumentList, Seq[Any] => LogicalPlan) = {
    (ArgumentList(args: _*),
      pf orElse {
        case args =>
          throw new IllegalArgumentException(
            "Invalid arguments for resolved function: " + args.mkString(", "))
      })
  }

  /**
    * Internal registry of table-valued functions.
    */
  private val builtinFunctions: Map[String, TVF] = Map(
    "coverage" -> Map(
      /* coverage(tableName) */
      tvf("table" -> StringType) { case Seq(table: Any) =>
        Coverage(table.toString)
      }),
    "coverage_norm" -> Map(
      /* coverage(tableName) */
      tvf("table" -> StringType) { case Seq(table: Any) =>
        CoverageNorm(table.toString)
      }),
    "coverage_hist" -> Map(
      /* coverage_hist(tableName) */
      tvf("table" -> StringType) { case Seq(table: Any) =>
        CoverageHist(table.toString)
      },

      /* coverage_hist(tableName,step) */
        tvf("table" -> StringType, "step" -> IntegerType) { case Seq(table: Any,step: Integer) =>
        CoverageHistStep(table.toString,step)
      },
      /* coverage_hist(tableName,buckets) */
      tvf("table" -> StringType, "buckets" -> ArrayType(DoubleType)) { case Seq(table: Any,buckets:Array[Double]) =>
        CoverageHistBuckets(table.toString,buckets.asInstanceOf[Array[Integer]])
      }

    ),
    "range" -> Map(
      /* range(end) */
      tvf("end" -> LongType) { case Seq(end: Long) =>
        Range(0, end, 1, None)
      })
  )

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case u: UnresolvedTableValuedFunction if u.functionArgs.forall(_.resolved) =>
      val resolvedFunc = builtinFunctions.get(u.functionName.toLowerCase(Locale.ROOT)) match {
        case Some(tvf) =>
          val resolved = tvf.flatMap { case (argList, resolver) =>
            argList.implicitCast(u.functionArgs) match {
              case Some(casted) =>
                Some(resolver(casted.map(_.eval())))
              case _ =>
                None
            }
          }
          resolved.headOption.getOrElse {
            val argTypes = u.functionArgs.map(_.dataType.typeName).mkString(", ")
            u.failAnalysis(
              s"""error: table-valued function ${u.functionName} with alternatives:
                 |${tvf.keys.map(_.toString).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
                 |cannot be applied to: (${argTypes})""".stripMargin)
          }
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table-valued function")
      }

      // If alias names assigned, add `Project` with the aliases
      if (u.output.nonEmpty) {
        val outputAttrs = resolvedFunc.output
        // Checks if the number of the aliases is equal to expected one
        if (u.output.size != outputAttrs.size) {
          u.failAnalysis(s"Number of given aliases does not match number of output columns. " +
            s"Function name: ${u.functionName}; number of aliases: " +
            s"${u.output.size}; number of output columns: ${outputAttrs.size}.")
        }
        val aliases = outputAttrs.zip(u.output).map {
          case (attr, name) => Alias(attr, name.toString())()
        }
        Project(aliases, resolvedFunc)
      } else {
        resolvedFunc
      }
  }
}

/** Factory for constructing new `Coverage` nodes. */
object Coverage {
  def apply(tableName:String): Coverage = {
    val output = StructType(Seq(
      StructField("sampleId", StringType, nullable = false),
      StructField("contigName",StringType,nullable = true),
      StructField("position",IntegerType,nullable = false),
      StructField("coverage",IntegerType,nullable = false)
      )
      ).toAttributes
    new Coverage(tableName:String,output)
  }

}

case class Coverage(tableName:String,
                    output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {


  val numElements: BigInt = 1

  def toSQL(): String = {

    s"SELECT sampleId,contigName,position,coverage AS `${output.head.name}` FROM coverage('$tableName')"
  }

  override def newInstance(): Coverage = copy(output = output.map(_.newInstance()))

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = LongType.defaultSize * numElements
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"Coverage ('$tableName')"
  }
}

/** Factory for constructing new `CoverageNorm` nodes. */
object CoverageNorm {
  def apply(tableName:String): CoverageNorm = {
    val output = StructType(Seq(
      StructField("sampleId", StringType, nullable = false),
      StructField("contigName",StringType,nullable = true),
      StructField("position",IntegerType,nullable = false),
      StructField("coverage",IntegerType,nullable = false)
    )
    ).toAttributes
    new CoverageNorm(tableName:String,output)
  }

}

case class CoverageNorm(tableName:String,
                    output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {


  val numElements: BigInt = 1

  def toSQL(): String = {

    s"SELECT sampleId,contigName,position,coverage AS `${output.head.name}` FROM coverage_norm('$tableName')"
  }

  override def newInstance(): CoverageNorm = copy(output = output.map(_.newInstance()))

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = LongType.defaultSize * numElements
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"CoverageNorm ('$tableName')"
  }
}

/*coverage_hist*/
object CoverageHist {
  def apply(tableName:String): CoverageHist = {
    val output = StructType(Seq(
      StructField("sampleId", StringType, nullable = false),
      StructField("contigName",StringType,nullable = true),
      StructField("position",IntegerType,nullable = false),
      //StructField("coverage",StringType,nullable = false),
      StructField("coverage",ArrayType(IntegerType,false),nullable = false),
      StructField("coverageTotal",IntegerType,nullable = false)
    )
    ).toAttributes
    new CoverageHist(tableName:String,output)
  }

}

case class CoverageHist(tableName:String,
                    output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {


  val numElements: BigInt = 1

  def toSQL(): String = {

    s"SELECT sampleId,contigName,position,coverage AS `${output.head.name}` FROM coverage_hist('$tableName')"
  }

  override def newInstance(): CoverageHist = copy(output = output.map(_.newInstance()))

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = LongType.defaultSize * numElements
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"Coverage_hist ('$tableName')"
  }
}

/*coverage_hist*/
object CoverageHistStep {
  def apply(tableName:String,step:Integer): CoverageHistStep = {
    val output = StructType(Seq(
      StructField("sampleId", StringType, nullable = false),
      StructField("contigName",StringType,nullable = true),
      StructField("position",IntegerType,nullable = false),
      //StructField("coverage",StringType,nullable = false),
      StructField("coverage",ArrayType(IntegerType,false),nullable = false),
      StructField("coverageTotal",IntegerType,nullable = false)
    )
    ).toAttributes
    new CoverageHistStep(tableName:String,step:Integer,output)
  }

}

case class CoverageHistStep(tableName:String,
                            step:Integer,
                        output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {


  val numElements: BigInt = 1

  def toSQL(): String = {

    s"SELECT sampleId,contigName,position,coverage AS `${output.head.name}` FROM coverage_hist('$tableName',$step)"
  }

  override def newInstance(): CoverageHistStep = copy(output = output.map(_.newInstance()))

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = LongType.defaultSize * numElements
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"Coverage_hist ('$tableName',$step)"
  }
}

/*coverage_hist*/
object CoverageHistBuckets {
  def apply(tableName:String,buckets:Array[Integer]): CoverageHistBuckets = {
    val output = StructType(Seq(
      StructField("sampleId", StringType, nullable = false),
      StructField("contigName",StringType,nullable = true),
      StructField("position",IntegerType,nullable = false),
      //StructField("coverage",StringType,nullable = false),
      StructField("coverage",ArrayType(IntegerType,false),nullable = false),
      StructField("coverageTotal",IntegerType,nullable = false)
    )
    ).toAttributes
    new CoverageHistBuckets(tableName:String,buckets:Array[Integer],output)
  }

}

case class CoverageHistBuckets(tableName:String,
                               buckets:Array[Integer],
                            output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {


  val numElements: BigInt = 1

  def toSQL(): String = {

    s"SELECT sampleId,contigName,position,coverage AS `${output.head.name}` FROM coverage_hist('$tableName',$buckets)"
  }

  override def newInstance(): CoverageHistBuckets = copy(output = output.map(_.newInstance()))

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = LongType.defaultSize * numElements
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"Coverage_hist ('$tableName',$buckets)"
  }
}

