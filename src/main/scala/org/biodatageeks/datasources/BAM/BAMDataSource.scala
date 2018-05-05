package org.biodatageeks.datasources.BAM

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}


class BAMDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "BAM"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    if(parameters.exists(_._1 == "directory") )
      new BAMRelation(parameters("directory"))(sqlContext)
    else
      new BAMRelation(parameters("path"))(sqlContext)
  }
}