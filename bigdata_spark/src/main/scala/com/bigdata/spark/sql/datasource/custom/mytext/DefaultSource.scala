package com.bigdata.spark.sql.datasource.custom.mytext

import com.bigdata.spark.sql.datasource.custom.RuozedataTextDataSourceRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

//DefaultSource名字固定的
class DefaultSource extends RelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new BigdataTextDataSourceRelation(sqlContext,p)
      case _ => throw new IllegalArgumentException("path is required!")
    }
  }
}
