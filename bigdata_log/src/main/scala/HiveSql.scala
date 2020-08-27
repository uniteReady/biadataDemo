import scala.io.Source

object HiveSql {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\code\\bigdata\\hadoop_offline\\out\\part-r-00000")
    source
      .getLines()
      .toArray
      .map(x => {
        val splits = x.split(",")
        val province = splits(14)
        val responsesize = splits(9).toInt

        (province,responsesize)

      })
      .groupBy(_._1)
      .mapValues(x => (x.map(_._2).sum,x.length))
      .foreach(println(_))




  }

}
