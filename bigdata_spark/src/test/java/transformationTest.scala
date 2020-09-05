//import org.apache.spark.{SparkConf, SparkContext}
//import org.junit.{Before, Test}
//
//class transformationTest {
//  val conf = new SparkConf().setAppName("demo").setMaster("local")
//  val sc = new SparkContext(conf)
//
////  @Before
////  def setup(): Unit ={
////
////
////  }
//
//  /**
//   * * @param zeroValue the initial value for the accumulated result of each partition for the
//   * *                  `seqOp` operator, and also the initial value for the combine results from
//   * *                  different partitions for the `combOp` operator - this will typically be the
//   * *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
//   * * @param seqOp an operator used to accumulate results within a partition
//   * * @param combOp an associative operator used to combine results from different partitions
//   **/
//  @Test
//  def testAggregate(): Unit ={
//    val i = sc.makeRDD(List(1, 2, 3, 4),2)
////      .aggregate(0)(_ + _, _ + _)
//      .aggregate(1)(
//        (x,y)=>{
//          println(s"x=$x + y=$y")
//          (x + y)
//
//      }, _ * _)
//
//    println(i)
//
//  }
//
//  /**
//   * * @param zeroValue the initial value for the accumulated result of each partition for the
//   * *                  `seqOp` operator, and also the initial value for the combine results from
//   * *                  different partitions for the `combOp` operator - this will typically be the
//   * *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
//   * * @param seqOp an operator used to accumulate results within a partition
//   * * @param combOp an associative operator used to combine results from different partitions
//   *
//   *
//   * combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),cleanedSeqOp, combOp, partitioner)
//   *
//   **/
//    @Test
//  def testAggregateByKey(): Unit ={
//    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
//    rdd.aggregateByKey(0)((x,y)=>math.max(x,y),_ + _)
//      .foreach(println)
//  }
//
//
//
//
//  /**
//   * Merge the values for each key using an associative function and a neutral "zero value" which
//   * may be added to the result an arbitrary number of times, and must not change the result
//   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
//   *
//   *
//   * combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),cleanedFunc, cleanedFunc, partitioner)
//   *    分区内和分区外计算规则一样的aggregateByKey
//   */
//  @Test
//  def testFoldByKey(): Unit ={
//    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",3)))
//    dataRDD1.foldByKey(0)(_+_)
//      .foreach(println(_))
//
//
//
//  }
//
//  /**
//   * createCombiner: V => C,
//   * mergeValue: (C, V) => C,
//   * mergeCombiners: (C, C) => C
//   */
//
//  @Test
//  def testCombineByKey(): Unit ={
//    val list = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
//    val input = sc.makeRDD(list, 2)
//
//    input.combineByKey(
//      (_, 1),
//      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
//      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
//    ).foreach(println(_))
//  }
//
//  /**
//   * * Users provide three functions:
//   * *
//   * *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
//   * *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
//   * *  - `mergeCombiners`, to combine two C's into a single one.
//   */
////  @Test
////  def testCombineByKeyWithClassTag(): Unit ={
////    val list = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
////    val input = sc.makeRDD(list, 2)
////
////    input.combineByKeyWithClassTag[Tuple2[Int,Int]](
////      (_, 1),
////      (acc, v) => (acc._1 + v, acc._2 + 1),
////      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
////    ).foreach(println(_))
////  }
////
//
//
//
//
//
//}
