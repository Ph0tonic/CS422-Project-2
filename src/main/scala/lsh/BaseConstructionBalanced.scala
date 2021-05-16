package lsh

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed: Int, nbPartitions: Int) extends Construction {
  //build buckets here
  val buckets: RDD[(Int, String)] = new MinHash(seed).execute(data).map { case (a, b) => (b, a) }.cache()
  val countByKey: Broadcast[collection.Map[Int, Long]] = sqlContext.sparkContext.broadcast(buckets.countByKey())

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here
    val minQueries = new MinHash(seed)
      .execute(queries)

    val parts: Array[Int] = computePartitions(computeMinHashHistogram(minQueries))

    val partitioner = new Partitioner{
      override def numPartitions: Int = 8
      override def getPartition(key: Any): Int = key match {
        case id:Int => parts.foldLeft(0)(
          (acc, r) => if (r < id) acc + 1 else acc
        )
      }
    }
    val partitionedBucket = buckets.partitionBy(partitioner)

    minQueries
      .map { case (a, b) => (b, a) }
      .partitionBy(partitioner)
      .leftOuterJoin(partitionedBucket)
      .mapValues {
        case (query, movies) => (query, movies.toSet)
        case (query, None) => (query, Set.empty[String])
      }.values
  }

  def computeMinHashHistogram(queries: RDD[(String, Int)]): Array[(Int, Int)] = {
    //compute histogram for target buckets of queries
    val counts = countByKey.value
    queries
      .map { case (_, b) => (b, counts.getOrElse(b, 0L).toInt) }
      .distinct()
      .sortByKey()
      .collect()
  }

  def computePartitions(histogram: Array[(Int, Int)]): Array[Int] = {
    //compute the boundaries of bucket partitions
    var count = 0
    val total = histogram.foldLeft(0)(_ + _._2)
    val maxNb = Math.ceil(total / nbPartitions)
    val data = new mutable.ArrayBuilder.ofInt
    for ((index, nb) <- histogram) {
      count += nb
      if (count > maxNb) {
        data += index
        count = 0
      }
    }
    data.result()
  }
}