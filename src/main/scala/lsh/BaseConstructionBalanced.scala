package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed: Int, partitions: Int) extends Construction {
  //build buckets here
  val buckets = new MinHash(seed).execute(data).map { case (a, b) => (b, a) }.groupByKey().cache()

  def computeMinHashHistogram(queries: RDD[(String, Int)]): Array[(Int, Int)] = {
    //compute histogram for target buckets of queries
    val counts = buckets.countByKey()
    queries
      .map { case (_, b) => (b, counts.getOrElse(b, 0L).toInt) }
      .distinct()
      .sortByKey()
      .collect()
  }

  def computePartitions(histogram: Array[(Int, Int)]): Array[Int] = {
    //compute the boundaries of bucket partitions
    var count = 0
    val total = histogram.foldLeft(0)(_+_._2)
    val maxNb = Math.ceil(total/partitions)
    val data = new mutable.ArrayBuilder.ofInt
    data += 0

    for ((index, nb) <- histogram) {
      count += nb
      if(count > maxNb) {
        data += index
        count = 0
      }
    }
    data.result()
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here
    val minQueries = new MinHash(seed)
      .execute(queries)
      .map{ case (a,b) => (b,a) }

    //TODO: shuffling
    buckets.rightOuterJoin(minQueries)
      .map{ case (key,(movies, query)) => (query, movies.getOrElse(Set.empty).toSet)}
      .filter(_._2.nonEmpty)
  }
}