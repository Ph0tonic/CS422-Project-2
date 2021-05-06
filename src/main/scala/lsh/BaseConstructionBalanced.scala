package lsh

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed: Int, nbPartitions: Int) extends Construction {
  //build buckets here
  val buckets = new MinHash(seed).execute(data).map { case (a, b) => (b, a) }.groupByKey().cache()

  buckets.count()

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here
    val minQueries = new MinHash(seed)
      .execute(queries)
      .cache()

    val parts: Array[Int] = computePartitions(computeMinHashHistogram(minQueries))

    val partitionnedBucket = buckets.map {
      case (id, movie) => (parts.foldLeft(0)(
        (acc, r) => if (r < id) acc + 1 else acc
      ), (id, movie))
    }

    minQueries
      .map {
        case (query, id) => (parts.foldLeft(0)(
          (acc, r) => if (r < id) acc + 1 else acc
        ), (id, query))
      }
      .partitionBy(new HashPartitioner(nbPartitions))
      .leftOuterJoin(partitionnedBucket)
      .filter {
        case (bucketId, ((id1, query), Some((id2, movies)))) => id1 == id2
        case (bucketId, ((id1, query), None)) => true
      }
      .mapValues{
        case ((id, query), Some((_, movies))) => (query, movies.toSet)
        case ((id, query), None) => (query, Set.empty[String])
      }.values
  }

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