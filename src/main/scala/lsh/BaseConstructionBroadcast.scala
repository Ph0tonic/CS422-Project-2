package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed: Int) extends Construction with Serializable {
  //build buckets here
  val buckets = new MinHash(seed).execute(data).map { case (a, b) => (b, a) }.groupByKey().collectAsMap()
  val sparkBuckets = sqlContext.sparkContext.broadcast(buckets)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    new MinHash(seed)
        .execute(queries)
        .mapValues(sparkBuckets.value.getOrElse(_, Set.empty).toSet)
  }
}
