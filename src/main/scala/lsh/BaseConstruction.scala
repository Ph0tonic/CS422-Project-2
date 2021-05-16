package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //build buckets here
  val buckets = new MinHash(seed).execute(data).map{ case (a,b) => (b,a) }.groupByKey().cache()
  buckets.count()

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    //compute near neighbors here
    val minQueries = new MinHash(seed)
      .execute(queries)
      .map{ case (a,b) => (b,a) }

    minQueries.leftOuterJoin(buckets)
      .map{ case (_,(query, movies)) => (query, movies.getOrElse(Set.empty).toSet)}
  }
}
