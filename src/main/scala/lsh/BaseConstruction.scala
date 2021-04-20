package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed).execute(data).map{ case (a,b) => (b,a) }.groupByKey().cache()

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    val queries = new MinHash(seed)
      .execute(rdd)
      .map{ case (a,b) => (b,a) }

    //TODO: minhash + shuffling
    minHash.rightOuterJoin(queries)
      .map{ case (key,(movies, query)) => (query, movies.getOrElse(Set.empty).toSet)}
      .filter(_._2.nonEmpty)
  }
}
