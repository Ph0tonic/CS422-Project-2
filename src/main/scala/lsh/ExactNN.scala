package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {

  // data = movies
  val movies: RDD[(String, Set[String])] = data.mapValues(_.toSet).cache()

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // compute exact near neighbors here
    queries
      .mapValues(_.toSet)
      .cartesian(movies)
      .map { case ((key, searchKeyWords), (movie, keyWords)) =>
        (key, (movie, searchKeyWords.intersect(keyWords).size.doubleValue() / searchKeyWords.union(keyWords).size.doubleValue()))
      }
      .filter(_._2._2 > threshold)
      .aggregateByKey(Set.empty[String])(_ + _._1, _ union _)
  }
}
