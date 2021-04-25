package lsh

import org.apache.spark.rdd.RDD

class ANDConstruction(children: List[Construction]) extends Construction {
  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    children
      .map(_.eval(queries.zipWithIndex().map{ case ((key, value), index) => (key+"|"+index,value) }))
      .reduce(_ union _)
      .reduceByKey(_ intersect _)
      .map { case (key, value) => (key.split("\\|").head, value)}
  }
}
