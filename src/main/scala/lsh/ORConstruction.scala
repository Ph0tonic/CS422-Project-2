package lsh

import org.apache.spark.rdd.RDD

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute OR construction results here
    children.map(_.eval(rdd)).reduce(_ union _).reduceByKey(_ union _)
  }
}
