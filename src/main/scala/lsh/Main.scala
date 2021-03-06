package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.lang.System

object Main {
  def generate(sc : SparkContext, input_file : String, output_file : String, fraction : Double) : Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(withReplacement = false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_recall = recall_vec._1/recall_vec._2

    avg_recall
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_precision = precision_vec._1/precision_vec._2

    avg_precision
  }

  def construction1(sqlContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction1 composition here
    // Target precision > 0.94

    new ANDConstruction(List(
      new BaseConstruction(sqlContext, rdd_corpus, 120),
      new BaseConstruction(sqlContext, rdd_corpus, 121),
      new BaseConstruction(sqlContext, rdd_corpus, 122),
      new BaseConstruction(sqlContext, rdd_corpus, 123),
      new BaseConstruction(sqlContext, rdd_corpus, 124)
    ))
  }

  def construction2(sqlContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction2 composition here
    // Target recall > 0.94

    new ORConstruction(List(
      new BaseConstruction(sqlContext, rdd_corpus, 120),
      new BaseConstruction(sqlContext, rdd_corpus, 121),
      new BaseConstruction(sqlContext, rdd_corpus, 122),
      new BaseConstruction(sqlContext, rdd_corpus, 123),
      new BaseConstruction(sqlContext, rdd_corpus, 124)
    ))
  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Projects\\CS422-Project-2-bwermeil\\")

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //type your queries here
    val root = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/"
    val corpus_file = new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath
//    val corpus_file = root + "/corpus-10.csv/part-00000"

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.length).toList))

    val query_file = new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath
//    val query_file = root + "/queries-10-2.csv/part-00000"

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.length).toList))
      .sample(withReplacement = false, 0.05)

//    val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
//    val lsh =  new BaseConstruction(sqlContext, rdd_corpus, 42)
//
//    val ground = exact.eval(rdd_query)
//    val res = lsh.eval(rdd_query)

    // ExactNN

    // data = movies
    val movies: RDD[(String, Set[String])] = rdd_corpus.mapValues(_.toSet).cache()

//    val threshold = 0.55
    // compute exact near neighbors here
    val temp = rdd_query
      .mapValues(_.toSet)
      .cartesian(movies)
      .map { case ((key, searchKeyWords), (movie, keyWords)) =>
        searchKeyWords.intersect(keyWords).size.doubleValue() / searchKeyWords.union(keyWords).size.doubleValue()
      }
      .filter(!_.isInfinity)
      .cache()

    for(i <- 0 to 100 by 5) {
      val threshold = i.doubleValue()
      val temp2 = temp.filter(dist => dist >= threshold/100).cache()
      val dist = temp2.sum() / temp2.count()
      println("Distance calculated ", i , " : ", dist)
    }

//    assert(Main.recall(ground, res) >= 0.8)
//    assert(Main.precision(ground, res) >= 0.9)
//
//    assert(res.count() == rdd_query.count())

    println("SUCCESSFUL RUN")
  }     
}
