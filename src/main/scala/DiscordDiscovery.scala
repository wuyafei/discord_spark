import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.mllib.rdd.RDDFunctions._
import util.control.Breaks._
/**
 * Created by wuyafei on 15/7/4.
 */
object DiscordDiscovery {
  def main (args: Array[String]): Unit = {
    val data_file = "data/xmitdb_orig.txt"
    val conf = new SparkConf().setAppName("Discord Discovery").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val window = 360
    val range = 1.55
    val ts = sc.textFile(data_file, 4).map(_.toDouble).zipWithIndex().map(x => (x._2, x._1))
    val partition_num = ts.partitions.length

    class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
      def numPartitions = p
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }

    val partitioned = ts.mapPartitionsWithIndex((i, p) => {
      val overlap = p.take(window - 1).toArray     //be careful with iterator
      val spill = overlap.iterator.map((i - 1, _))
      val keep = (overlap.iterator ++ p).map((i, _))
      if (i == 0) keep else keep ++ spill
    }).partitionBy(new StraightPartitioner(partition_num)).values.cache()

    def distance(d1:Array[Double],d2:Array[Double]):Double = {
      val pair = d1.zip(d2)
      val sum = pair.map(x => math.pow(x._1 - x._2, 2)).sum
      math.sqrt(sum)
    }

    val candidates = partitioned.mapPartitionsWithIndex((idx,par) => {
      val pair_data = par.toArray
      val data = pair_data.map(x => x._2)
      val index = pair_data.map(x => x._1)
      val len = data.length
      var cndd = Array((-1,-1L,0.0,Array(0.0)))     //(partition_idx, record_idx, nn_dist, time_series)
      for(i <- 0 to (len - window)){
        breakable {
          var nn_dist = 9999999999.9
          for (j <- 0 to (len - window)) {
            if (math.abs(i - j) >= window) {
              val dist = distance(data.slice(i, i + window), data.slice(j, j + window))
              if (dist < range)
                break()
              if(dist < nn_dist)
                nn_dist = dist
            }
          }
          cndd = cndd :+(idx, index(i), nn_dist, data.slice(i, i + window))
        }
      }
      cndd.drop(1).iterator
    }).collect()

    val broadcast_candidates = sc.broadcast(candidates)

    val checked_candidates = partitioned.mapPartitionsWithIndex((idx, par) => {
      val pair_data = par.toArray
      val data = pair_data.map(x => x._2)
      val index = pair_data.map(x => x._1)
      val len = data.length
      var checked_cndd= Array((-1L, 0.0))
      for(seq_candidate <- broadcast_candidates.value){
        breakable{
          if(seq_candidate._1 == idx) break()
          var nn_dist = 99999999.9
          for(j <- 0 to (len-window)){
            if(math.abs(index(j) - seq_candidate._2) >= window){
              val dist = distance(seq_candidate._4, data.slice(j, j+window))
              if(dist < range){
                break()
              }
              if(dist < nn_dist)
                nn_dist = dist
            }
          }
          val new_dist = if(nn_dist < seq_candidate._3) nn_dist else seq_candidate._3
          checked_cndd = checked_cndd :+ (seq_candidate._2, new_dist)
        }
      }
      checked_cndd.drop(1).iterator
    }).groupByKey().filter(_._2.size == partition_num - 1).map(x => (x._1, x._2.min)).sortByKey().collect()

    if(checked_candidates.length == 0){
      println("No discord found!")
      return
    }
    var discords = Array(checked_candidates(0))
    for(i <- 1 until  checked_candidates.length){
      if(checked_candidates(i)._1 - discords.last._1 >= window)
        discords = discords :+ checked_candidates(i)
      else if(checked_candidates(i)._2 > discords.last._2)
        discords = discords.dropRight(1) :+ checked_candidates(i)
    }
    discords.foreach(x => println("%d, %f".format(x._1, x._2)))
  }

}
