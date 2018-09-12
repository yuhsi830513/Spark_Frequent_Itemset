import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ListBuffer, Set => mutSet}
import scala.math.Ordering.Implicits._
import scala.util.Sorting
import java.io.{File, PrintWriter}

object SON1{
  def main(args: Array[String]): Unit = {
    val support = args(2).toFloat
    val path = args(1)

    val conf = new SparkConf().setAppName("frequentItemSets").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(path)
        .mapPartitionsWithIndex(removeFirst)
    val baskets = 
    if (args(0) == "1"){ //case1: (reviewer: (productID1, pID2, .....))
      lines.map(_.split(","))
        .map(s => (s(0), s(1)))
        .distinct().groupByKey()
    }
    else{      //case2: (product: (reviewer1, reviewer2, .....))
      lines.map(_.split(","))
        .map(s => (s(1), s(0)))
        .distinct().groupByKey()
    }
    
    val totalBaskets = baskets.count().toFloat

    //1st Stage: find candidate frequent set
    val firstStage = baskets.mapPartitions(itemLists => {
      var someBaskets = ListBuffer.empty[(List[String])]
      itemLists.toList.map(b => someBaskets += b._2.toList)
      println("\n\nFirst stage")

      val p = support/ totalBaskets* someBaskets.size
      val singles = someBaskets.flatten.groupBy(identity)
                        .map(i => (List(i._1), i._2.size))
                        .filter(i => i._2 >= p)//singles: Map[List[String], Int]

      var result = ListBuffer.empty[(List[String], Int)] 
      singles.map(s =>{result.append((s._1, 1))})
      
      var candList = singles.keys.toList  
      var itr = 2
      var sizeOfC = 1
      do {
        println("itr = " + itr) 
        var combList = combination(candList, itr)//List[List[String]]
        var validSingles = combList.flatten.toSet
        println("Number of combinations: "+ combList.size.toString)
        /*Version 1*/
        /*var candidates = someBaskets.map(basket =>
          for {comb <- combList if comb.forall(basket.contains)}
            yield(comb)
          //we want a list of valid combination List[List[String]]
          //aggregate(start:A) (seqOp, comOp) 
          ).flatMap(x=>x).groupBy(identity)
          .mapValues(i=>i.size)
          .filter(comb => comb._2 >= p).toMap
        */
        /*version 2
          var counter = itemLists.map(basket =>{
          var smallBasket = basket.toSet.intersect(validSingles)
          stcombList.filter(comb => comb.forall(smallBasket.contains))
        }).flatten.groupBy(identity)
          .mapValues(i => i.size)
        */
        /*version 3*/
        var candidates = combList.par.map(comb => {
          var count = 0
          someBaskets.map(basket => {
            var smallBasket = basket.toSet.intersect(validSingles)
            if(comb.forall(smallBasket.contains))
              count += 1
          })
          (comb, count)
        }).filter(comb => comb._2 >= p).toMap
        println("counter ended")
        candidates.foreach(c => {result.append(c)})
        sizeOfC = candidates.size
        candList = candidates.keys.toList
        itr += 1
      }while(sizeOfC != 0)
      result.toIterator
    }).reduceByKey((a, b) => 1).collect()
    
    //2nd Stage: filter out real frequent item set
    val secondStage = baskets.mapPartitions(someBaskets => {
      var itemLists = ListBuffer.empty[(List[String])]
      someBaskets.toList.map(b => itemLists += b._2.toList)
      checkSupport(itemLists, firstStage).toIterator
    }).reduceByKey((a, b) => a + b)
      .filter(item => item._2.toFloat >= support)
      .map(item => item._1)
      .sortBy(item => (item.size, item))
      .collect()

    val outFile = new File("OutputFiles/Yuhsi_Chou_SON_" + 
                  path.substring(path.lastIndexOf('/')+1, path.lastIndexOf('.')+1) + 
                  "case" + args(0) + "-" + support.toInt.toString + ".txt")
    if(!outFile.getParentFile().exists()) outFile.getParentFile.mkdirs()
    val writer = new PrintWriter(outFile)
    var size = 0
    for(items <- secondStage){
      if(items.size != size){
        size += 1
        writer.write("\n\n" + items.mkString("('", "', '", "')"))
      }
      else writer.write(", " + items.mkString("('", "', '", "')"))
    }
    writer.close()
    sc.stop()
  }
	

  def removeFirst(index: Int, iter: Iterator[String]): Iterator[String] = {
    if (index == 0 && iter.hasNext) {
      iter.next
      iter
    } else iter
  } 



  def combination(candidates: List[List[String]], itr: Int): List[List[String]] = {
    val singles = candidates.flatten.distinct
    var newCombinations = mutSet.empty[List[String]]
    candidates.map(comb => {
      singles.foreach( s => {
        var temp = ListBuffer(comb : _*)
        if(!comb.contains(s)){ 
          temp += s
        }
        if(temp.size == itr)
          newCombinations += temp.toList.sorted
      })
    })
    return newCombinations.toList
  }

  def checkSupport(itemLists: ListBuffer[List[String]], cand:Array[(List[String],Int)]): Array[(List[String],Int)] = {
    var finalResult= cand.map(c=>{
      var count = 0
      itemLists.foreach(basket => { if(c._1.forall(basket.contains)) count += 1 })
      (c._1,count)
    })
    finalResult
  }
}

