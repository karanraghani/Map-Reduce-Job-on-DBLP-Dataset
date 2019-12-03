import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class MyStatReducer extends Reducer[Text, IntWritable, Text, Text] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    // since the values can be iterated through only once !!
    var myList = new ArrayBuffer[Int]()
    for (eachElement <- values.asScala){
      myList += eachElement.get()
    }

    logger.info("---------------On Converting value to Scala iterator String o/p obatained -------", myList)
    //logger.info("===========On creating new list values in the list ======", splitList)

    myList = myList.sorted    // creating the sorted list that will help in getting the median
    val maxCollaborators = myList.max
    val mean:Double = myList.sum/myList.length
    var median: Double = 0.0
    val myListLen = myList.length
    if (myListLen == 1){
      median = myList(0)
    }else if (myListLen %2 == 0){
      median = (myList(myListLen/2 -1) + myList(myListLen/2))/2.0
    }else if(myListLen%2 != 0){
      median = myList(myListLen/2)
    }

    //val median: Double = (myList(myList.length/2 -1) + myList(myList.length/2))/2.0
    context.write(key, new Text(maxCollaborators + "," + mean + "," + median))
  }
}
