/**

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


class MyAuthorCollaborationReducer extends Reducer[Text,IntWritable,Text,Text] {

  val Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {

    val value : IntWritable = new IntWritable(0)
    for (value <- values){

    }
    val coAuthorCountList = values.asScala.toList
    //val authors = (preprocessedXML \\ "author").map(author => author.text.toLowerCase.trim).toList.sorted
    val coAuthorCount = new ListBuffer[Int]
    //values.asScalaforx => coAuthorCount.add(x.get()))

    while (coAuthorCountList.)

    val sum = values.asScala.foldLeft(0)(_ + _.get)
    /*var coAuthorList = List()
    coAuthorCount.foreach(coAuthorList += _.get )*/
    val author = new Text(key)
    if (coAuthorCount.length!=0) {

      val maxCollaboration = coAuthorCount.tail.toString()
      val meanCollaboration: Double = sum / coAuthorCount.length
      var medianCollaboration : Int = 0
      val numOfCollaborators = coAuthorCount.length
      // calculating median:
      // if size is odd: median <- n/2
      if (numOfCollaborators == 1) {
        medianCollaboration = coAuthorCount.head.get()
      }
      else if (numOfCollaborators % 2 != 0) {
        medianCollaboration = coAuthorCount(coAuthorCount.length/2).get()
      }
      else if (coAuthorCount.size % 2 == 0) {
        //medianCollaboration = (coAuthorCount(numOfCollaborators/2 - 1) + coAuthorCount(numOfCollaborators/2))/2

        val x = coAuthorCount(numOfCollaborators/2).get()
        val y = coAuthorCount(numOfCollaborators/2 -1).get()
        medianCollaboration = (x+y)/2

      }

      context.write(author, new Text(maxCollaboration + "," + meanCollaboration + "," + medianCollaboration))
    }

    else {
      context.write(author, new Text( 0 + "," + 0 + "," + 0))
    }
  }
}
*/