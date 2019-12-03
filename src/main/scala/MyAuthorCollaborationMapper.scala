import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML
// Count Number of co authors and pass it to reducer as list iterable
class MyAuthorCollaborationMapper extends Mapper[LongWritable, Text, Text, IntWritable]{
  val logger = LoggerFactory.getLogger(this.getClass.getName)
  val conf = ConfigFactory.load()

  // creating int iterable object with value 1 for context.write
  // val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val fileDtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val inputXml =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
      <!DOCTYPE dblp SYSTEM "$fileDtd">
      <dblp>""" + value.toString + "</dblp>"

    val preprocessedXML = xml.XML.loadString(inputXml)
    val authors = (preprocessedXML \\ "author").map(author => author.text.toLowerCase.trim).toList.sorted

    for (eachAuthor <- authors){
      context.write(new Text(eachAuthor), new IntWritable(authors.size))
    }
  }
}
