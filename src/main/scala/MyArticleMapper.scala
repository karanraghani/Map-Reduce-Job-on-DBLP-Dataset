import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
 * Mapper to parse xml for different years of  publication
 * Then for each Year will be passed with value iterable 1 to the reducer
 */


class MyArticleMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    val logger = LoggerFactory.getLogger(this.getClass.getName)
    val conf = ConfigFactory.load()
  logger.info("Mapper for Number of articles in an year")

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      val fileDtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
      val inputXml = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
      <!DOCTYPE dblp SYSTEM "$fileDtd">
      <dblp>"""+ value.toString + "</dblp>"

      // implementation of Number of articles per year ----
      //convert inputXml from string into an XML literal with the loadString method
      val preprocessedXML = xml.XML.loadString(inputXml)
      val years = (preprocessedXML \\ "year").map(year => year.text.toLowerCase.trim).toList.sorted
      // extracting the NodeSeq of all matched elements

      //yearPublished.foreach(context.write(new Text(_),one)

    for (eachYear <- years){
      // emitting year,1 as key,value pair
      context.write(new Text(eachYear), new IntWritable(1))
    }
  }

}
