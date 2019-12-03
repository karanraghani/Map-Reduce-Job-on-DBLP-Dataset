import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class MyAuthorshipScoreMapper extends Mapper[LongWritable, Text, Text, DoubleWritable] {
  val logger = LoggerFactory.getLogger(this.getClass.getName)
  val conf = ConfigFactory.load()
  // creating int iterable object with value 1 for context.write

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, DoubleWritable]#Context): Unit = {
    val fileDtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val inputXml =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
      <!DOCTYPE dblp SYSTEM "$fileDtd">
      <dblp>""" + value.toString + "</dblp>"

    val preprocessedXML = xml.XML.loadString(inputXml)
    val authors = (preprocessedXML \\ "author").map(year => year.text.toLowerCase.trim).toList

    val factor: Double = 1.0/authors.size
    var carryOver: Double = 0.0
    var authorshipScore: Double = 0.0

    for (eachAuthor <- authors.reverse) {
        if (eachAuthor != authors.head) {
          authorshipScore = (factor + carryOver) * 3.0 / 4.0
          carryOver = (factor + carryOver) * 1.0 / 4.0
        }
        else {
          authorshipScore = factor + carryOver
        }
        context.write(new Text(eachAuthor), new DoubleWritable(authorshipScore))
    }
  }
}
