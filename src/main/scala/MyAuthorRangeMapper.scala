import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML


class MyAuthorRangeMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger = LoggerFactory.getLogger(this.getClass.getName)
  val conf = ConfigFactory.load()
  // creating int iterable object with value 1 for context.write

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val fileDtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val inputXml = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
      <!DOCTYPE dblp SYSTEM "$fileDtd">
      <dblp>"""+ value.toString + "</dblp>"

    // implementation of range on numbers of authors for articles
    //convert inputXml from string into an XML literal with the loadString method
    val preprocessedXML = xml.XML.loadString(inputXml)
    val authors = (preprocessedXML \\ "author").map(year => year.text.toLowerCase.trim).toList.sorted

    val count = authors.length
    var passkey = "0"
    if (count == 1){
    passkey = "1"
    }
    else if (count == 2){
    passkey = "2"
    }
    else if (count== 3){
    passkey = "3"
    }
    else if (count >= 4 && count <= 10){
    passkey = "4-10"
    }
    else if (count >= 11 && count <= 20){passkey = "11-20"}
    else if (count >= 21 && count <= 40){passkey = "21-40"}
    else if (count >= 41 && count <= 80){passkey = "41-80"}
    else if (count >= 81 && count <= 120){passkey = "81-120"}
    else if (count >= 121 && count <= 180){passkey = "121-180"}
    else if (count >= 181 && count <= 240){passkey = "181-240"}
    else if (count >= 241 && count <= 320){passkey = "241-320"}
    else if (count >= 321){passkey = "321-"}
    context.write(new Text(passkey), new IntWritable(1))
  }
}
