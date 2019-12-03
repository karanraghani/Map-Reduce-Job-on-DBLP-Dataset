  import com.typesafe.config.ConfigFactory
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.io._
  import org.apache.hadoop.mapreduce.Mapper
  import org.slf4j.{Logger, LoggerFactory}

  import scala.xml.XML

  /**
   * Mapper to parse xml for different authors of a publication
   * Then for each author pass value iterable 1 to the reducer
   * key, value = authorname, 1
   */

  class MyAuthorMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    // loading up Logger
  val logger = LoggerFactory.getLogger(this.getClass)
    // loading up configuration file
  val conf = ConfigFactory.load()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    // Process the XML and extract only the CS authors form the XML input.
    val fileDtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

    // prefixing header for xml to be recognized and parsed by the XML parser
    val inputXml = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
      <!DOCTYPE dblp SYSTEM "$fileDtd">
      <dblp>"""+ value.toString + "</dblp>"

    // implementation authors and their number of publications
    //convert inputXml from string into an XML literal with the loadString method
    val preprocessedXML = xml.XML.loadString(inputXml)
    val authors = (preprocessedXML \\ "year").map(author => author.text.toLowerCase.trim).toList.sorted

    for (eachAuthor <- authors) {
      // emitting key value pars
      context.write(new Text(eachAuthor), new IntWritable(1))
    }

    }

}
