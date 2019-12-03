import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * Summing up the number of publications made by an author
  */

class MyAuthorReducer extends Reducer[Text, IntWritable, Text, IntWritable] {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Reducer for Author Count")

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    // iterating over values and summing up the iterables value
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    // emitting author, sum of values for each author
    context.write(key, new IntWritable(sum))
  }
}






