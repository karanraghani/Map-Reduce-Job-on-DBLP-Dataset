import java.lang
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

/**
 * Summing up the number of publications in the given key Year.
 */

class MyArticleReducer extends Reducer[Text,IntWritable,Text,IntWritable] {

  val Logger = LoggerFactory.getLogger(this.getClass)
  Logger.info("Reducer for number of publication in an Year ")

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // iterating over values and summing up the iterables value
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      // emitting year, sum of publication in that year
      context.write(key, new IntWritable(sum))
    }
}

