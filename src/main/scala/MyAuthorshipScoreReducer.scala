import java.lang

import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class MyAuthorshipScoreReducer extends Reducer[Text,DoubleWritable,Text,DoubleWritable]{
  val Logger = LoggerFactory.getLogger(this.getClass)
  override def reduce(key: Text, values: lang.Iterable[DoubleWritable], context: Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {
    val sum = values.asScala.foldLeft(0.0)(_ + _.get)
    context.write(key, new DoubleWritable(sum))
  }

}
