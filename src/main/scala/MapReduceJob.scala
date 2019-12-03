import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author Karan Raghani
  * This is the Starting point of the Map reduce Job, defining the configurations of the mapper reducer and the input and output formats
  */
object MapReduceJob {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val conf = ConfigFactory.load()

    val configuration = new Configuration
    // referred to Mahouts Documentation for XML parser
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))
    configuration.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

    // First Map/Reducer to Count number of authors Job1

    logger.info("Configuration for Author Count MapReduce Job")
    val job = Job.getInstance(configuration, "Author_Count")
    job.setJarByClass(this.getClass)
    //Mapper for Author Count
    job.setMapperClass(classOf[MyAuthorMapper])
    job.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job.setCombinerClass(classOf[MyAuthorReducer])
    //Reducer for Author_count
    job.setReducerClass(classOf[MyAuthorReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(conf.getString("PATH_OUTPUT1")))
    //System.exit(if (job.waitForCompletion(true)) 0 else 1)
    // waiting for the job to complete
    job.waitForCompletion(true)

    // Job Configuration for Year Mapper Reducer Job 2
    logger.info("Configuration for Year MapReduce Job")
    val job2 = Job.getInstance(configuration, "Year_MapperReducer")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[MyArticleMapper])
    job2.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job2.setCombinerClass(classOf[MyArticleReducer])
    // setting up my Year Reducer
    job2.setReducerClass(classOf[MyArticleReducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job2, new Path(args(0)))
    FileOutputFormat.setOutputPath(job2, new Path(args(1) + conf.getString("PATH_OUTPUT2")))
    //System.exit(if (job.waitForCompletion(true)) 0 else 1)
    // waiting for the job to complete
    job2.waitForCompletion(true)

    // Job Configuration for Author Range Map Reduce job 3
    logger.info("Configuration for Author Range MapReduce Job")
    val job3 = Job.getInstance(configuration, "AuthorshipRange_MapperReducer")
    job3.setJarByClass(this.getClass)
    job3.setMapperClass(classOf[MyAuthorRangeMapper])
    job3.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job3.setCombinerClass(classOf[MyArticleReducer])
    // setting up Year Reducer
    job3.setReducerClass(classOf[MyArticleReducer])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job3, new Path(args(0)))
    FileOutputFormat.setOutputPath(job3, new Path(args(1) + conf.getString("PATH_OUTPUT3")))
    //System.exit(if (job3.waitForCompletion(true)) 0 else 1)
    // waiting for the job to complete
    job3.waitForCompletion(true)

    // Jon Configuration for Authorship Score Map Reduce Job 4
    logger.info("Configuration for Authorship Score MapReduce Job")
    val job4 = Job.getInstance(configuration, "AuthorshipScore_MapperReducer")
    job4.setJarByClass(this.getClass)
    job4.setMapperClass(classOf[MyAuthorshipScoreMapper])
    job4.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job4.setCombinerClass(classOf[MyAuthorshipScoreReducer])
    // setting up my Year Reducer
    job4.setReducerClass(classOf[MyAuthorshipScoreReducer])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[DoubleWritable])
    FileInputFormat.addInputPath(job4, new Path(args(0)))
    FileOutputFormat.setOutputPath(job4, new Path(conf.getString("PATH_OUTPUT4")))
    //System.exit(if (job4.waitForCompletion(true)) 0 else 1)
    // waiting for the job to complete
    job4.waitForCompletion(true)

    // Jon Configuration for AuthorCollaboration Score Map Reduce Job 5
    logger.info("Configuration for AuthorCollaboration Score MapReduce Job")
    val job5 = Job.getInstance(configuration, "AuthorCollaboration_MapperReducer")
    job5.setJarByClass(this.getClass)
    job5.setMapperClass(classOf[MyAuthorCollaborationMapper])
    job5.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job5.setMapOutputKeyClass(classOf[Text])
    job5.setMapOutputValueClass(classOf[IntWritable])
    //job5.setCombinerClass(classOf[MyAuthorshipScoreReducer])
    // setting up my Year Reducer
    job5.setReducerClass(classOf[MyStatReducer])
    job5.setOutputKeyClass(classOf[Text])
    job5.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job5, new Path(args(0)))
    FileOutputFormat.setOutputPath(job5, new Path(conf.getString("PATH_OUTPUT5")))
    System.exit(if (job5.waitForCompletion(true)) 0 else 1)

  }
}