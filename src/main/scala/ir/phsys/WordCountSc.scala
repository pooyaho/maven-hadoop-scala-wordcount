/**
 * @author : Пуя Гуссейни
 *         Email : info@pooya-hfp.ir
 *         Date: 10/19/13
 *         Time: 4:56 PM
 */
package ir.phsys

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._

import java.util.Iterator
import java.util.StringTokenizer

object WordCountSc {
  def main(args: Array[String]) {
    val conf: JobConf = new JobConf(classOf[WordCount])
    conf.setJobName("wordcount")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[WordCount.Map])
    conf.setCombinerClass(classOf[WordCount.Reduce])
    conf.setReducerClass(classOf[WordCount.Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[_, _]])
    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))
    JobClient.runJob(conf)
  }

  object Map {
    private final val one: IntWritable = new IntWritable(1)
  }

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter) {
      val line: String = value.toString
      val tokenizer: StringTokenizer = new StringTokenizer(line)
      while (tokenizer.hasMoreTokens) {
        word.set(tokenizer.nextToken)
        output.collect(word, Map.one)
      }
    }

    private val word: Text = new Text
  }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    def reduce(key: Text, values: Iterator[IntWritable], output: OutputCollector[Text, IntWritable],
               reporter: Reporter) {
      var sum: Int = 0
      while (values.hasNext) {
        sum += values.next.get
      }
      output.collect(key, new IntWritable(sum))
    }
  }

}
