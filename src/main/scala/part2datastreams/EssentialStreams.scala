package part2datastreams

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object EssentialStreams {

  def applicationTemplate(): Unit = {
    // 1 - execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // any computations
    val simpleNumberStream: DataStream[Int] = env.fromElements(1,2,3,4)

    // perform some actions
    simpleNumberStream.print()

    // end
    env.execute() // triggers above descibed computations
  }

  // transformations
  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1,2, 3, 4, 5)

    // checking parallelism
    println(s"Current Parallelism: ${env.getParallelism}")

    // set different parallelism
    env.setParallelism(2)
    println(s"New Parallelism: ${env.getParallelism}")

    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    val expandedNumbers: DataStream[Int] = numbers.flatMap(n => List(n, n+1))

    val filteredNumbers: DataStream[Int] = numbers
      .filter(_ % 2 == 0)
      .setParallelism(4)

    val finalData = expandedNumbers.writeAsText("output/expandedStream.txt")
    // set parallelism in the sink
    finalData.setParallelism(3)

    env.execute()
  }

  /**
   * Exercise Fizzbuzz on flink
   * - take a stream of 100 natural numbers
   * - for every number
   *   - if n % 3 == 0 => "fizz"
   *   - if n % 5 == 0 => "buzz"
   *   - if both => "fizzbuzz"
   *  - write the "fizzbuzz" numbers to a file
   */
  def fizzBuzz(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val numbers: DataStream[Int] = env.fromCollection(Seq.range(0, 100))
    val numbers: DataStream[Long] = env.fromSequence(0, 100)

    val fbNumbers = numbers
      .map(n => {
        val fbValue = n match {
          case x if x % 3 == 0 && x % 5 == 0 => "fizzbuzz"
          case x if x % 3 == 0 => "fizz"
          case x if x % 5 == 0 => "buzz"
          case _ => ""
        }
        (n, fbValue)
      })
      .filter(_._2.equals("fizzbuzz"))
      .map(_._1)
      .setParallelism(4)

//    val finalData = fbNumbers.writeAsText("output/fizzbuzz.txt").setParallelism(1)
    fbNumbers.addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("output/streaming_sink"),
          new SimpleStringEncoder[Long]("UTF-8")
        )
        .build()
    ).setParallelism(1)

    env.execute()
  }



  def main(args: Array[String]): Unit = {
    fizzBuzz()
  }
}
