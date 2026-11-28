# Scarlet

Scarlet is a single-node data processing engine in Scala 3.

Scarlet's key design pillars are:
- Fast performance, with accelerated vectorized kernels
- Strong type safety, with column types passed to the kernel through query execution
- Lazy evaluation, enabling smart query optimization
- Functional style, where datasets are immutable and each step returns a new dataset

Scarlet is inspired by Spark's execution model and API.

### Example job

```scala
import com.ewoodbury.scarlet.api.DistCollection

object Purchases:
  def main(args: Array[String]): Unit =
    val orders = DistCollection(
      Seq(
        "ada" -> 40,
        "ada" -> 15,
        "grace" -> 20,
        "grace" -> 80,
        "linus" -> 5,
      ),
      numPartitions = 2,
    )

    val totals = orders
      .filter { case (_, amount) => amount >= 10 }
      .map { case (customer, amount) => customer -> amount.toLong }
      .reduceByKey(_ + _)

    val loyal = totals.filter { case (_, spent) => spent >= 50 }

    println(loyal.collect())
    println(loyal.map { case (_, spent) => spent }.sum)
```
