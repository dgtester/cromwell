package cromwell.util

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class EnhancedExponentialBackoffSpec extends FlatSpec with Matchers {

  it should "honor initial gap" in {
    val exponentialBackoff = {
      new EnhancedExponentialBackoff.Builder()
        .setInitialGapMillis(3.seconds.toMillis.toInt)
        .setInitialIntervalMillis(1.second.toMillis.toInt)
        .setMaxIntervalMillis(2.seconds.toMillis.toInt)
        .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
        .setRandomizationFactor(0D)
        .build()
    }

    exponentialBackoff.nextBackOffMillis() shouldBe 3.seconds.toMillis.toInt
    exponentialBackoff.nextBackOffMillis() shouldBe 1.second.toMillis.toInt
  }

  it should "not break original behavior" in {
    val exponentialBackoff = {
      new EnhancedExponentialBackoff.Builder()
        .setInitialIntervalMillis(1.second.toMillis.toInt)
        .setMaxIntervalMillis(2.seconds.toMillis.toInt)
        .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
        .setRandomizationFactor(0D)
        .build()
    }

    exponentialBackoff.nextBackOffMillis() shouldBe 1.second.toMillis.toInt
  }

}
