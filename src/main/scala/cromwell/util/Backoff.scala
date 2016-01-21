package cromwell.util

import com.google.api.client.util.ExponentialBackOff

import scala.concurrent.duration.{Duration, FiniteDuration}

sealed trait Backoff {
  def backoffMillis: Long
  def next: Backoff
}

final class InitialGapBackoff(initialGapMillis: FiniteDuration, googleBackoff: ExponentialBackOff) extends Backoff {

  assert(initialGapMillis.compareTo(Duration.Zero) != 0, "Initial gap cannot be null, use SimpleBackoff instead.")
  override val backoffMillis = initialGapMillis.toMillis

  def this(initialGap: FiniteDuration, initialInterval: FiniteDuration, maxInterval: FiniteDuration, multiplier: Double) = {
    this(initialGap, new ExponentialBackOff.Builder()
      .setInitialIntervalMillis(initialInterval.toMillis.toInt)
      .setMaxIntervalMillis(maxInterval.toMillis.toInt)
      .setMultiplier(multiplier)
      .setMaxElapsedTimeMillis(Int.MaxValue)
      .build())
  }

  override def next = new SimpleBackoff(googleBackoff)
}

final class SimpleBackoff(googleBackoff: ExponentialBackOff) extends Backoff {

  def this(initialInterval: FiniteDuration, maxInterval: FiniteDuration, multiplier: Double) = {
    this(new ExponentialBackOff.Builder()
      .setInitialIntervalMillis(initialInterval.toMillis.toInt)
      .setMaxIntervalMillis(maxInterval.toMillis.toInt)
      .setMultiplier(multiplier)
      .setMaxElapsedTimeMillis(Int.MaxValue)
      .build())
  }

  override def backoffMillis = googleBackoff.nextBackOffMillis()
  override def next = this
}