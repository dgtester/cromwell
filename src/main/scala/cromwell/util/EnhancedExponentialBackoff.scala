package cromwell.util

import com.google.api.client.util.ExponentialBackOff

import scala.concurrent.duration.{Duration, FiniteDuration}

sealed trait Backoff {
  def backoffMillis: Long
  def next: InitializedBackoff
}

/**
  * Useful when using Backoffs in a "functional" backoff.next.backoffMillis way.
  * This prevents the first call to next to shunt out the first value of backoffMillis.
  */
final case class UnInitializedBackoff(next: InitializedBackoff) extends Backoff {
  override def backoffMillis = throw new UninitializedError()
}

sealed trait InitializedBackoff extends Backoff {
  def uninitialized: UnInitializedBackoff = UnInitializedBackoff(this)
}

final class InitialGapBackoff(initialGapMillis: FiniteDuration, googleBackoff: ExponentialBackOff) extends InitializedBackoff {

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

final class SimpleBackoff(googleBackoff: ExponentialBackOff) extends InitializedBackoff {

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