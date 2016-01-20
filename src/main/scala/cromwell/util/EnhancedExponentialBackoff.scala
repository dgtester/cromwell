package cromwell.util

import com.google.api.client.util.ExponentialBackOff

class EnhancedExponentialBackoff(enhancedBuilder: EnhancedExponentialBackoff.Builder) extends ExponentialBackOff(enhancedBuilder) {
  private var firstCall: Boolean = true

  override def nextBackOffMillis(): Long = {
    if (firstCall && enhancedBuilder.getInitialGapMillis.isDefined) {
      firstCall = false
      enhancedBuilder.getInitialGapMillis.get
    } else {
      super.nextBackOffMillis()
    }
  }
}

object EnhancedExponentialBackoff {
  class Builder extends ExponentialBackOff.Builder {
    private var gap: Option[Long] = None

    def setInitialGapMillis(gap: Int): EnhancedExponentialBackoff.Builder = {
      this.gap = Some(gap.toLong)
      this
    }

    def getInitialGapMillis = gap

    override def build() = {
      new EnhancedExponentialBackoff(this)
    }
  }
}