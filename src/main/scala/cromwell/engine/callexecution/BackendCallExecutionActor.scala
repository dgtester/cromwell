package cromwell.engine.callexecution

import com.google.api.client.util.ExponentialBackOff
import cromwell.engine.backend._
import cromwell.engine.callexecution.CallExecutionActor._
import cromwell.logging.WorkflowLogger

import scala.language.postfixOps

/**
  * Actor to manage the execution of a single backend call.
  * */
class BackendCallExecutionActor(backendCall: BackendCall) extends CallExecutionActor {
  override val logger = WorkflowLogger(
    this.getClass.getSimpleName,
    backendCall.workflowDescriptor,
    akkaLogger = Option(akkaLogger),
    callTag = Option(backendCall.key.tag)
  )

  override val call = backendCall.call
  override def poll(handle: ExecutionHandle) = backendCall.poll(handle)
  override def execute(mode: ExecutionMode) = mode match {
    case Execute => backendCall.execute
    case Resume(jobKey) => backendCall.resume(jobKey)
    case UseCachedCall(cachedBackendCall) => backendCall.useCachedCall(cachedBackendCall)
  }

  override val backoff: ExponentialBackOff = backendCall.backend.pollBackoff
}
