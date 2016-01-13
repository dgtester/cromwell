package cromwell.engine.callexecution

import cromwell.engine.backend._
import cromwell.logging.WorkflowLogger

import scala.language.postfixOps
import CallExecutionActor._

/**
  * Actor to manage the execution of a single backend call.
  * */
class BackendCallExecutionActor(backendCall: BackendCall) extends CallExecutionActor {

  override val logger = WorkflowLogger(
    "CallExecutionActor",
    backendCall.workflowDescriptor,
    akkaLogger = Option(akkaLogger),
    callTag = Option(backendCall.key.tag)
  )
  override val call = backendCall.call

  override def pollerFunction(handle: ExecutionHandle) = backendCall.poll(handle)

  override def executionFunction(mode: ExecutionMode) = mode match {
    case Execute => backendCall.execute
    case Resume(jobKey) => backendCall.resume(jobKey)
    case UseCachedCall(cachedBackendCall) => backendCall.useCachedCall(cachedBackendCall)
  }
}
