package cromwell.engine.callexecution

import cromwell.engine.backend.ExecutionHandle
import cromwell.engine.callexecution.CallExecutionActor._
import cromwell.engine.finalcall.FinalCall
import cromwell.logging.WorkflowLogger

import scala.concurrent.Future

case class FinalCallExecutionActor(override val call: FinalCall) extends CallExecutionActor {

  override val logger = WorkflowLogger(
    "CallExecutionActor",
    call.workflow,
    akkaLogger = Option(akkaLogger),
    callTag = Option(call.unqualifiedName)
  )

  override def pollerFunction(handle: ExecutionHandle) = call.poll(ec, handle)
  override def executionFunction(mode: ExecutionMode) = mode match {
    case UseCachedCall(cachedBackendCall) => Future.failed(new UnsupportedOperationException("Cannot use cached results for a FinalCall"))
    case _ => call.execute
  }
}
