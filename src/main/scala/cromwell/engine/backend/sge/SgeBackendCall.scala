package cromwell.engine.backend.sge

import com.google.api.client.util.ExponentialBackOff
import cromwell.engine.backend.local.LocalBackend
import cromwell.engine.backend.{BackendCall, LocalFileSystemBackendCall, _}
import cromwell.engine.workflow.CallKey
import cromwell.engine.{AbortRegistrationFunction, WorkflowDescriptor}
import wdl4s.CallInputs

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class SgeBackendCall(backend: SgeBackend,
                          workflowDescriptor: WorkflowDescriptor,
                          key: CallKey,
                          locallyQualifiedInputs: CallInputs,
                          callAbortRegistrationFunction: Option[AbortRegistrationFunction]) extends BackendCall with LocalFileSystemBackendCall {
  val workflowRootPath = LocalBackend.hostExecutionPath(workflowDescriptor)
  val callRootPath = LocalBackend.hostCallPath(workflowDescriptor, call.unqualifiedName, key.index)
  val stdout = callRootPath.resolve("stdout")
  val stderr = callRootPath.resolve("stderr")
  val script = callRootPath.resolve("script.sh")
  val returnCode = callRootPath.resolve("rc")
  val engineFunctions: SgeEngineFunctions = new SgeEngineFunctions(callRootPath, stdout, stderr, workflowDescriptor.ioManager)
  callRootPath.toFile.mkdirs

  override def execute(implicit ec: ExecutionContext) = backend.execute(this)

  override def poll(previous: ExecutionHandle)(implicit ec: ExecutionContext) = Future.successful(previous)

  override def useCachedCall(avoidedTo: BackendCall)(implicit ec: ExecutionContext): Future[ExecutionHandle] =
    backend.useCachedCall(avoidedTo.asInstanceOf[SgeBackendCall], this)

  override def stdoutStderr: CallLogs = backend.stdoutStderr(this)

  override val pollBackoff = new ExponentialBackOff.Builder()
    .setInitialIntervalMillis(10.seconds.toMillis.toInt)
    .setMaxIntervalMillis(1.minute.toMillis.toInt)
    .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
    .setMultiplier(1.1)
    .build()
}
