package cromwell.engine.backend.local

import better.files._
import cromwell.engine.backend.{BackendCall, LocalFileSystemBackendCall, _}
import cromwell.engine.workflow.CallKey
import cromwell.engine.{AbortRegistrationFunction, CallContext, WorkflowDescriptor}
import wdl4s.CallInputs

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class LocalBackendCall(backend: LocalBackend,
                            workflowDescriptor: WorkflowDescriptor,
                            key: CallKey,
                            locallyQualifiedInputs: CallInputs,
                            callAbortRegistrationFunction: Option[AbortRegistrationFunction]) extends BackendCall with LocalFileSystemBackendCall {
  val workflowRootPath = LocalBackend.hostExecutionPath(workflowDescriptor)
  val callRootPath = LocalBackend.hostCallPath(workflowDescriptor, call.unqualifiedName, key.index)
  val dockerContainerExecutionDir = LocalBackend.containerExecutionPath(workflowDescriptor)
  lazy val containerCallRoot = runtimeAttributes.docker match {
    case Some(docker) => LocalBackend.containerCallPath(workflowDescriptor, call.unqualifiedName, key.index)
    case None => callRootPath
  }
  val returnCode = callRootPath.resolve("rc")
  val stdout = callRootPath.resolve("stdout")
  val stderr = callRootPath.resolve("stderr")
  val script = callRootPath.resolve("script")
  private val callContext = new CallContext(callRootPath.fullPath, stdout.fullPath, stderr.fullPath)
  val engineFunctions = new LocalCallEngineFunctions(workflowDescriptor.ioManager, callContext)

  callRootPath.toFile.mkdirs

  override def execute(implicit ec: ExecutionContext) = backend.execute(this)

  override def poll(previous: ExecutionHandle)(implicit ec: ExecutionContext) = Future.successful(previous)

  override def useCachedCall(avoidedTo: BackendCall)(implicit ec: ExecutionContext): Future[ExecutionHandle] =
    backend.useCachedCall(avoidedTo.asInstanceOf[LocalBackendCall], this)

  override def stdoutStderr: CallLogs = backend.stdoutStderr(this)

  override val pollingInitialInterval: FiniteDuration = 10.seconds
  override val pollingMaxInterval: FiniteDuration = 10.minutes
  override val pollingMultiplier: Double = 1.1D
  override val pollingInitialGap: FiniteDuration = 0.second
}
