package cromwell.engine.workflow

import akka.actor.FSM.SubscribeTransitionCallBack
import akka.actor.{Actor, ActorRef, Props}
import akka.event.{Logging, LoggingReceive}
import akka.pattern.{ask, pipe}
import cromwell.engine
import cromwell.engine.ExecutionIndex._
import cromwell.engine.ExecutionStatus.ExecutionStatus
import cromwell.engine._
import cromwell.engine.EnhancedFullyQualifiedName
import cromwell.engine.backend.{Backend, CallLogs, CallMetadata}
import cromwell.engine.db.DataAccess._
import cromwell.engine.db.ExecutionDatabaseKey
import cromwell.engine.db.slick._
import cromwell.engine.workflow.WorkflowActor.{Restart, Start}
import cromwell.server.CromwellServer
import cromwell.util.WriteOnceStore
import cromwell.webservice._
import org.joda.time.DateTime
import spray.json._
import wdl4s._
import wdl4s.values.WdlFile
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object WorkflowManagerActor {
  class WorkflowNotFoundException(message: String) extends RuntimeException(message)
  class CallNotFoundException(message: String) extends RuntimeException(message)

  sealed trait WorkflowManagerActorMessage
  case class SubmitWorkflow(source: WorkflowSourceFiles) extends WorkflowManagerActorMessage
  case class WorkflowStatus(id: WorkflowId) extends WorkflowManagerActorMessage
  case class WorkflowQuery(parameters: Seq[(String, String)]) extends WorkflowManagerActorMessage
  case class WorkflowOutputs(id: WorkflowId) extends WorkflowManagerActorMessage
  case class CallOutputs(id: WorkflowId, callFqn: FullyQualifiedName) extends WorkflowManagerActorMessage
  case class CallStdoutStderr(id: WorkflowId, callFqn: FullyQualifiedName) extends WorkflowManagerActorMessage
  case class WorkflowStdoutStderr(id: WorkflowId) extends WorkflowManagerActorMessage
  case class SubscribeToWorkflow(id: WorkflowId) extends WorkflowManagerActorMessage
  case class WorkflowAbort(id: WorkflowId) extends WorkflowManagerActorMessage
  final case class WorkflowMetadata(id: WorkflowId) extends WorkflowManagerActorMessage
  final case class RestartWorkflows(workflows: Seq[WorkflowDescriptor]) extends WorkflowManagerActorMessage
  final case class CallCaching(id: WorkflowId, parameters: QueryParameters, call: Option[String]) extends WorkflowManagerActorMessage

  def props(backend: Backend): Props = Props(new WorkflowManagerActor(backend))

  // How long to delay between restarting each workflow that needs to be restarted.  Attempting to
  // restart 500 workflows at exactly the same time crushes the database connection pool.
  lazy val RestartDelay = 200 milliseconds
}

/**
 * Responses to messages:
 * SubmitWorkflow: Returns a `Future[WorkflowId]`
 * WorkflowStatus: Returns a `Future[Option[WorkflowState]]`
 * WorkflowOutputs: Returns a `Future[Option[binding.WorkflowOutputs]]` aka `Future[Option[Map[String, WdlValue]]`
 *
 */
class WorkflowManagerActor(backend: Backend) extends Actor with CromwellActor {
  import WorkflowManagerActor._
  private val log = Logging(context.system, this)
  private val tag = "WorkflowManagerActor"

  type WorkflowActorRef = ActorRef

  private val workflowStore = new WriteOnceStore[WorkflowId, WorkflowActorRef]

  if (getAbortJobsOnTerminate) {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        log.info(s"$tag: Received shutdown signal. Aborting all running workflows...")
        workflowStore.toMap.foreach{case (id, actor)=>
          CromwellServer.workflowManagerActor ! WorkflowManagerActor.WorkflowAbort(id)
        }
        var numRemaining = -1
        while(numRemaining != 0) {
          Thread.sleep(1000)
          val result = globalDataAccess.getWorkflowsByState(Seq(WorkflowRunning, WorkflowAborting))
          numRemaining = Await.result(result,Duration.Inf).size
          log.info(s"$tag: Waiting for all workflows to abort ($numRemaining remaining).")
        }
        log.info(s"$tag: All workflows aborted.")
      }
    })
  }

  override def preStart() {
    restartIncompleteWorkflows()
  }

  def receive = LoggingReceive {
    case SubmitWorkflow(source) => submitWorkflow(source, maybeWorkflowId = None) pipeTo sender
    case WorkflowStatus(id) => globalDataAccess.getWorkflowState(id) pipeTo sender
    case WorkflowQuery(rawParameters) => query(rawParameters) pipeTo sender
    case WorkflowAbort(id) =>
      workflowStore.toMap.get(id) match {
        case Some(x) =>
          x ! WorkflowActor.AbortWorkflow
          sender ! Some(WorkflowAborting)
        case None => sender ! None
      }
    case WorkflowOutputs(id) => workflowOutputs(id) pipeTo sender
    case CallOutputs(workflowId, callName) => callOutputs(workflowId, callName) pipeTo sender
    case CallStdoutStderr(workflowId, callName) => callStdoutStderr(workflowId, callName) pipeTo sender
    case WorkflowStdoutStderr(workflowId) => workflowStdoutStderr(workflowId) pipeTo sender
    case WorkflowMetadata(workflowId) => workflowMetadata(workflowId) pipeTo sender
    case SubscribeToWorkflow(id) =>
      //  NOTE: This fails silently. Currently we're ok w/ this, but you might not be in the future
      workflowStore.toMap.get(id) foreach {_ ! SubscribeTransitionCallBack(sender())}
    case RestartWorkflows(w :: ws) =>
      restartWorkflow(w)
      context.system.scheduler.scheduleOnce(RestartDelay) {
        self ! RestartWorkflows(ws)
      }
    case RestartWorkflows(Nil) => // No more workflows need restarting.
    case CallCaching(id, parameters, callName) => callCaching(id, parameters, callName) pipeTo sender
  }

  /**
   * Returns a `Future[Any]` which will be failed if there is no workflow with the specified id.
   */
  private def assertWorkflowExistence(id: WorkflowId): Future[Any] = {
    // Confirm the workflow exists by querying its state.  If no state is found the workflow doesn't exist.
    globalDataAccess.getWorkflowState(id) map {
      case None => throw new WorkflowNotFoundException(s"Workflow '$id' not found")
      case _ =>
    }
  }

  private def assertCallExistence(id: WorkflowId, callFqn: FullyQualifiedName): Future[Any] = {
    globalDataAccess.getExecutionStatus(id, ExecutionDatabaseKey(callFqn, None)) map {
      case None => throw new CallNotFoundException(s"Call '$callFqn' not found in workflow '$id'.")
      case _ =>
    }
  }

  /**
   * Retrieve the entries that produce stdout and stderr.
   */
  private def getCallLogKeys(id: WorkflowId, callFqn: FullyQualifiedName): Future[Seq[ExecutionDatabaseKey]] = {
    globalDataAccess.getExecutionStatuses(id, callFqn) map {
      case map if map.isEmpty => throw new CallNotFoundException(s"Call '$callFqn' not found in workflow '$id'.")
      case entries =>
        val callKeys = entries.keys filterNot { _.isCollector(entries.keys) }
        callKeys.toSeq.sortBy(_.index)
    }
  }

  private def workflowOutputs(id: WorkflowId): Future[engine.WorkflowOutputs] = {
    for {
      _ <- assertWorkflowExistence(id)
      outputs <- globalDataAccess.getWorkflowOutputs(id)
    } yield {
      SymbolStoreEntry.toWorkflowOutputs(outputs)
    }
  }

  private def callOutputs(workflowId: WorkflowId, callFqn: String): Future[engine.CallOutputs] = {
    for {
      _ <- assertWorkflowExistence(workflowId)
      _ <- assertCallExistence(workflowId, callFqn)
      outputs <- globalDataAccess.getOutputs(workflowId, ExecutionDatabaseKey(callFqn, None))
    } yield {
      SymbolStoreEntry.toCallOutputs(outputs)
    }
  }

  private def assertCallFqnWellFormed(descriptor: WorkflowDescriptor, callFqn: FullyQualifiedName): Try[String] = {
    descriptor.namespace.resolve(callFqn) match {
      case Some(c: Call) => Success(c.unqualifiedName)
      case _ => Failure(new UnsupportedOperationException("Expected a fully qualified name to have at least two parts"))
    }
  }

  private def hasLogs(entries: Iterable[ExecutionDatabaseKey])(key: ExecutionDatabaseKey) = {
    !key.fqn.isScatter && !key.isCollector(entries)
  }

  private def callStdoutStderr(workflowId: WorkflowId, callFqn: String): Future[Any] = {
    def callKey(descriptor: WorkflowDescriptor, callName: String, key: ExecutionDatabaseKey) =
      BackendCallKey(descriptor.namespace.workflow.findCallByName(callName).get, key.index)
    def backendCallFromKey(descriptor: WorkflowDescriptor, callName: String, key: ExecutionDatabaseKey) =
      backend.bindCall(descriptor, callKey(descriptor, callName, key))
    for {
        _ <- assertWorkflowExistence(workflowId)
        descriptor <- globalDataAccess.getWorkflow(workflowId)
        _ <- assertCallExistence(workflowId, callFqn)
        callName <- Future.fromTry(assertCallFqnWellFormed(descriptor, callFqn))
        callLogKeys <- getCallLogKeys(workflowId, callFqn)
        callStandardOutput <- Future.successful(callLogKeys.map(key => backendCallFromKey(descriptor, callName, key)).map(_.stdoutStderr))
      } yield callStandardOutput
  }

  private def workflowStdoutStderr(workflowId: WorkflowId): Future[Map[FullyQualifiedName, Seq[CallLogs]]] = {
    def logMapFromStatusMap(descriptor: WorkflowDescriptor, statusMap: Map[ExecutionDatabaseKey, ExecutionStatus]): Try[Map[FullyQualifiedName, Seq[CallLogs]]] = {
      Try {
        val sortedMap = statusMap.toSeq.sortBy(_._1.index)
        val callsToPaths = for {
          (key, status) <- sortedMap if hasLogs(statusMap.keys)(key)
          callName = assertCallFqnWellFormed(descriptor, key.fqn).get
          callKey = BackendCallKey(descriptor.namespace.workflow.findCallByName(callName).get, key.index)
          backendCall = backend.bindCall(descriptor, callKey)
          callStandardOutput = backend.stdoutStderr(backendCall)
        } yield key.fqn -> callStandardOutput

        callsToPaths groupBy { _._1 } mapValues { v => v map { _._2 } }
      }
    }

    for {
      _ <- assertWorkflowExistence(workflowId)
      descriptor <- globalDataAccess.getWorkflow(workflowId)
      callToStatusMap <- globalDataAccess.getExecutionStatuses(workflowId)
      x = callToStatusMap mapValues { _.executionStatus }
      callToLogsMap <- Future.fromTry(logMapFromStatusMap(descriptor, callToStatusMap mapValues { _.executionStatus }))
    } yield callToLogsMap
  }

  private def buildWorkflowMetadata(workflowExecution: WorkflowExecution,
                                    workflowExecutionAux: WorkflowExecutionAux,
                                    workflowOutputs: engine.WorkflowOutputs,
                                    callMetadata: Map[FullyQualifiedName, Seq[CallMetadata]]): WorkflowMetadataResponse = {

    val startDate = new DateTime(workflowExecution.startDt)
    val endDate = workflowExecution.endDt map { new DateTime(_) }
    val workflowInputs = Source.fromInputStream(workflowExecutionAux.jsonInputs.getAsciiStream).mkString.parseJson.asInstanceOf[JsObject]

    WorkflowMetadataResponse(
      id = workflowExecution.workflowExecutionUuid.toString,
      workflowName = workflowExecution.name,
      status = workflowExecution.status,
      // We currently do not make a distinction between the submission and start dates of a workflow, but it's
      // possible at least theoretically that a workflow might not begin to execute immediately upon submission.
      submission = startDate,
      start = Option(startDate),
      end = endDate,
      inputs = workflowInputs,
      outputs = Option(workflowOutputs) map { _.mapToValues },
      calls = callMetadata)
  }

  private def workflowMetadata(id: WorkflowId): Future[WorkflowMetadataResponse] = {
    for {
      workflowExecution <- globalDataAccess.getWorkflowExecution(id)
      workflowOutputs <- workflowOutputs(id)
      // The workflow has been persisted in the DB so we know the workflowExecutionId must be non-null,
      // so the .get on the Option is safe.
      workflowExecutionAux <- globalDataAccess.getWorkflowExecutionAux(WorkflowId.fromString(workflowExecution.workflowExecutionUuid))
      callStandardStreamsMap <- workflowStdoutStderr(id)
      executions <- globalDataAccess.getExecutions(WorkflowId.fromString(workflowExecution.workflowExecutionUuid))
      callInputs <- globalDataAccess.getAllInputs(id)
      callOutputs <- globalDataAccess.getAllOutputs(id)
      jesJobs <- globalDataAccess.jesJobInfo(id)
      localJobs <- globalDataAccess.localJobInfo(id)
      sgeJobs <- globalDataAccess.sgeJobInfo(id)
      executionEvents <- globalDataAccess.getAllExecutionEvents(id)

      callMetadata = CallMetadataBuilder.build(executions, callStandardStreamsMap, callInputs, callOutputs, executionEvents, jesJobs ++ localJobs ++ sgeJobs)
      workflowMetadata = buildWorkflowMetadata(workflowExecution, workflowExecutionAux, workflowOutputs, callMetadata)

    } yield workflowMetadata
  }

  private def submitWorkflow(source: WorkflowSourceFiles, maybeWorkflowId: Option[WorkflowId]): Future[WorkflowId] = {
    val workflowId: WorkflowId = maybeWorkflowId.getOrElse(WorkflowId.randomId())
    log.info(s"$tag submitWorkflow input id = $maybeWorkflowId, effective id = $workflowId")
    val isRestart = maybeWorkflowId.isDefined

    val futureId = for {
      descriptor <- Future.fromTry(Try(WorkflowDescriptor(workflowId, source)))
      workflowActor = context.actorOf(WorkflowActor.props(descriptor, backend), s"WorkflowActor-$workflowId")
      _ <- Future.fromTry(workflowStore.insert(workflowId, workflowActor))
      _ <- workflowActor ? (if (isRestart) Restart else Start)
    } yield workflowId

    futureId onFailure {
      case e =>
        val messageOrBlank = Option(e.getMessage).mkString
        log.error(e, s"$tag: Workflow failed submission: " + messageOrBlank)
    }

    futureId
  }

  private def restartWorkflow(restartableWorkflow: WorkflowDescriptor): Unit = {
    log.info("Invoking restartableWorkflow on " + restartableWorkflow.id.shortString)
    submitWorkflow(restartableWorkflow.sourceFiles, Option(restartableWorkflow.id))
  }

  private def restartIncompleteWorkflows(): Unit = {
    def logRestarts(restartableWorkflows: Traversable[WorkflowDescriptor]): Unit = {
      val num = restartableWorkflows.size
      val displayNum = if (num == 0) "no" else num.toString
      val plural = if (num == 1) "" else "s"

      log.info(s"$tag Found $displayNum workflow$plural to restart.")

      if (num > 0) {
        val ids = restartableWorkflows.map { _.id.toString }.toSeq.sorted
        log.info(s"$tag Restarting workflow ID$plural: " + ids.mkString(", "))
      }
    }

    val result = for {
      restartableWorkflows <- globalDataAccess.getWorkflowsByState(Seq(WorkflowSubmitted, WorkflowRunning))
      _ = logRestarts(restartableWorkflows)
      _ = self ! RestartWorkflows(restartableWorkflows.toSeq)
    } yield ()

    result recover {
      case e: Throwable => log.error(e, e.getMessage)
    }
  }

  private def query(rawParameters: Seq[(String, String)]): Future[WorkflowQueryResponse] = {
    for {
    // Future/Try to wrap the exception that might be thrown from WorkflowQueryParameters.apply.
      parameters <- Future.fromTry(Try(WorkflowQueryParameters(rawParameters)))
      response <- globalDataAccess.queryWorkflows(parameters)
    } yield response
  }

  private def callCaching(id: WorkflowId, parameters: QueryParameters, callName: Option[String]): Future[Int] = {
    for {
      _ <- assertWorkflowExistence(id)
      cachingParameters <- CallCachingParameters.from(id, callName, parameters)
      updateCount <- globalDataAccess.updateCallCaching(cachingParameters)
    } yield updateCount
  }
}
