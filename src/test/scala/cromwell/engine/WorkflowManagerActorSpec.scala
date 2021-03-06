package cromwell.engine

import java.util.UUID

import akka.testkit.{EventFilter, TestActorRef, _}
import cromwell.CromwellTestkitSpec._
import cromwell.engine.workflow.WorkflowManagerActor._
import wdl4s._
import wdl4s.command.CommandPart
import wdl4s.types.{WdlArrayType, WdlStringType}
import wdl4s.values.{WdlArray, WdlInteger, WdlString}
import cromwell.engine.ExecutionStatus.{NotStarted, Running}
import cromwell.engine.backend.local.LocalBackend
import cromwell.engine.backend.{Backend, CallLogs}
import cromwell.engine.db.DataAccess._
import cromwell.engine.db.ExecutionDatabaseKey
import cromwell.engine.workflow.WorkflowManagerActor
import cromwell.util.SampleWdl
import cromwell.util.SampleWdl.{HelloWorld, HelloWorldWithoutWorkflow, Incr}
import cromwell.webservice.WorkflowMetadataResponse
import cromwell.{engine, CromwellSpec, CromwellTestkitSpec}

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import Hashing._

class WorkflowManagerActorSpec extends CromwellTestkitSpec {
  val backendInstance = Backend.from(CromwellSpec.Config, system)

  "A WorkflowManagerActor" should {

    val TestExecutionTimeout = 5.seconds.dilated

    "run the Hello World workflow" in {

      implicit val workflowManagerActor = TestActorRef(WorkflowManagerActor.props(backendInstance), self, "Test the WorkflowManagerActor")

      val workflowId = waitForHandledMessagePattern(pattern = "transitioning from Running to Succeeded") {
        messageAndWait[WorkflowId](SubmitWorkflow(HelloWorld.asWorkflowSources()))
      }

      val status = messageAndWait[Option[WorkflowState]](WorkflowStatus(workflowId)).get
      status shouldEqual WorkflowSucceeded

      val workflowOutputs = messageAndWait[engine.WorkflowOutputs](WorkflowOutputs(workflowId))

      val actualWorkflowOutputs = workflowOutputs.map { case (k, CallOutput(WdlString(string), _)) => k -> string }
      actualWorkflowOutputs shouldEqual Map(HelloWorld.OutputKey -> HelloWorld.OutputValue)

      val callOutputs = messageAndWait[engine.CallOutputs](CallOutputs(workflowId, "hello.hello"))
      val actualCallOutputs = callOutputs.map { case (k, CallOutput(WdlString(string), _)) => k -> string }
      actualCallOutputs shouldEqual Map("salutation" -> HelloWorld.OutputValue)

    }

    "Not try to restart any workflows when there are no workflows in restartable states" in {
      waitForPattern("Found no workflows to restart.") {
        TestActorRef(WorkflowManagerActor.props(backendInstance), self, "No workflows")
      }

    }

    "Try to restart workflows when there are workflows in restartable states" in {
      val workflows = Map(
        WorkflowId(UUID.randomUUID()) -> WorkflowSubmitted,
        WorkflowId(UUID.randomUUID()) -> WorkflowRunning)
      val ids = workflows.keys.map(_.toString).toSeq.sorted
      val key = SymbolStoreKey("hello.hello", "addressee", None, input = true)
      val worldWdlString = WdlString("world")

      import ExecutionContext.Implicits.global
      val setupFuture = Future.sequence(
        workflows map { case (workflowId, workflowState) =>
          val status = if (workflowState == WorkflowSubmitted) NotStarted else Running
          val descriptor = WorkflowDescriptor(workflowId, SampleWdl.HelloWorld.asWorkflowSources())
          val worldSymbolHash = worldWdlString.getHash(descriptor)
          val symbols = Map(key -> new SymbolStoreEntry(key, WdlStringType, Option(worldWdlString), worldSymbolHash))
          // FIXME? null AST
          val task = Task.empty
          val call = new Call(None, key.scope, task, Set.empty[FullyQualifiedName], Map.empty, None)
          for {
            _ <- globalDataAccess.createWorkflow(descriptor, symbols.values, Seq(call), new LocalBackend(system))
            _ <- globalDataAccess.updateWorkflowState(workflowId, workflowState)
            _ <- globalDataAccess.setStatus(workflowId, Seq(ExecutionDatabaseKey(call.fullyQualifiedName, None)), status)
          } yield ()
        }
      )
      Await.result(setupFuture, Duration.Inf)

      waitForPattern("Restarting workflow IDs: " + ids.mkString(", ")) {
        waitForPattern("Found 2 workflows to restart.") {
          // Workflows are always set back to Submitted on restart.
          waitForPattern("transitioning from Submitted to Running.", occurrences = 2) {
            // Both the previously in-flight call and the never-started call should get started.
            waitForPattern("starting calls: hello.hello", occurrences = 2) {
              waitForPattern("transitioning from Running to Succeeded", occurrences = 2) {
                TestActorRef(WorkflowManagerActor.props(backendInstance), self, "2 restartable workflows")
              }
            }
          }
        }
      }
    }


    "Handle coercion failures gracefully" in {
      within(TestExecutionTimeout) {
        implicit val workflowManagerActor = TestActorRef(WorkflowManagerActor.props(backendInstance), self, "Test WorkflowManagerActor coercion failures")
        waitForErrorWithException("Workflow failed submission") {
          Try {
            messageAndWait[WorkflowId](SubmitWorkflow(Incr.asWorkflowSources()))
          } match {
            case Success(_) => fail("Expected submission to fail with uncoercable inputs")
            case Failure(e) =>
              e.getMessage contains  "\nThe following errors occurred while processing your inputs:\n\nCould not coerce value for 'incr.incr.val' into: WdlIntegerType"
          }
        }
      }

    }

    "error when running a workflowless WDL" in {

      implicit val workflowManagerActor = TestActorRef(WorkflowManagerActor.props(backendInstance), self, "Test a workflowless submission")
      Try(messageAndWait[WorkflowId](SubmitWorkflow(HelloWorldWithoutWorkflow.asWorkflowSources()))) match {
        case Success(_) => fail("Expected submission to fail due to no runnable workflows")
        case Failure(e) => e.getMessage contains  "Namespace does not have a local workflow to run"
      }

    }

    "error when asked for outputs of a nonexistent workflow" in {

      within(TestExecutionTimeout) {
        implicit val workflowManagerActor = TestActorRef(WorkflowManagerActor.props(backendInstance),
          self, "Test WorkflowManagerActor output lookup failure")
        val id = WorkflowId(UUID.randomUUID())
        Try {
          messageAndWait[engine.WorkflowOutputs](WorkflowOutputs(id))
        } match {
          case Success(_) => fail("Expected lookup to fail with unknown workflow")
          case Failure(e) => e.getMessage shouldBe s"Workflow '$id' not found"
        }

      }
    }

    "error when asked for call logs of a nonexistent workflow" in {

      within(TestExecutionTimeout) {
        implicit val workflowManagerActor = TestActorRef(WorkflowManagerActor.props(backendInstance),
          self, "Test WorkflowManagerActor call log lookup failure")
        val id = WorkflowId.randomId()
        Try {
          messageAndWait[CallLogs](CallStdoutStderr(id, "foo.bar"))
        } match {
          case Success(_) => fail("Expected lookup to fail with unknown workflow")
          case Failure(e) => e.getMessage shouldBe s"Workflow '$id' not found"
        }
      }
    }


    "error when asked for logs of a nonexistent workflow" in {

      within(TestExecutionTimeout) {
        implicit val workflowManagerActor = TestActorRef(WorkflowManagerActor.props(backendInstance),
          self, "Test WorkflowManagerActor log lookup failure")
        val id = WorkflowId.randomId()
        Try {
          messageAndWait[Map[LocallyQualifiedName, CallLogs]](WorkflowStdoutStderr(id))
        } match {
          case Success(_) => fail("Expected lookup to fail with unknown workflow")
          case Failure(e) => e.getMessage shouldBe s"Workflow '$id' not found"
        }
      }

    }

    "run workflows in the correct directory" in {
      val outputs = runWdl(sampleWdl = SampleWdl.CurrentDirectory,
        EventFilter.info(pattern = s"starting calls: whereami.whereami", occurrences = 1))
      val outputName = "whereami.whereami.pwd"
      val salutation = outputs.get(outputName).get
      val actualOutput = salutation.asInstanceOf[CallOutput].wdlValue.asInstanceOf[WdlString].value.trim
      actualOutput should endWith("/call-whereami")
    }

    "build metadata correctly" in {

      implicit val workflowManagerActor = TestActorRef(WorkflowManagerActor.props(backendInstance), self, "Test Workflow metadata construction")

      val workflowId = waitForHandledMessagePattern(pattern = "transitioning from Running to Succeeded") {
        messageAndWait[WorkflowId](SubmitWorkflow(new SampleWdl.ScatterWdl().asWorkflowSources()))
      }

      val status = messageAndWait[Option[WorkflowState]](WorkflowStatus(workflowId)).get
      status shouldEqual WorkflowSucceeded

      val metadata = messageAndWait[WorkflowMetadataResponse](WorkflowMetadata(workflowId))
      metadata should not be null

      metadata.status shouldBe WorkflowSucceeded.toString
      metadata.start shouldBe defined
      metadata.end shouldBe defined
      metadata.outputs shouldBe defined
      metadata.outputs.get should have size 5
      metadata.calls should have size 5

      // ok if this explodes, it's a test
      val devOutputs = metadata.outputs.get.get("w.A.A_out").get
      val wdlArray = devOutputs.asInstanceOf[WdlArray]
      wdlArray.wdlType shouldBe WdlArrayType(WdlStringType)

      (wdlArray.value map { case WdlString(string) => string }) shouldEqual Vector("jeff", "chris", "miguel", "thibault", "khalid", "scott")

      val devCalls = metadata.calls.get("w.C").get
      devCalls should have size 6
      devCalls foreach { call =>
        call.start shouldBe defined
        call.end shouldBe defined
        call.jobId should not be defined
        call.returnCode.get shouldBe 0
        call.stdout shouldBe defined
        call.stderr shouldBe defined
        call.inputs should have size 1
        call.backend.get shouldEqual "Local"
        call.backendStatus should not be defined
        call.executionStatus shouldBe "Done"
      }

      (devCalls map { _.outputs.get.get("C_out").get.asInstanceOf[WdlInteger].value }) shouldEqual Vector(400, 500, 600, 800, 600, 500)
    }
  }
}
