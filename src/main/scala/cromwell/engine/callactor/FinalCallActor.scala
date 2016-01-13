package cromwell.engine.callactor

import akka.actor.ActorRef
import cromwell.engine.WorkflowDescriptor
import cromwell.engine.callexecution.CallExecutionActor
import cromwell.engine.workflow.FinalCallKey

class FinalCallActor(override val key: FinalCallKey) extends CallActor {

  override protected def getCallExecutionActor: ActorRef = context.actorOf(CallExecutionActor.props(key.scope))

  // Marked as lazy so that it can be called in the superclass constructor:
  override lazy val workflowDescriptor: WorkflowDescriptor = key.scope.workflow
}
