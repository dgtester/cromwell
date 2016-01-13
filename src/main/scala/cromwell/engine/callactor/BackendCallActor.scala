package cromwell.engine.callactor

import cromwell.engine.callexecution.CallExecutionActor
import cromwell.engine.{AbortRegistrationFunction, WorkflowDescriptor}
import cromwell.engine.backend.Backend
import cromwell.engine.workflow.BackendCallKey
import wdl4s._

class BackendCallActor(override val key: BackendCallKey,
                       locallyQualifiedInputs: CallInputs,
                       backend: Backend,
                       override val workflowDescriptor: WorkflowDescriptor) extends CallActor {

  override def getCallExecutionActor = {
    val backendCall = backend.bindCall(workflowDescriptor, key, locallyQualifiedInputs, Option(AbortRegistrationFunction(registerAbortFunction)))
    val executionActorName = s"CallExecutionActor-${workflowDescriptor.id}-${call.unqualifiedName}"
    context.actorOf(CallExecutionActor.props(backendCall), executionActorName)
  }
}
