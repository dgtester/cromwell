package cromwell.engine.backend

import cromwell.engine.ExecutionEventEntry
import cromwell.engine.ExecutionHash
import cromwell.engine.CallOutputs

/**
 * ADT representing the result of an execution of a BackendCall.
 */
sealed trait ExecutionResult

/**
 * A successful execution with resolved outputs.
 */
final case class SuccessfulExecution(outputs: CallOutputs, executionEvents: Seq[ExecutionEventEntry], returnCode: Int, hash: ExecutionHash, resultsClonedFrom: Option[BackendCall] = None) extends ExecutionResult

/**
 * A user-requested abort of the command.
 */
case object AbortedExecution extends ExecutionResult

/**
 * Failed execution, possibly having a return code.
 */
final case class FailedExecution(e: Throwable, returnCode: Option[Int] = None) extends ExecutionResult

