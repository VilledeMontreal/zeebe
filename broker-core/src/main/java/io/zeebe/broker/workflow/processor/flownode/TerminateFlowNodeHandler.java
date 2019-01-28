/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow.processor.flownode;

import io.zeebe.broker.incident.processor.IncidentState;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.EventOutput;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class TerminateFlowNodeHandler implements BpmnStepHandler<ExecutableFlowNode> {

  protected final IncidentState incidentState;
  private BpmnStepContext<ExecutableFlowNode> context;

  public TerminateFlowNodeHandler(IncidentState incidentState) {
    this.incidentState = incidentState;
  }

  @Override
  public void handle(BpmnStepContext<ExecutableFlowNode> context) {
    this.context = context;
    final EventOutput output = context.getOutput();
    final TypedRecord<WorkflowInstanceRecord> record = context.getRecord();

    if (terminate(context)) {
      final long elementInstanceKey = record.getKey();

      incidentState.forExistingWorkflowIncident(elementInstanceKey, this::resolveExistingIncident);
      context.getElementInstanceState().getVariablesState().removeScope(elementInstanceKey);
      context.getValue().setVariableScopeKey(record.getValue().getScopeInstanceKey());

      output.appendFollowUpEvent(
          record.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATED, context.getValue());
    }
  }

  /**
   * To be overridden by subclasses
   *
   * @param context current processor context
   * @return true if terminated, false otherwise
   */
  protected boolean terminate(BpmnStepContext<ExecutableFlowNode> context) {
    return true;
  }

  private void resolveExistingIncident(IncidentRecord incidentRecord, long incidentKey) {
    context.getOutput().appendResolvedIncidentEvent(incidentKey, incidentRecord);
  }
}
