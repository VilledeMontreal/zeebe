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

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.protocol.impl.record.value.incident.ErrorType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

/**
 * Applies output mappings, removes the element's variable scope, sets the record's variable scope
 * key to the flow scope key, and publishes the ELEMENT_COMPLETED event.
 */
public class CompleteFlowNodeHandler<T extends ExecutableFlowNode> implements BpmnStepHandler<T> {
  private final IOMappingHelper ioMappingHelper = new IOMappingHelper();
  private final WorkflowInstanceIntent completedIntent;

  public CompleteFlowNodeHandler() {
    this(WorkflowInstanceIntent.ELEMENT_COMPLETED);
  }

  public CompleteFlowNodeHandler(WorkflowInstanceIntent completedIntent) {
    this.completedIntent = completedIntent;
  }

  @Override
  public void handle(BpmnStepContext<T> context) {
    final TypedRecord<WorkflowInstanceRecord> record = context.getRecord();
    final long variableScopeKey = record.getValue().getVariableScopeKey();
    final long scopeInstanceKey = record.getValue().getScopeInstanceKey();
    final long elementInstanceKey = record.getKey();

    try {
      ioMappingHelper.applyOutputMappings(context, variableScopeKey, scopeInstanceKey);

      if (complete(context)) {
        context.getElementInstanceState().getVariablesState().removeScope(elementInstanceKey);
        context.getValue().setVariableScopeKey(scopeInstanceKey);
        context
            .getOutput()
            .appendFollowUpEvent(elementInstanceKey, completedIntent, context.getValue());
      }

    } catch (MappingException e) {
      context.raiseIncident(ErrorType.IO_MAPPING_ERROR, e.getMessage());
    }
  }

  /**
   * To be overridden by subclasses
   *
   * @param context current processor context
   * @return true if completed, false otherwise
   */
  public boolean complete(BpmnStepContext<T> context) {
    return true;
  }
}
