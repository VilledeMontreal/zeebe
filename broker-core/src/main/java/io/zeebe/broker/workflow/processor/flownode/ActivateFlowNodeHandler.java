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
import io.zeebe.broker.workflow.state.VariablesState;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.protocol.impl.record.value.incident.ErrorType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

/**
 * Will apply input mappings, create a variable scope for the element, and publish the given
 * activatedIntent to the stream. Overload {@link ActivateFlowNodeHandler#activate(BpmnStepContext)}
 * to hook into the process, between creating the variable scope and publishing the event.
 *
 * @param <T> the actual ExecutableFlowNode type
 */
public class ActivateFlowNodeHandler<T extends ExecutableFlowNode> implements BpmnStepHandler<T> {
  private final IOMappingHelper ioMappingHelper = new IOMappingHelper();
  private final WorkflowInstanceIntent activatedIntent;

  public ActivateFlowNodeHandler() {
    this(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
  }

  public ActivateFlowNodeHandler(WorkflowInstanceIntent activatedIntent) {
    this.activatedIntent = activatedIntent;
  }

  @Override
  public void handle(BpmnStepContext<T> context) {
    final VariablesState variablesState = context.getElementInstanceState().getVariablesState();
    final TypedRecord<WorkflowInstanceRecord> record = context.getRecord();
    final long currentVariableScopeKey = record.getValue().getVariableScopeKey();
    final long nextVariableScopeKey = record.getKey();

    try {
      ioMappingHelper.applyInputMappings(context, currentVariableScopeKey, nextVariableScopeKey);
      variablesState.createScope(nextVariableScopeKey, currentVariableScopeKey);
      context.getValue().setVariableScopeKey(nextVariableScopeKey);

      if (activate(context)) {
        context
            .getOutput()
            .appendFollowUpEvent(record.getKey(), activatedIntent, context.getValue());
      }
    } catch (MappingException e) {
      context.raiseIncident(ErrorType.IO_MAPPING_ERROR, e.getMessage());
    }
  }

  /**
   * To be overridden by subclasses
   *
   * @param context current processor context
   * @return true if activated, false otherwise
   */
  protected boolean activate(BpmnStepContext<T> context) {
    return true;
  }
}
