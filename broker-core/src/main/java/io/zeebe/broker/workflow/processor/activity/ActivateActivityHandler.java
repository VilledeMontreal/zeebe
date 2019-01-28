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
package io.zeebe.broker.workflow.processor.activity;

import io.zeebe.broker.workflow.model.element.ExecutableActivity;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.CatchEventBehavior.MessageCorrelationKeyException;
import io.zeebe.broker.workflow.processor.flownode.ActivateFlowNodeHandler;
import io.zeebe.protocol.impl.record.value.incident.ErrorType;

public class ActivateActivityHandler extends ActivateFlowNodeHandler<ExecutableActivity> {
  @Override
  protected boolean activate(BpmnStepContext<ExecutableActivity> context) {
    boolean activated = false;

    try {
      context.getCatchEventBehavior().subscribeToEvents(context, context.getElement());
      activated = true;
    } catch (MessageCorrelationKeyException e) {
      context.raiseIncident(ErrorType.EXTRACT_VALUE_ERROR, e.getMessage());
    }

    return activated;
  }
}
