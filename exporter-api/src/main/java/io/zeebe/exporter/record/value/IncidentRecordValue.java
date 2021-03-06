/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter.record.value;

import io.zeebe.exporter.record.RecordValue;

/**
 * Represents a workflow incident.
 *
 * <p>See {@link io.zeebe.protocol.intent.IncidentIntent} for intents.
 */
public interface IncidentRecordValue extends RecordValue {
  /** @return the type of error this incident is caused by. */
  String getErrorType();

  /** @return the description of the error this incident is caused by. */
  String getErrorMessage();

  /**
   * @return the BPMN process id this incident belongs to. Can be <code>null</code> if the incident
   *     belongs to no workflow instance.
   */
  String getBpmnProcessId();

  /**
   * @return the key of the workflow instance this incident belongs to. Can be <code>null</code> if
   *     the incident belongs to no workflow instance.
   */
  long getWorkflowInstanceKey();

  /**
   * @return the id of the element this incident belongs to. Can be <code>null</code> if the
   *     incident belongs to no element or workflow instance.
   */
  String getElementId();

  /**
   * @return the key of the element instance this incident belongs to. Can be <code>null</code> if
   *     the incident belongs to no workflow element or workflow instance.
   */
  long getElementInstanceKey();

  /**
   * @return the key of the job this incident belongs to. Can be <code>null</code> if the incident
   *     belongs to no task.
   */
  long getJobKey();

  /**
   * @return the key of the element instance to use in order to update the correct payload before
   *     resolving the incident.
   */
  long getVariableScopeKey();
}
