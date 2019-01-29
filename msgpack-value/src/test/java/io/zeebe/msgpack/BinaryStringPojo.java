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
package io.zeebe.msgpack;

import io.zeebe.msgpack.property.BinaryProperty;
import io.zeebe.msgpack.property.StringProperty;
import org.agrona.DirectBuffer;

public class BinaryStringPojo extends UnpackedObject {
  private final BinaryProperty resourceProp = new BinaryProperty("resource");
  private final StringProperty resourceNameProp = new StringProperty("resourceName", "resource");

  public BinaryStringPojo() {
    this.declareProperty(resourceProp).declareProperty(resourceNameProp);
  }

  public DirectBuffer getResource() {
    return resourceProp.getValue();
  }

  public BinaryStringPojo setResource(DirectBuffer resource) {
    this.resourceProp.setValue(resource, 0, resource.capacity());
    return this;
  }

  public DirectBuffer getResourceName() {
    return resourceNameProp.getValue();
  }

  public BinaryStringPojo setResourceName(DirectBuffer resourceName) {
    this.resourceNameProp.setValue(resourceName);
    return this;
  }
}
