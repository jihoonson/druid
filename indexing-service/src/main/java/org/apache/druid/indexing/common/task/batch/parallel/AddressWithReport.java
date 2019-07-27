/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.task.batch.parallel;

import java.util.Objects;

public class AddressWithReport<T extends SubTaskReport>
{
  private final String host;
  private final int port;
  private final T report;

  public AddressWithReport(String host, int port, T report)
  {
    this.host = host;
    this.port = port;
    this.report = report;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public T getReport()
  {
    return report;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AddressWithReport<?> that = (AddressWithReport<?>) o;
    return port == that.port &&
           Objects.equals(host, that.host) &&
           Objects.equals(report, that.report);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, port, report);
  }

  @Override
  public String toString()
  {
    return "AddressWithReport{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", report=" + report +
           '}';
  }
}
