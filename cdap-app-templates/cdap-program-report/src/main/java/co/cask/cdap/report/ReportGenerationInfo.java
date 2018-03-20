/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.report;

/**
 * Represents the information of the generation of a program operation status report in an HTTP response.
 */
public class ReportGenerationInfo {
  private final long created;
  private final ReportStatus status;
  private final String request;

  public ReportGenerationInfo(long created, ReportStatus status, String request) {
    this.created = created;
    this.status = status;
    this.request = request;
  }

  public long getCreated() {
    return created;
  }

  public ReportStatus getStatus() {
    return status;
  }

  public String getRequest() {
    return request;
  }
}