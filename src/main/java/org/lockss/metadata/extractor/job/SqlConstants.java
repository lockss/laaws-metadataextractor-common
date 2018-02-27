/*

Copyright (c) 2014-2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.metadata.extractor.job;

/**
 * Constants used in SQL code.
 * 
 * @author Fernando García-Loygorri
 */
public class SqlConstants {
  //
  // Database table names.
  //
  /** Name of the job type table. */
  public static final String JOB_TYPE_TABLE = "job_type";

  /** Name of the job status table. */
  public static final String JOB_STATUS_TABLE = "job_status";

  /** Name of the job table. */
  public static final String JOB_TABLE = "job";

  //
  // Database table column names.
  //
  /** Name of plugin_id column. */
  public static final String PLUGIN_ID_COLUMN = "plugin_id";

  /** Name of au_key column. */
  public static final String AU_KEY_COLUMN = "au_key";

  /** Type name column. */
  public static final String TYPE_NAME_COLUMN = "type_name";

  /** Priority column. */
  public static final String PRIORITY_COLUMN = "priority";

  /** Archival Unit creation time column. */
  public static final String CREATION_TIME_COLUMN = "creation_time";

  /** Job type identifier column. */
  public static final String JOB_TYPE_SEQ_COLUMN = "job_type_seq";

  /** Job status identifier column. */
  public static final String JOB_STATUS_SEQ_COLUMN = "job_status_seq";

  /** Status name column. */
  public static final String STATUS_NAME_COLUMN = "status_name";

  /** Job identifier column. */
  public static final String JOB_SEQ_COLUMN = "job_seq";

  /** Description column. */
  public static final String DESCRIPTION_COLUMN = "description";

  /** Job start time column. */
  public static final String START_TIME_COLUMN = "start_time";

  /** Job end time column. */
  public static final String END_TIME_COLUMN = "end_time";

  /** Status message column. */
  public static final String STATUS_MESSAGE_COLUMN = "status_message";

  /** Owner column. */
  public static final String OWNER_COLUMN = "owner";

  //
  // Maximum lengths of variable text length database columns.
  //
  public static final int MAX_PLUGIN_ID_COLUMN = 256;

  /** Length of the AU key column. */
  public static final int MAX_AU_KEY_COLUMN = 512;

  /** Length of the type name column. */
  public static final int MAX_TYPE_NAME_COLUMN = 32;

  /** Length of the status name column. */
  public static final int MAX_STATUS_NAME_COLUMN = 32;

  /** Length of the description column. */
  public static final int MAX_DESCRIPTION_COLUMN = 128;

  /** Length of the status message column. */
  public static final int MAX_STATUS_MESSAGE_COLUMN = 512;

  /** Length of the owner column. */
  public static final int MAX_OWNER_COLUMN = 32;

  /**
   * Types of jobs.
   */
  public static final String JOB_TYPE_DELETE_AU = "delete_au";
  public static final String JOB_TYPE_PUT_AU = "put_au";
  public static final String JOB_TYPE_PUT_INCREMENTAL_AU = "put_incremental_au";

  /**
   * Statuses of jobs.
   */
  public static final String JOB_STATUS_CREATED = "created";
  public static final String JOB_STATUS_DELETED = "deleted";
  public static final String JOB_STATUS_DONE = "done";
  public static final String JOB_STATUS_RUNNING = "running";
  public static final String JOB_STATUS_TERMINATED = "terminated";
  public static final String JOB_STATUS_TERMINATING = "terminating";
}
