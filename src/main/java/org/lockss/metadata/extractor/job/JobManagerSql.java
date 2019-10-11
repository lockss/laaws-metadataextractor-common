/*

Copyright (c) 2016-2019 Board of Trustees of Leland Stanford Jr. University,
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

import static java.sql.Types.*;
import static org.lockss.metadata.extractor.job.SqlConstants.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lockss.db.DbException;
import org.lockss.plugin.PluginManager;
import org.lockss.util.Logger;

/**
 * The JobManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class JobManagerSql {
  private static final Logger log = Logger.getLogger(JobManagerSql.class);

  private static final String INITIAL_JOB_STATUS_MESSAGE = "Waiting for launch";

  // Query to retrieve a job.
  private static final String FIND_JOB_QUERY = "select "
      + JOB_SEQ_COLUMN
      + ", " + JOB_TYPE_SEQ_COLUMN
      + ", " + DESCRIPTION_COLUMN
      + ", " + PLUGIN_ID_COLUMN
      + ", " + AU_KEY_COLUMN
      + ", " + CREATION_TIME_COLUMN
      + ", " + START_TIME_COLUMN
      + ", " + END_TIME_COLUMN
      + ", " + JOB_STATUS_SEQ_COLUMN
      + ", " + STATUS_MESSAGE_COLUMN
      + ", " + PRIORITY_COLUMN
      + " from " + JOB_TABLE
      + " where " + JOB_SEQ_COLUMN + " = ?";

  // Query to retrieve all the jobs of an Archival Unit.
  private static final String FIND_AU_JOB_QUERY = "select "
      + JOB_SEQ_COLUMN
      + ", " + JOB_TYPE_SEQ_COLUMN
      + ", " + DESCRIPTION_COLUMN
      + ", " + PLUGIN_ID_COLUMN
      + ", " + AU_KEY_COLUMN
      + ", " + CREATION_TIME_COLUMN
      + ", " + START_TIME_COLUMN
      + ", " + END_TIME_COLUMN
      + ", " + JOB_STATUS_SEQ_COLUMN
      + ", " + STATUS_MESSAGE_COLUMN
      + ", " + PRIORITY_COLUMN
      + " from " + JOB_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?"
      + " order by " + JOB_SEQ_COLUMN;

  // Query to delete all jobs.
  private static final String DELETE_ALL_JOBS_QUERY = "delete from "
      + JOB_TABLE;

  // Query to delete a job.
  private static final String DELETE_JOB_QUERY = DELETE_ALL_JOBS_QUERY
      + " where " + JOB_SEQ_COLUMN + " = ?";

  // Query to retrieve all the job statues.
  private static final String GET_JOB_STATUSES_QUERY = "select "
      + JOB_STATUS_SEQ_COLUMN
      + "," + STATUS_NAME_COLUMN
      + " from " + JOB_STATUS_TABLE;

  // Query to retrieve all the job types.
  private static final String GET_JOB_TYPES_QUERY = "select "
      + JOB_TYPE_SEQ_COLUMN
      + "," + TYPE_NAME_COLUMN
      + " from " + JOB_TYPE_TABLE;

  // Query to add a job.
  private static final String INSERT_JOB_QUERY = "insert into "
      + JOB_TABLE
      + "(" + JOB_SEQ_COLUMN
      + "," + JOB_TYPE_SEQ_COLUMN
      + "," + DESCRIPTION_COLUMN
      + "," + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + CREATION_TIME_COLUMN
      + "," + START_TIME_COLUMN
      + "," + END_TIME_COLUMN
      + "," + JOB_STATUS_SEQ_COLUMN
      + "," + STATUS_MESSAGE_COLUMN
      + "," + PRIORITY_COLUMN
      + ") values (default,?,?,?,?,?,?,?,?,?,"
      + "(select coalesce(max(" + PRIORITY_COLUMN + "), 0) + 1"
      + " from " + JOB_TABLE
      + " where " + PRIORITY_COLUMN + " >= 0))";

  // Query to add a job using MySQL.
  private static final String INSERT_JOB_MYSQL_QUERY = "insert into "
      + JOB_TABLE
      + "(" + JOB_SEQ_COLUMN
      + "," + JOB_TYPE_SEQ_COLUMN
      + "," + DESCRIPTION_COLUMN
      + "," + PLUGIN_ID_COLUMN
      + "," + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + CREATION_TIME_COLUMN
      + "," + START_TIME_COLUMN
      + "," + END_TIME_COLUMN
      + "," + JOB_STATUS_SEQ_COLUMN
      + "," + STATUS_MESSAGE_COLUMN
      + "," + PRIORITY_COLUMN
      + ") values (default,?,?,?,?,?,?,?,?,?,?,"
      + "(select next_priority from "
      + "(select coalesce(max(" + PRIORITY_COLUMN + "), 0) + 1 as next_priority"
      + " from " + JOB_TABLE
      + " where " + PRIORITY_COLUMN + " >= 0) as temp_job_table))";

  // Query to find a page of jobs.
  private static final String FIND_NEXT_PAGE_JOBS_QUERY = "select "
      + JOB_SEQ_COLUMN
      + ", " + JOB_TYPE_SEQ_COLUMN
      + ", " + DESCRIPTION_COLUMN
      + ", " + PLUGIN_ID_COLUMN
      + ", " + AU_KEY_COLUMN
      + ", " + CREATION_TIME_COLUMN
      + ", " + START_TIME_COLUMN
      + ", " + END_TIME_COLUMN
      + ", " + JOB_STATUS_SEQ_COLUMN
      + ", " + STATUS_MESSAGE_COLUMN
      + ", " + PRIORITY_COLUMN
      + " from " + JOB_TABLE
      + " where " + JOB_SEQ_COLUMN + " > ?"
      + " order by " + JOB_SEQ_COLUMN;

  // Query to delete inactive jobs.
  private static final String DELETE_INACTIVE_JOBS_QUERY = DELETE_ALL_JOBS_QUERY
	+ " where " + JOB_STATUS_SEQ_COLUMN + " != ?"
	+ " and " + JOB_STATUS_SEQ_COLUMN + " != ?";

  // Query to delete an inactive job.
  private static final String DELETE_INACTIVE_JOB_QUERY = DELETE_JOB_QUERY
	+ " and " + JOB_STATUS_SEQ_COLUMN + " != ?"
	+ " and " + JOB_STATUS_SEQ_COLUMN + " != ?";

  // Query to free incomplete jobs so that they can be started again.
  private static final String FREE_INCOMPLETE_JOBS_QUERY = "update " + JOB_TABLE
	+ " set " + START_TIME_COLUMN + " = null"
	+ ", " + END_TIME_COLUMN + " = null"
	+ ", " + JOB_STATUS_SEQ_COLUMN + " = ?"
	+ ", " + STATUS_MESSAGE_COLUMN + " = '" + INITIAL_JOB_STATUS_MESSAGE
	+ "'"
	+ ", " + OWNER_COLUMN + " = null"
	+ " where " + OWNER_COLUMN + " is not null"
	+ " and " + JOB_STATUS_SEQ_COLUMN + " < ?";

  // Query to select the highest priority job.
  private static final String FIND_HIGHEST_PRIORITY_JOBS_QUERY = "select "
      + JOB_SEQ_COLUMN
      + " from " + JOB_TABLE
      + " where " + OWNER_COLUMN + " is null"
      + " order by " + PRIORITY_COLUMN
      + ", " + JOB_SEQ_COLUMN;

  // Query to claim an unclaimed job.
  private static final String CLAIM_UNCLAIMED_JOB_QUERY = "update "
      + JOB_TABLE
      + " set " + OWNER_COLUMN + " = ?"
      + " where " + OWNER_COLUMN + " is null"
      + " and " + JOB_SEQ_COLUMN + " = ?";

  // Query to retrieve a job type.
  private static final String FIND_JOB_TYPE_QUERY = "select "
      + "jt." + TYPE_NAME_COLUMN
      + " from " + JOB_TABLE + " j"
      + ", " + JOB_TYPE_TABLE + " jt"
      + " where j." + JOB_TYPE_SEQ_COLUMN + " = jt." + JOB_TYPE_SEQ_COLUMN
      + " and j." + JOB_SEQ_COLUMN + " = ?";

  // Query to mark a job as running.
  private static final String MARK_JOB_AS_RUNNING_QUERY = "update "
      + JOB_TABLE
      + " set " + START_TIME_COLUMN + " = ?"
      + ", " + JOB_STATUS_SEQ_COLUMN + " = ?"
      + ", " + STATUS_MESSAGE_COLUMN + " = ?"
      + " where " + JOB_SEQ_COLUMN + " = ?";

  // Query to mark a job as finished.
  private static final String MARK_JOB_AS_FINISHED_QUERY = "update "
      + JOB_TABLE
      + " set " + END_TIME_COLUMN + " = ?"
      + ", " + JOB_STATUS_SEQ_COLUMN + " = ?"
      + ", " + STATUS_MESSAGE_COLUMN + " = ?"
      + " where " + JOB_SEQ_COLUMN + " = ?";

  // Query to find the count of reindexing jobs with a given status.
  private static final String COUNT_REINDEXING_JOBS_BY_STATUS_QUERY = "select"
      + " count(*)"
      + " from " + JOB_TABLE
      + " where " + JOB_TYPE_SEQ_COLUMN + " != ? "
      + " and " + JOB_STATUS_SEQ_COLUMN + " = ?";

  // Query to find the reindexing jobs with a given status.
  private static final String REINDEXING_JOBS_BY_STATUS_QUERY = "select "
      + JOB_SEQ_COLUMN
      + ", " + JOB_TYPE_SEQ_COLUMN
      + ", " + PLUGIN_ID_COLUMN
      + ", " + AU_KEY_COLUMN
      + ", " + START_TIME_COLUMN
      + ", " + END_TIME_COLUMN
      + ", " + PRIORITY_COLUMN
      + ", " + STATUS_MESSAGE_COLUMN
      + " from " + JOB_TABLE
      + " where " + JOB_TYPE_SEQ_COLUMN + " != ?"
      + " and " + JOB_STATUS_SEQ_COLUMN + " = ?";

  // Query to find the reindexing jobs with a given status and started before a
  // given timestamp.
  private static final String REINDEXING_JOBS_BEFORE_BY_STATUS_QUERY = "select "
      + JOB_SEQ_COLUMN
      + ", " + JOB_TYPE_SEQ_COLUMN
      + ", " + PLUGIN_ID_COLUMN
      + ", " + AU_KEY_COLUMN
      + ", " + START_TIME_COLUMN
      + ", " + END_TIME_COLUMN
      + ", " + PRIORITY_COLUMN
      + ", " + STATUS_MESSAGE_COLUMN
      + " from " + JOB_TABLE
      + " where " + JOB_TYPE_SEQ_COLUMN + " != ?"
      + " and " + JOB_STATUS_SEQ_COLUMN + " = ?"
      + " and " + START_TIME_COLUMN + " < ?";

  // Query to update the job queue truncation timestamp.
  private static final String UPDATE_TRUNCATION_TIMESTAMP_QUERY = "update "
      + JOB_METADATA_TABLE
      + " set " + TRUNCATION_TIME_COLUMN + " = ?";

  // Query to get the job queue truncation timestamp.
  private static final String GET_TRUNCATION_TIMESTAMP_QUERY = "select "
      + TRUNCATION_TIME_COLUMN
      + " from " + JOB_METADATA_TABLE;

  // Query to find the count of reindexing jobs.
  private static final String COUNT_REINDEXING_JOBS_QUERY = "select count(*)"
      + " from " + JOB_TABLE
      + " where " + JOB_TYPE_SEQ_COLUMN + " != ? ";

  // Query to find the count of reindexing jobs with a given status and message.
  private static final String
  COUNT_REINDEXING_JOBS_BY_STATUS_AND_MESSAGE_QUERY = "select count(*)"
      + " from " + JOB_TABLE
      + " where " + JOB_TYPE_SEQ_COLUMN + " != ? "
      + " and " + JOB_STATUS_SEQ_COLUMN + " = ?"
      + " and " + STATUS_MESSAGE_COLUMN + " = ?";

  private final JobDbManager dbManager;
  private final Map<String, Long> jobStatusSeqByName;
  private final Map<String, Long> jobTypeSeqByName;

  /**
   * Constructor.
   */
  JobManagerSql(JobDbManager dbManager) throws DbException {
    this.dbManager = dbManager;

    // Get a connection to the database.
    Connection conn = dbManager.getConnection();

    try {
      jobStatusSeqByName = mapJobStatusByName(conn);
      jobTypeSeqByName = mapJobTypeByName(conn);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Populates the map of job status database identifiers by name.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Long> with the map of job status database identifiers
   *         by name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Map<String, Long> mapJobStatusByName(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "mapJobStatusByName(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");
    Map<String, Long> result = new HashMap<String, Long>();

    PreparedStatement stmt =
	dbManager.prepareStatement(conn, GET_JOB_STATUSES_QUERY);
    ResultSet resultSet = null;

    try {
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the job statuses.
      while (resultSet.next()) {
  	Long jobStatusSeq = resultSet.getLong(JOB_STATUS_SEQ_COLUMN);
  	if (log.isDebug3())
  	  log.debug3(DEBUG_HEADER + "jobStatusSeq = " + jobStatusSeq);

  	String statusName = resultSet.getString(STATUS_NAME_COLUMN);
  	if (log.isDebug3())
  	  log.debug3(DEBUG_HEADER + "statusName = " + statusName);

  	result.put(statusName, jobStatusSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the job statuses";
      log.error(message, sqle);
      log.error("SQL = '" + GET_JOB_STATUSES_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the job statuses";
      log.error(message, dbe);
      log.error("SQL = '" + GET_JOB_STATUSES_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    return result;
  }

  /**
   * Populates the map of job type database identifiers by name.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Long> with the map of job type database identifiers
   *         by name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Map<String, Long> mapJobTypeByName(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "mapJobTypeByName(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");
    Map<String, Long> result = new HashMap<String, Long>();

    PreparedStatement stmt =
	dbManager.prepareStatement(conn, GET_JOB_TYPES_QUERY);
    ResultSet resultSet = null;

    try {
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the job statuses.
      while (resultSet.next()) {
  	Long jobTypeSeq = resultSet.getLong(JOB_TYPE_SEQ_COLUMN);
  	if (log.isDebug3())
  	  log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);

  	String typeName = resultSet.getString(TYPE_NAME_COLUMN);
  	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "typeName = " + typeName);

  	result.put(typeName, jobTypeSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the job statuses";
      log.error(message, sqle);
      log.error("SQL = '" + GET_JOB_TYPES_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the job statuses";
      log.error(message, dbe);
      log.error("SQL = '" + GET_JOB_TYPES_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    return result;
  }

  /**
   * Creates a job to extract and store the metadata of an Archival Unit.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @param needFullReindex
   *          A boolean with the indication of whether a full extraction is to
   *          be performed or not.
   * @return a JobAuStatus with the created metadata extraction job properties.
   * @throws IllegalArgumentException
   *           if the Archival Unit does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  JobAuStatus createMetadataExtractionJob(String auId,
      boolean needFullReindex) throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "createMetadataExtractionJob(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auId = " + auId);
      log.debug2(DEBUG_HEADER + "needFullReindex = " + needFullReindex);
    }

    JobAuStatus result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Create the Archival Unit metadata extraction job.
      result = createMetadataExtractionJob(conn, auId, needFullReindex);

      JobDbManager.commitOrRollback(conn, log);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Creates a job to extract and store the metadata of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @param needFullReindex
   *          A boolean with the indication of whether a full re-indexing is to
   *          be performed or not.
   * @return a JobAuStatus with the created metadata extraction job properties.
   * @throws IllegalArgumentException
   *           if the Archival Unit does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private JobAuStatus createMetadataExtractionJob(Connection conn,
      String auId, boolean needFullReindex)
      throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "createMetadataExtractionJob(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auId = " + auId);
      log.debug2(DEBUG_HEADER + "needFullReindex = " + needFullReindex);
    }

    JobAuStatus result = null;

    // Get any existing jobs for the Archival Unit.
    List<JobAuStatus> auJobs = getAuJobs(conn, auId);

    // Loop through all the Archival Unit jobs.
    for (JobAuStatus job : auJobs) {
      Long jobTypeSeq = job.getType();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);

      // Check whether it's a job extracting the metadata of the Archival Unit
      // and it is of the same type.
      if ((jobTypeSeqByName.get(JOB_TYPE_PUT_AU).equals(jobTypeSeq)
	  && needFullReindex) ||
	  (jobTypeSeqByName.get(JOB_TYPE_PUT_INCREMENTAL_AU).equals(jobTypeSeq)
	      && !needFullReindex)) {
	// Yes: Get its status.
	Long jobStatusSeq = (long)job.getStatusCode();

	// Check whether it has not been started.
	if (jobStatusSeqByName.get(JOB_STATUS_CREATED).equals(jobStatusSeq)) {
	  // Yes: Do not create a new job: Reuse the existing one.
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Reusing job = " + job);
	  result = job;
	} else {
	  // No: Try to delete it.
	  boolean deletedPutJob =
	      deleteInactiveJob(conn, Long.valueOf(job.getId()));
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "deletedPutJob = " + deletedPutJob);

	  // Check whether it could not be deleted.
	  if (!deletedPutJob) {
	    // Yes: Do not create a new job: Reuse the existing one.
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "Reusing job = " + job);
	    result = job;
	  } else {
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "New job needed");
	  }
	}
      } else if (jobTypeSeqByName.get(JOB_TYPE_DELETE_AU).equals(jobTypeSeq) ||
	  (jobTypeSeqByName.get(JOB_TYPE_PUT_AU).equals(jobTypeSeq)
	      && !needFullReindex) ||
	  (jobTypeSeqByName.get(JOB_TYPE_PUT_INCREMENTAL_AU).equals(jobTypeSeq)
	      && needFullReindex)) {
	// Yes: Try to delete it.
	boolean deletedJob =
	    deleteInactiveJob(conn, Long.valueOf(job.getId()));
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "deletedJob = " + deletedJob);
      }
    }

    // Check whether no job can be reused.
    if (result == null) {
      // Yes: Create the job.
      String jobType =
	  needFullReindex ? JOB_TYPE_PUT_AU : JOB_TYPE_PUT_INCREMENTAL_AU;
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jobType = " + jobType);

      String jobTypeLabel = needFullReindex ? "Full Metadata Extraction"
	  : "Incremental Metadata Extraction";
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "jobTypeLabel = " + jobTypeLabel);

      Long jobSeq = addJob(conn, jobTypeSeqByName.get(jobType), jobTypeLabel,
	  auId, new Date().getTime(), null, null,
	  jobStatusSeqByName.get(JOB_STATUS_CREATED),
	  INITIAL_JOB_STATUS_MESSAGE);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jobSeq = " + jobSeq);

      result = getJob(conn, jobSeq);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the properties of a job.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the job identifier.
   * @return a JobAuStatus with the properties of the job.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private JobAuStatus getJob(Connection conn, Long jobSeq) throws DbException {
    final String DEBUG_HEADER = "getJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    JobAuStatus job = null;

    PreparedStatement findJob =
	dbManager.prepareStatement(conn, FIND_JOB_QUERY);

    ResultSet resultSet = null;

    try {
      // Get the job.
      findJob.setLong(1, jobSeq);
      resultSet = dbManager.executeQuery(findJob);

      if (resultSet.next()) {
	job = getJobFromResultSet(resultSet);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Found job " + job);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU jobs";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_JOB_QUERY + "'.");
      log.error("jobSeq = " + jobSeq);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot find AU jobs";
      log.error(message, dbe);
      log.error("SQL = '" + FIND_JOB_QUERY + "'.");
      log.error("jobSeq = " + jobSeq);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(findJob);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "job = " + job);
    return job;
  }

  /**
   * Populates the data of a job with a result set.
   * 
   * @param resultSet
   *          A ResultSet with the source of the data.
   * @return a JobAuStatus with the resulting job data.
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  private JobAuStatus getJobFromResultSet(ResultSet resultSet)
      throws SQLException {
    JobAuStatus job = new JobAuStatus();

    job.setId(String.valueOf(resultSet.getLong(JOB_SEQ_COLUMN)));
    job.setType(resultSet.getLong(JOB_TYPE_SEQ_COLUMN));

    String description = resultSet.getString(DESCRIPTION_COLUMN);

    if (!resultSet.wasNull()) {
      job.setDescription(description);
    }

    job.setCreationDate(new Date(resultSet.getLong(CREATION_TIME_COLUMN)));

    Long startTime = resultSet.getLong(START_TIME_COLUMN);

    if (!resultSet.wasNull()) {
      job.setStartDate(new Date(startTime));
    }

    Long endTime = resultSet.getLong(END_TIME_COLUMN);

    if (!resultSet.wasNull()) {
      job.setEndDate(new Date(endTime));
    }

    String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);

    if (pluginId != null && pluginId.trim().length() > 0) {
      String auKey = resultSet.getString(AU_KEY_COLUMN);

      if (auKey != null && auKey.trim().length() > 0) {
	job.setAuId(PluginManager.generateAuId(pluginId, auKey));
      }
    }

    job.setStatusCode((int)resultSet.getLong(JOB_STATUS_SEQ_COLUMN));

    String statusMessage = resultSet.getString(STATUS_MESSAGE_COLUMN);

    if (!resultSet.wasNull()) {
      job.setStatusMessage(statusMessage);
    }

    return job;
  }

  /**
   * Provides the jobs of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a List<JobAuStatus> with the jobs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private List<JobAuStatus> getAuJobs(Connection conn, String auId)
      throws DbException {
    final String DEBUG_HEADER = "getAuJobs(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    List<JobAuStatus> jobs = new ArrayList<JobAuStatus>();

    String pluginId = null;
    String auKey = null;
    PreparedStatement findJobs =
	dbManager.prepareStatement(conn, FIND_AU_JOB_QUERY);

    ResultSet resultSet = null;
    JobAuStatus job;

    try {

      // Get the Archival Unit jobs.
      pluginId = PluginManager.pluginIdFromAuId(auId);
      findJobs.setString(1, pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      findJobs.setString(2, auKey);

      resultSet = dbManager.executeQuery(findJobs);

      // Loop through the results.
      while (resultSet.next()) {
	// Get the next job.
	job = getJobFromResultSet(resultSet);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Found job " + job);

	// Add it to the results.
	jobs.add(job);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU jobs";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_AU_JOB_QUERY + "'.");
      log.error("auId = " + auId);
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot find AU jobs";
      log.error(message, dbe);
      log.error("SQL = '" + FIND_AU_JOB_QUERY + "'.");
      log.error("auId = " + auId);
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(findJobs);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobs = " + jobs);
    return jobs;
  }

  /**
   * Adds a job to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobTypeSeq
   *          A Long with the database identifier of the type of the job to be
   *          added.
   * @param description
   *          A String with the description of the job to be added.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @param jobStatusSeq
   *          A Long with the database identifier of the status of the job to be
   *          added.
   * @param statusMessage
   *          A String with the message of the status of the job to be added.
   * @return a Long with the database identifier of the created job.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long addJob(Connection conn, Long jobTypeSeq, String description, String auId,
      long creationTime, Long startTime, Long endTime, Long jobStatusSeq,
      String statusMessage) throws DbException {
    final String DEBUG_HEADER = "addJob(): ";
    if (log.isDebug2()) {
      log.debug(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);
      log.debug(DEBUG_HEADER + "description = " + description);
      log.debug(DEBUG_HEADER + "auId = " + auId);
      log.debug(DEBUG_HEADER + "creationTime = " + creationTime);
      log.debug(DEBUG_HEADER + "startTime = " + startTime);
      log.debug(DEBUG_HEADER + "endTime = " + endTime);
      log.debug(DEBUG_HEADER + "jobStatusSeq = " + jobStatusSeq);
      log.debug(DEBUG_HEADER + "statusMessage = " + statusMessage);
    }

    String pluginId = null;
    String auKey = null;
    ResultSet resultSet = null;
    Long jobSeq = null;
    PreparedStatement createJob = null;

    if (dbManager.isTypeMysql()) {
      	createJob = dbManager.prepareStatement(conn, INSERT_JOB_MYSQL_QUERY,
      	    Statement.RETURN_GENERATED_KEYS);
    } else {
      createJob = dbManager.prepareStatement(conn, INSERT_JOB_QUERY,
	  Statement.RETURN_GENERATED_KEYS);
    }

    try {
      // skip auto-increment key field #0
      createJob.setLong(1, jobTypeSeq);

      if (description != null) {
	createJob.setString(2,
	    JobDbManager.truncateVarchar(description, MAX_DESCRIPTION_COLUMN));
      } else {
	createJob.setNull(2, VARCHAR);
      }

      pluginId = PluginManager.pluginIdFromAuId(auId);
      createJob.setString(3, pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      createJob.setString(4, auKey);

      createJob.setLong(5, creationTime);

      if (startTime != null) {
	createJob.setLong(6, startTime.longValue());
      } else {
	createJob.setNull(6, BIGINT);
      }

      if (endTime != null) {
	createJob.setLong(7, endTime.longValue());
      } else {
	createJob.setNull(7, BIGINT);
      }

      createJob.setLong(8, jobStatusSeq);

      if (statusMessage != null) {
	createJob.setString(9, JobDbManager.truncateVarchar(statusMessage,
	    MAX_STATUS_MESSAGE_COLUMN));
      } else {
	createJob.setNull(9, VARCHAR);
      }

      dbManager.executeUpdate(createJob);
      resultSet = createJob.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create Job table row for auId " + auId);
	return null;
      }

      jobSeq = resultSet.getLong(1);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Added jobSeq = " + jobSeq);
    } catch (SQLException sqle) {
      String message = "Cannot add job";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_JOB_QUERY + "'.");
      log.error("jobTypeSeq = " + jobTypeSeq);
      log.error("description = " + description);
      log.error("auId = " + auId);
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
      log.error("creationTime = " + creationTime);
      log.error("startTime = " + startTime);
      log.error("endTime = " + endTime);
      log.error("jobStatusSeq = " + jobStatusSeq);
      log.error("statusMessage = " + statusMessage);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot add job";
      log.error(message, dbe);
      log.error("SQL = '" + INSERT_JOB_QUERY + "'.");
      log.error("jobTypeSeq = " + jobTypeSeq);
      log.error("description = " + description);
      log.error("auId = " + auId);
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
      log.error("creationTime = " + creationTime);
      log.error("startTime = " + startTime);
      log.error("endTime = " + endTime);
      log.error("jobStatusSeq = " + jobStatusSeq);
      log.error("statusMessage = " + statusMessage);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(createJob);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);
    return jobSeq;
  }

  /**
   * Creates a job to remove the metadata of an Archival Unit.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a JobAuStatus with the created metadata removal job properties.
   * @throws IllegalArgumentException
   *           if the Archival Unit does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  JobAuStatus createMetadataRemovalJob(String auId)
      throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "createMetadataRemovalJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    JobAuStatus result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Create the Archival Unit metadata removal job.
      result = createMetadataRemovalJob(conn, auId);

      JobDbManager.commitOrRollback(conn, log);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Creates a job to remove the metadata of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a JobAuStatus with the created metadata removal job properties.
   * @throws IllegalArgumentException
   *           if the Archival Unit does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private JobAuStatus createMetadataRemovalJob(Connection conn, String auId)
      throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "createMetadataRemovalJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    JobAuStatus result = null;

    // Get any existing jobs for the Archival Unit.
    List<JobAuStatus> auJobs = getAuJobs(conn, auId);

    // Loop through all the Archival Unit jobs.
    for (JobAuStatus job : auJobs) {
      Long jobTypeSeq = job.getType();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);

      // Check whether it's a job deleting the metadata of the Archival Unit.
      if (jobTypeSeqByName.get(JOB_TYPE_DELETE_AU).equals(jobTypeSeq)) {
	// Yes: Get its status.
	Long jobStatusSeq = (long)job.getStatusCode();

	// Check whether it has not been started.
	if (jobStatusSeqByName.get(JOB_STATUS_CREATED).equals(jobStatusSeq)) {
	  // Yes: Do not create a new job: Reuse the existing one.
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Reusing job = " + job);
	  result = job;
	} else {
	  // No: Try to delete it.
	  boolean deletedDeleteJob =
	      deleteInactiveJob(conn, Long.valueOf(job.getId()));
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "deletedDeleteJob = " + deletedDeleteJob);

	  // Check whether it could not be deleted.
	  if (!deletedDeleteJob) {
	    // Yes: Do not create a new job: Reuse the existing one.
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "Reusing job = " + job);
	    result = job;
	  } else {
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "New job needed");
	  }
	}
      } else if (jobTypeSeqByName.get(JOB_TYPE_PUT_AU).equals(jobTypeSeq)
	  || jobTypeSeqByName.get(JOB_TYPE_PUT_INCREMENTAL_AU)
	  .equals(jobTypeSeq)) {
	// Yes: Try to delete it.
	boolean deletedPutJob =
	    deleteInactiveJob(conn, Long.valueOf(job.getId()));
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "deletedPutJob = " + deletedPutJob);
      }
    }

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "result = " + result);

    // Check whether no job can be reused.
    if (result == null) {
      // Yes: Create the job.
      Long jobSeq = addJob(conn, jobTypeSeqByName.get(JOB_TYPE_DELETE_AU),
	  "Metadata Removal", auId, new Date().getTime(), null, null,
	  jobStatusSeqByName.get(JOB_STATUS_CREATED),
	  INITIAL_JOB_STATUS_MESSAGE);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jobSeq = " + jobSeq);

      result = getJob(conn, jobSeq);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides an Archival Unit job.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a JobAuStatus with the created metadata removal job properties.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  JobAuStatus getAuJob(String auId) throws DbException {
    final String DEBUG_HEADER = "getAuJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    JobAuStatus result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the Archival Unit job.
      result = getAuJob(conn, auId);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides an Archival Unit job.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a JobAuStatus with the created metadata removal job properties.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  JobAuStatus getAuJob(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "getAuJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    JobAuStatus result = null;

    // Get any existing jobs for the Archival Unit.
    List<JobAuStatus> auJobs = getAuJobs(conn, auId);

    // Loop through all the Archival Unit jobs.
    for (JobAuStatus job : auJobs) {
      Long jobTypeSeq = job.getType();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);

      // Check whether it's a job extracting the metadata of the Archival Unit.
      if (jobTypeSeqByName.get(JOB_TYPE_PUT_AU).equals(jobTypeSeq)
	  || jobTypeSeqByName.get(JOB_TYPE_PUT_INCREMENTAL_AU)
	  .equals(jobTypeSeq)) {
	// Yes: Return it.
	result = job;
	break;
	// No: Check whether it's a job deleting the metadata of the Archival
	// Unit.
      } else if (jobTypeSeqByName.get(JOB_TYPE_DELETE_AU).equals(jobTypeSeq)) {
	// Yes: Return it if there is no metadata extraction job.
	result = job;
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides a list of all currently active jobs or a pageful of the list
   * defined by the continuation token and size.
   * 
   * @param limit
   *          An Integer with the maximum number of jobs to be returned.
   * @param continuationToken
   *          A JobContinuationToken with the pagination token, if any.
   * @return a JobPage with the requested list of jobs.
   * @throws ConcurrentModificationException
   *           if there is a conflict between the pagination request and the
   *           current content in the database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  JobPage getJobs(Integer limit, JobContinuationToken continuationToken)
      throws DbException {
    final String DEBUG_HEADER = "getJobs(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "limit = " + limit);
      log.debug2(DEBUG_HEADER + "continuationToken = " + continuationToken);
    }

    JobPage result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the job queue last truncation timestamp.
      long truncationTime = getJobQueueTruncationTimestamp(conn);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "truncationTime = " + truncationTime);

      // Get the continuation token members, if any.
      Long queueTruncationTimestamp = null;
      Long lastJobSeq = null;

      if (continuationToken != null) {
	queueTruncationTimestamp =
	    continuationToken.getQueueTruncationTimestamp();
	lastJobSeq = continuationToken.getLastJobSeq();
      }

      // Evaluate the pagination consistency of the request.
      if (queueTruncationTimestamp != null
	  && queueTruncationTimestamp.longValue() != truncationTime) {
	String message = "Incompatible pagination request: request timestamp: "
	    + queueTruncationTimestamp
	    + ", current timestamp:" + " " + truncationTime;
	log.warning(message);
	throw new ConcurrentModificationException("Incompatible pagination for "
	    + "jobs: Content has changed since previous request");
      }

      // Get the jobs.
      result = getJobs(conn, truncationTime, limit, lastJobSeq);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides a list of all currently active jobs or a pageful of the list
   * defined by the continuation token and size.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param queueTruncationTimestamp
   *          A Long with the job queue last truncation timestamp.
   * @param limit
   *          An Integer with the maximum number of jobs to be returned.
   * @param lastJobSeq
   *          A Long with the last job database identifier.
   * @return a JobPage with the requested list of jobs.
   * @throws ConcurrentModificationException
   *           if there is a conflict between the pagination request and the
   *           current content in the database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private JobPage getJobs(Connection conn, Long queueTruncationTimestamp,
      Integer limit, Long lastJobSeq) throws DbException {
    final String DEBUG_HEADER = "getJobs(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER
	  + "queueTruncationTimestamp = " + queueTruncationTimestamp);
      log.debug2(DEBUG_HEADER + "limit = " + limit);
      log.debug2(DEBUG_HEADER + "lastJobSeq = " + lastJobSeq);
    }

    JobPage result = new JobPage();
    List<Job> jobs = new ArrayList<Job>();
    result.setJobs(jobs);

    Job job;
    String sql = FIND_NEXT_PAGE_JOBS_QUERY;

    PreparedStatement findJobs = dbManager.prepareStatement(conn, sql);
    ResultSet resultSet = null;

    // Indication of whether there are more results that may be requested in a
    // subsequent pagination request.
    boolean hasMore = false;

    try {
      // Handle the first page of requested results.
      if (lastJobSeq == null) {
	findJobs.setLong(1, -1);
      } else {
	findJobs.setLong(1, lastJobSeq);
      }

      boolean isPaginating = false;

      // Determine whether this is a paginating request.
      if (limit != null && limit.intValue() > 0) {
	// Yes.
	isPaginating = true;

	// Request one more row than needed, to determine whether there will be
	// additional results besides this requested page.
	findJobs.setMaxRows(limit + 1);
      }

      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "isPaginating = " + isPaginating);

      resultSet = dbManager.executeQuery(findJobs);

      // Loop through the results.
      while (resultSet.next()) {
	// Check whether the full requested page has already been obtained.
	if (isPaginating && jobs.size() == limit) {
	  // Yes: There are more results after this page.
	  hasMore = true;
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "hasMore = " + hasMore);

	  // Do not process the additional row.
	  break;
	}

	// Get the next job.
	job = new Job(getJobFromResultSet(resultSet));
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Found job " + job);

	// Add it to the results.
	jobs.add(job);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find jobs";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      log.error("limit = " + limit);
      log.error("lastJobSeq = " + lastJobSeq);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot find jobs";
      log.error(message, dbe);
      log.error("SQL = '" + sql + "'.");
      log.error("limit = " + limit);
      log.error("lastJobSeq = " + lastJobSeq);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(findJobs);
    }

    // Check whether there are additional items after this page.
    if (hasMore) {
      // Yes: Build and save the response continuation token.
      JobContinuationToken continuationToken = new JobContinuationToken(
	  queueTruncationTimestamp,
	  Long.valueOf(jobs.get(jobs.size()-1).getId()));
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "continuationToken = " + continuationToken);

      result.setContinuationToken(continuationToken);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    return result;
  }

  /**
   * Deletes from the database all inactive jobs.
   * 
   * @return an int with the count of jobs deleted.
   * @throws Exception
   *           if there are problems deleting the jobs.
   */
  int deleteAllInactiveJobs() throws Exception {
    final String DEBUG_HEADER = "deleteAllInactiveJobs(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    int deletedCount = -1;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      deletedCount = deleteInactiveJobs(conn);
      updateJobQueueTruncationTimestamp(conn);
      JobDbManager.commitOrRollback(conn, log);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "deletedCount = " + deletedCount);
    return deletedCount;
  }

  /**
   * Deletes from the database a job given the job identifier.
   * 
   * @param jobSeq
   *          A Long with the job database identifier.
   * @return a JobAuStatus with the details of the deleted job.
   * @throws IllegalArgumentException
   *           if the job does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  JobAuStatus deleteJob(Long jobSeq)
      throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "deleteJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    JobAuStatus job = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      job = deleteJob(conn, jobSeq);
      JobDbManager.commitOrRollback(conn, log);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "job = " + job);
    return job;
  }

  /**
   * Deletes from the database a job given the job identifier.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the job database identifier.
   * @return a JobAuStatus with the details of the deleted job.
   * @throws IllegalArgumentException
   *           if the job does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private JobAuStatus deleteJob(Connection conn, Long jobSeq)
      throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "deleteJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    String message = "Cannot find job for jobSeq " + jobSeq + "'";
    JobAuStatus job = null;

    // Try to delete it.
    boolean deleted = false;

    try {
      job = getJob(conn, jobSeq);

      if (job == null) {
	log.error(message);
	throw new IllegalArgumentException(message);
      }

      message = "Cannot delete job for jobSeq " + jobSeq + "'";
      deleted = deleteInactiveJob(conn, jobSeq);
    } catch (DbException dbe) {
      log.error(message, dbe);
      throw dbe;
    }
    
    if (deleted) {
      job.setStatusCode(
	  jobStatusSeqByName.get(JOB_STATUS_DELETED).intValue());
      job.setEndDate(new Date());
      job.setStatusMessage("Deleted");
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "job = " + job);
    return job;
  }

  /**
   * Deletes all inactive jobs (not running and not terminating).
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return an int with the count of jobs deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int deleteInactiveJobs(Connection conn) throws DbException {
    final String DEBUG_HEADER = "deleteInactiveJobs(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    int deletedCount = -1;
    String message = "Cannot delete all inactive jobs";
    PreparedStatement deleteJob =
	dbManager.prepareStatement(conn, DELETE_INACTIVE_JOBS_QUERY);

    try {
      deleteJob.setLong(1, jobStatusSeqByName.get(JOB_STATUS_RUNNING));
      deleteJob.setLong(2, jobStatusSeqByName.get(JOB_STATUS_TERMINATING));

      deletedCount = dbManager.executeUpdate(deleteJob);
    } catch (DbException dbe) {
      log.error(message, dbe);
      log.error("SQL = '" + DELETE_INACTIVE_JOBS_QUERY + "'.");
      throw dbe;
    } catch (SQLException sqle) {
      log.error(message, sqle);
      log.error("SQL = '" + DELETE_INACTIVE_JOBS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      JobDbManager.safeCloseStatement(deleteJob);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "deletedCount = " + deletedCount);
    return deletedCount;
  }

  /**
   * Deletes an inactive job (not running and not terminating).
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the job identifier.
   * @return a boolean with <code>true</code> if the job was deleted,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private boolean deleteInactiveJob(Connection conn, Long jobSeq)
      throws DbException {
    final String DEBUG_HEADER = "deleteInactiveJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    int deletedCount = -1;
    PreparedStatement deleteJob =
	dbManager.prepareStatement(conn, DELETE_INACTIVE_JOB_QUERY);

    try {
      deleteJob.setLong(1, jobSeq);
      deleteJob.setLong(2, jobStatusSeqByName.get(JOB_STATUS_RUNNING));
      deleteJob.setLong(3, jobStatusSeqByName.get(JOB_STATUS_TERMINATING));

      deletedCount = dbManager.executeUpdate(deleteJob);
    } catch (SQLException sqle) {
      String message = "Cannot delete job";
      log.error(message, sqle);
      log.error("jobSeq = " + jobSeq);
      log.error("SQL = '" + DELETE_INACTIVE_JOB_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot delete job";
      log.error(message, dbe);
      log.error("jobSeq = " + jobSeq);
      log.error("SQL = '" + DELETE_INACTIVE_JOB_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseStatement(deleteJob);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "result = " + (deletedCount > 0));
    return deletedCount > 0;
  }

  /**
   * Provides the properties of a job.
   * 
   * @param jobId
   *          A String with the job database identifier.
   * @return a JobAuStatus with the details of the removed job.
   * @throws IllegalArgumentException
   *           if the job does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  JobAuStatus getJob(String jobId)
      throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "getJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobId = " + jobId);

    JobAuStatus result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      result = getJob(conn, jobId);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the properties of a job.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobId
   *          A String with the job database identifier.
   * @return a JobAuStatus with the details of the removed job.
   * @throws IllegalArgumentException
   *           if the job does not exist.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private JobAuStatus getJob(Connection conn, String jobId)
      throws IllegalArgumentException, DbException {
    final String DEBUG_HEADER = "getJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobId = " + jobId);

    String message = "Cannot find job for jobId " + jobId + "'";
    Long jobSeq = null;
    JobAuStatus job = null;

    try {
      jobSeq = Long.valueOf(jobId);
      job = getJob(conn, jobSeq);
    } catch (NumberFormatException nfe) {
      log.error(message, nfe);
      throw new IllegalArgumentException(message + ": Not numeric jobId");
    }

    if (job == null) {
      String message2 = "No job found for jobId " + jobId + "'";
      log.error(message2);
      throw new IllegalArgumentException(message2);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "job = " + job);
    return job;
  }

  /**
   * Frees any claimed jobs that have not been finished.
   * 
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void freeIncompleteJobs() throws DbException {
    final String DEBUG_HEADER = "freeIncompleteJobs(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      freeIncompleteJobs(conn);
      JobDbManager.commitOrRollback(conn, log);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Frees any claimed jobs that have not been finished.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void freeIncompleteJobs(Connection conn) throws DbException {
    final String DEBUG_HEADER = "freeIncompleteJobs(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    int updatedCount = -1;

    PreparedStatement freeIncompleteJobs =
	dbManager.prepareStatement(conn, FREE_INCOMPLETE_JOBS_QUERY);

    try {
      freeIncompleteJobs.setLong(1, jobStatusSeqByName.get(JOB_STATUS_CREATED));
      freeIncompleteJobs.setLong(2,
	  jobStatusSeqByName.get(JOB_STATUS_TERMINATED));

      updatedCount = dbManager.executeUpdate(freeIncompleteJobs);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "updatedCount = " + updatedCount);
    } catch (SQLException sqle) {
      String message = "Cannot free incomplete jobs";
      log.error(message, sqle);
      log.error("SQL = '" + FREE_INCOMPLETE_JOBS_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot free incomplete jobs";
      log.error(message, dbe);
      log.error("SQL = '" + FREE_INCOMPLETE_JOBS_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseStatement(freeIncompleteJobs);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Claims the next job for a task.
   * 
   * @param owner
   *          A String with the name of the task claiming its next job.
   * @return a Long with the database identifier of the job that has been
   *         claimed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long claimNextJob(String owner) throws DbException {
    final String DEBUG_HEADER = "claimNextJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "owner = " + owner);

    Long jobSeq = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      jobSeq = claimNextJob(conn, owner);
      JobDbManager.commitOrRollback(conn, log);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);
    return jobSeq;
  }

  /**
   * Claims the next job for a task.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param owner
   *          A String with the name of the task claiming its next job.
   * @return a Long with the database identifier of the job that has been
   *         claimed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long claimNextJob(Connection conn, String owner) throws DbException {
    final String DEBUG_HEADER = "claimNextJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "owner = " + owner);

    Long jobSeq = null;
    boolean claimed = false;

    while (!claimed) {
      jobSeq = findHighestPriorityUnclaimedJob(conn);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jobSeq = " + jobSeq);

      if (jobSeq == null) {
	break;
      }

      claimed = claimUnclaimedJob(conn, owner, jobSeq);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "claimed = " + claimed);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);
    return jobSeq;
  }

  /**
   * Provides the unclaimed job with the highest priority.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Long with the database identifier of the unclaimed job with the
   *         highest priority.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findHighestPriorityUnclaimedJob(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "findHighestPriorityUnclaimedJob(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    Long jobSeq = null;

    PreparedStatement findHighestPriorityJob =
	dbManager.prepareStatement(conn, FIND_HIGHEST_PRIORITY_JOBS_QUERY);

    ResultSet resultSet = null;

    try {
      // Request the single top job.
      findHighestPriorityJob.setMaxRows(1);

      resultSet = dbManager.executeQuery(findHighestPriorityJob);

      // Get the results.
      if (resultSet.next()) {
	jobSeq = resultSet.getLong(JOB_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jobSeq = " + jobSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find the highest priority unclaimed job";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_HIGHEST_PRIORITY_JOBS_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot find the highest priority unclaimed job";
      log.error(message, dbe);
      log.error("SQL = '" + FIND_HIGHEST_PRIORITY_JOBS_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(findHighestPriorityJob);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);
    return jobSeq;
  }

  /**
   * Claims an unclaimed job for a task.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param owner
   *          A String with the name of the task claiming its next job.
   * @param jobSeq
   *          A Long with the database identifier of the job being claimed.
   * @return <code>true</code> if the job was actually claimed,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean claimUnclaimedJob(Connection conn, String owner, Long jobSeq)
      throws DbException {
    final String DEBUG_HEADER = "claimUnclaimedJob(): ";
    if (log.isDebug2()) {
      log.debug(DEBUG_HEADER + "owner = " + owner);
      log.debug(DEBUG_HEADER + "jobSeq = " + jobSeq);
    }

    int updatedCount = -1;

    PreparedStatement claimUnclaimedJob =
	dbManager.prepareStatement(conn, CLAIM_UNCLAIMED_JOB_QUERY);

    try {
      claimUnclaimedJob.setString(1,
	  JobDbManager.truncateVarchar(owner, MAX_OWNER_COLUMN));
      claimUnclaimedJob.setLong(2, jobSeq);
      updatedCount = dbManager.executeUpdate(claimUnclaimedJob);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "updatedCount = " + updatedCount);
    } catch (SQLException sqle) {
      String message = "Cannot claim unclaimed job";
      log.error(message, sqle);
      log.error("owner = '" + owner + "'.");
      log.error("jobSeq = " + jobSeq);
      log.error("SQL = '" + CLAIM_UNCLAIMED_JOB_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot claim unclaimed job";
      log.error(message, dbe);
      log.error("owner = '" + owner + "'.");
      log.error("jobSeq = " + jobSeq);
      log.error("SQL = '" + CLAIM_UNCLAIMED_JOB_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseStatement(claimUnclaimedJob);
    }

    if (log.isDebug2())
      log.debug(DEBUG_HEADER + "updatedCount == 1 = " + (updatedCount == 1));
    return updatedCount == 1;
  }

  /**
   * Provides the type of a job.
   * 
   * @param jobSeq
   *          A Long with the job identifier.
   * @return a String with the job type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  String getJobType(Long jobSeq) throws DbException {
    final String DEBUG_HEADER = "getJobType(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    String result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      result = getJobType(conn, jobSeq);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the type of a job.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the job identifier.
   * @return a String with the job type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String getJobType(Connection conn, Long jobSeq) throws DbException {
    final String DEBUG_HEADER = "getJobType(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    String jobType = null;

    PreparedStatement findJob =
	dbManager.prepareStatement(conn, FIND_JOB_TYPE_QUERY);

    ResultSet resultSet = null;

    try {
      // Get the job type.
      findJob.setLong(1, jobSeq);
      resultSet = dbManager.executeQuery(findJob);

      if (resultSet.next()) {
	jobType = resultSet.getString(TYPE_NAME_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jobType " + jobType);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find Archival Unit jobs";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_JOB_TYPE_QUERY + "'.");
      log.error("jobSeq = " + jobSeq);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot find Archival Unit jobs";
      log.error(message, dbe);
      log.error("SQL = '" + FIND_JOB_TYPE_QUERY + "'.");
      log.error("jobSeq = " + jobSeq);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(findJob);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobType = " + jobType);
    return jobType;
  }

  /**
   * Marks a job as running.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the database identifier of the job being claimed.
   * @param statusMessage
   *          A String with the status message.
   * @return an int with the count of jobs updated.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int markJobAsRunning(Connection conn, Long jobSeq, String statusMessage)
      throws DbException {
    final String DEBUG_HEADER = "markJobAsRunning(): ";
    if (log.isDebug2()) {
      log.debug(DEBUG_HEADER + "jobSeq = " + jobSeq);
      log.debug(DEBUG_HEADER + "statusMessage = " + statusMessage);
    }

    int updatedCount = -1;

    PreparedStatement updateJobStatus =
	dbManager.prepareStatement(conn, MARK_JOB_AS_RUNNING_QUERY);

    try {
      updateJobStatus.setLong(1, new Date().getTime());
      updateJobStatus.setLong(2, jobStatusSeqByName.get(JOB_STATUS_RUNNING));
      updateJobStatus.setString(3, JobDbManager.truncateVarchar(statusMessage,
	  MAX_STATUS_MESSAGE_COLUMN));
      updateJobStatus.setLong(4, jobSeq);
      updatedCount = dbManager.executeUpdate(updateJobStatus);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "updatedCount = " + updatedCount);
    } catch (SQLException sqle) {
      String message = "Cannot update job status";
      log.error(message, sqle);
      log.error("jobSeq = " + jobSeq);
      log.error("statusMessage = '" + statusMessage + "'.");
      log.error("SQL = '" + MARK_JOB_AS_RUNNING_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot update job status";
      log.error(message, dbe);
      log.error("jobSeq = " + jobSeq);
      log.error("statusMessage = '" + statusMessage + "'.");
      log.error("SQL = '" + MARK_JOB_AS_RUNNING_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseStatement(updateJobStatus);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "updatedCount = " + updatedCount);
    return updatedCount;
  }

  /**
   * Marks a job as finished.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the database identifier of the job being claimed.
   * @param statusName
   *          A String with the name of the job status.
   * @param statusMessage
   *          A String with the status message.
   * @return an int with the count of jobs updated.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int markJobAsFinished(Connection conn, Long jobSeq, String statusName,
      String statusMessage) throws DbException {
    final String DEBUG_HEADER = "markJobAsFinished(): ";
    if (log.isDebug2()) {
      log.debug(DEBUG_HEADER + "jobSeq = " + jobSeq);
      log.debug(DEBUG_HEADER + "statusName = " + statusName);
      log.debug(DEBUG_HEADER + "statusMessage = " + statusMessage);
    }

    int updatedCount = -1;

    PreparedStatement updateJobStatus =
	dbManager.prepareStatement(conn, MARK_JOB_AS_FINISHED_QUERY);

    try {
      updateJobStatus.setLong(1, new Date().getTime());
      updateJobStatus.setLong(2, jobStatusSeqByName.get(statusName));
      updateJobStatus.setString(3, JobDbManager.truncateVarchar(statusMessage,
	  MAX_STATUS_MESSAGE_COLUMN));
      updateJobStatus.setLong(4, jobSeq);
      updatedCount = dbManager.executeUpdate(updateJobStatus);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "updatedCount = " + updatedCount);
    } catch (SQLException sqle) {
      String message = "Cannot mark a job as finished";
      log.error(message, sqle);
      log.error("jobSeq = " + jobSeq);
      log.error("statusName = '" + statusName + "'.");
      log.error("statusMessage = '" + statusMessage + "'.");
      log.error("SQL = '" + MARK_JOB_AS_FINISHED_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot mark a job as finished";
      log.error(message, dbe);
      log.error("jobSeq = " + jobSeq);
      log.error("statusName = '" + statusName + "'.");
      log.error("statusMessage = '" + statusMessage + "'.");
      log.error("SQL = '" + MARK_JOB_AS_FINISHED_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseStatement(updateJobStatus);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "updatedCount = " + updatedCount);
    return updatedCount;
  }

  /**
   * Marks a job as done.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the database identifier of the job being claimed.
   * @param statusMessage
   *          A String with the status message.
   * @return an int with the count of jobs updated.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int markJobAsDone(Connection conn, Long jobSeq, String statusMessage)
      throws DbException {
    return markJobAsFinished(conn, jobSeq, JOB_STATUS_DONE, statusMessage);
  }

  /**
   * Provides the identifier of the Archival Unit of a job.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the job identifier.
   * @return a String with the identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  String getJobAuId(Connection conn, Long jobSeq) throws DbException {
    final String DEBUG_HEADER = "getJobAuId(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    String auId = null;

    PreparedStatement findJob =
	dbManager.prepareStatement(conn, FIND_JOB_QUERY);

    ResultSet resultSet = null;

    try {
      // Get the job.
      findJob.setLong(1, jobSeq);
      resultSet = dbManager.executeQuery(findJob);

      if (resultSet.next()) {
	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

	auId = PluginManager.generateAuId(pluginId, auKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find job AuId";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_JOB_QUERY + "'.");
      log.error("jobSeq = " + jobSeq);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot find job AuId";
      log.error(message, dbe);
      log.error("SQL = '" + FIND_JOB_QUERY + "'.");
      log.error("jobSeq = " + jobSeq);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(findJob);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);
    return auId;
  }

  /**
   * Provides the count of reindexing jobs not started yet.
   * 
   * @return a long with the count of reidexing jobs not started yet.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getNotStartedReindexingJobsCount() throws DbException {
    return getReindexingJobsWithStatusCount(JOB_STATUS_CREATED);
  }

  /**
   * Provides data for reindexing jobs not started yet.
   * 
   * @param maxJobCount
   *          An int with the maximum number of jobs to return.
   * @return a List<Map<String, Object>> with the data for the jobs not started
   *         yet.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  List<Map<String, Object>> getNotStartedReindexingJobs(int maxJobCount)
      throws DbException {
    final String DEBUG_HEADER = "getNotStartedReindexingJobs(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "maxJobCount = " + maxJobCount);

    List<Map<String, Object>> notStartedJobs =
	new ArrayList<Map<String, Object>>();

    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    String sql = REINDEXING_JOBS_BY_STATUS_QUERY
	+ " order by " + PRIORITY_COLUMN
	+ ", " + JOB_SEQ_COLUMN;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "sql = " + sql);

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Prepare the query.
      stmt = dbManager.prepareStatement(conn, sql);
      stmt.setLong(1, jobTypeSeqByName.get(JOB_TYPE_DELETE_AU));
      stmt.setLong(2, jobStatusSeqByName.get(JOB_STATUS_CREATED));
      stmt.setMaxRows(maxJobCount);

      // Make the query.
      resultSet = dbManager.executeQuery(stmt);

      while (resultSet.next()) {
	Map<String, Object> job = new HashMap<String, Object>();

	Long jobTypeSeq = resultSet.getLong(JOB_TYPE_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);
	job.put(JOB_TYPE_SEQ_COLUMN, jobTypeSeq);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);
	job.put(AU_KEY_COLUMN, auKey);

	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
	job.put(PLUGIN_ID_COLUMN, pluginId);

	Long priority = resultSet.getLong(PRIORITY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "priority = " + priority);
	job.put(PRIORITY_COLUMN, priority);

	notStartedJobs.add(job);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the jobs not started yet";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      log.error("maxJobCount = " + maxJobCount);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the jobs not started yet";
      log.error(message, dbe);
      log.error("SQL = '" + sql + "'.");
      log.error("maxJobCount = " + maxJobCount);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "notStartedJobs.size() = " + notStartedJobs.size());
    return notStartedJobs;
  }

  /**
   * Updates the job queue truncation timestamp.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void updateJobQueueTruncationTimestamp(Connection conn)
      throws DbException {
    log.debug2("Invoked");

    int updatedCount = -1;

    PreparedStatement updateTruncationTimestamp =
	dbManager.prepareStatement(conn, UPDATE_TRUNCATION_TIMESTAMP_QUERY);

    try {
      updateTruncationTimestamp.setLong(1, new Date().getTime());

      updatedCount = dbManager.executeUpdate(updateTruncationTimestamp);
      if (log.isDebug3()) log.debug3("updatedCount = " + updatedCount);
    } catch (SQLException sqle) {
      String message = "Cannot update job queue truncation timestamp";
      log.error(message, sqle);
      log.error("SQL = '" + UPDATE_TRUNCATION_TIMESTAMP_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot update job queue truncation timestamp";
      log.error(message, dbe);
      log.error("SQL = '" + UPDATE_TRUNCATION_TIMESTAMP_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseStatement(updateTruncationTimestamp);
    }

    log.debug2("Done");
  }

  /**
   * Provides the job queue truncation timestamp.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a long with the job queue truncation timestamp.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getJobQueueTruncationTimestamp(Connection conn) throws DbException {
    log.debug2("Invoked");
    long timestamp = -1;

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      // Prepare the query.
      stmt = dbManager.prepareStatement(conn, GET_TRUNCATION_TIMESTAMP_QUERY);

      // Make the query.
      resultSet = dbManager.executeQuery(stmt);
      if (resultSet.next()) {
	timestamp = resultSet.getLong(TRUNCATION_TIME_COLUMN);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the queue truncation timestamp";
      log.error(message, sqle);
      log.error("SQL = '" + GET_TRUNCATION_TIMESTAMP_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the queue truncation timestamp";
      log.error(message, dbe);
      log.error("SQL = '" + GET_TRUNCATION_TIMESTAMP_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2("timestamp = " + timestamp);
    return timestamp;
  }

  /**
   * Provides the count of reindexing jobs.
   * 
   * @return a long with the count of reindexing jobs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getReindexingJobsCount() throws DbException {
    final String DEBUG_HEADER = "getReindexingJobsCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Prepare the query.
      stmt = dbManager.prepareStatement(conn, COUNT_REINDEXING_JOBS_QUERY);
      stmt.setLong(1, jobTypeSeqByName.get(JOB_TYPE_DELETE_AU));

      // Make the query.
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of all jobs";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_REINDEXING_JOBS_QUERY + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the count of all jobs";
      log.error(message, dbe);
      log.error("SQL = '" + COUNT_REINDEXING_JOBS_QUERY + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the count of reindexing jobs with a given status.
   * 
   * @param status
   *          A String with the requested status.
   * @return a long with the count of reindexing jobs with the given status.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getReindexingJobsWithStatusCount(String status) throws DbException {
    final String DEBUG_HEADER = "getReindexingJobsWithStatusCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "status = " + status);
    long rowCount = -1;

    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Prepare the query.
      stmt = dbManager.prepareStatement(conn,
	  COUNT_REINDEXING_JOBS_BY_STATUS_QUERY);
      stmt.setLong(1, jobTypeSeqByName.get(JOB_TYPE_DELETE_AU));
      stmt.setLong(2, jobStatusSeqByName.get(status));

      // Make the query.
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of reindexing jobs";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_REINDEXING_JOBS_BY_STATUS_QUERY + "'.");
      log.error("status = '" + status + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the count of reindexing jobs";
      log.error(message, dbe);
      log.error("SQL = '" + COUNT_REINDEXING_JOBS_BY_STATUS_QUERY + "'.");
      log.error("status = '" + status + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the count of successful reindexing jobs.
   * 
   * @return a long with the count of successful reindexing jobs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getSuccessfulReindexingJobsCount() throws DbException {
    return getReindexingJobsWithStatusAndMessageCount(JOB_STATUS_DONE,
	"Success");
  }

  /**
   * Provides the count of reindexing jobs with a given status and message.
   * 
   * @param status  A String with the requested status.
   * @param message A String with the requested message.
   * @return a long with the count of reindexing jobs with the given status and
   *         message.
   * @throws DbException if any problem occurred accessing the database.
   */
  long getReindexingJobsWithStatusAndMessageCount(String status,
      String statusMessage) throws DbException {
    final String DEBUG_HEADER =
	"getReindexingJobsWithStatusAndMessageCount(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "status = " + status);
      log.debug2(DEBUG_HEADER + "statusMessage = " + statusMessage);
    }

    long rowCount = -1;

    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Prepare the query.
      stmt = dbManager.prepareStatement(conn,
	  COUNT_REINDEXING_JOBS_BY_STATUS_AND_MESSAGE_QUERY);
      stmt.setLong(1, jobTypeSeqByName.get(JOB_TYPE_DELETE_AU));
      stmt.setLong(2, jobStatusSeqByName.get(status));
      stmt.setString(3, statusMessage);

      // Make the query.
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of reindexing jobs";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_REINDEXING_JOBS_BY_STATUS_AND_MESSAGE_QUERY
	  + "'.");
      log.error("status = '" + status + "'.");
      log.error("statusMessage = '" + statusMessage + "'.");
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the count of reindexing jobs";
      log.error(message, dbe);
      log.error("SQL = '" + COUNT_REINDEXING_JOBS_BY_STATUS_AND_MESSAGE_QUERY
	  + "'.");
      log.error("status = '" + status + "'.");
      log.error("statusMessage = '" + statusMessage + "'.");
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the count of failed reindexing jobs.
   * 
   * @return a long with the count of failed reindexing jobs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getFailedReindexingJobsCount() throws DbException {
    return getReindexingJobsWithStatusCount(JOB_STATUS_DONE)
	- getSuccessfulReindexingJobsCount();
  }

  /**
   * Provides data for finished reindexing jobs that are started before a given
   * timestamp.
   * 
   * @param maxJobCount
   *          An int with the maximum number of jobs to return.
   * @param beforeTime
   *          A long with the timestamp.
   * @return a List<Map<String, Object>> with the data for the finished jobs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  List<Map<String, Object>> getFinishedReindexingJobsBefore(int maxJobCount,
      long beforeTime) throws DbException {
    final String DEBUG_HEADER = "getFinishedReindexingJobsBefore(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "maxJobCount = " + maxJobCount);
      log.debug2(DEBUG_HEADER + "beforeTime = " + beforeTime);
    }

    List<Map<String, Object>> finishedJobs =
	new ArrayList<Map<String, Object>>();

    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    String sql = REINDEXING_JOBS_BEFORE_BY_STATUS_QUERY
	+ " order by " + START_TIME_COLUMN + " desc";
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "sql = " + sql);

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Prepare the query.
      stmt = dbManager.prepareStatement(conn, sql);
      stmt.setLong(1, jobTypeSeqByName.get(JOB_TYPE_DELETE_AU));
      stmt.setLong(2, jobStatusSeqByName.get(JOB_STATUS_DONE));
      stmt.setLong(3, beforeTime);
      stmt.setMaxRows(maxJobCount);

      // Make the query.
      resultSet = dbManager.executeQuery(stmt);

      while (resultSet.next()) {
	Map<String, Object> job = new HashMap<String, Object>();

	Long jobTypeSeq = resultSet.getLong(JOB_TYPE_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);
	job.put(JOB_TYPE_SEQ_COLUMN, jobTypeSeq);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);
	job.put(AU_KEY_COLUMN, auKey);

	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
	job.put(PLUGIN_ID_COLUMN, pluginId);

	Long startTime = resultSet.getLong(START_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "startTime = " + startTime);
	job.put(START_TIME_COLUMN, startTime);

	Long endTime = resultSet.getLong(END_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "endTime = " + endTime);
	job.put(END_TIME_COLUMN, endTime);

	Long priority = resultSet.getLong(PRIORITY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "priority = " + priority);
	job.put(PRIORITY_COLUMN, priority);

	String statusMessage = resultSet.getString(STATUS_MESSAGE_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "statusMessage = '" + statusMessage + "'");
	job.put(STATUS_MESSAGE_COLUMN, statusMessage);

	finishedJobs.add(job);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the finished jobs";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      log.error("maxJobCount = " + maxJobCount);
      log.error("beforeTime = " + beforeTime);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the finished jobs";
      log.error(message, dbe);
      log.error("SQL = '" + sql + "'.");
      log.error("maxJobCount = " + maxJobCount);
      log.error("beforeTime = " + beforeTime);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "finishedJobs.size() = " + finishedJobs.size());
    return finishedJobs;
  }

  /**
   * Provides data for failed reindexing jobs that are started before a given
   * timestamp.
   * 
   * @param maxJobCount
   *          An int with the maximum number of jobs to return.
   * @param beforeTime
   *          A long with the timestamp.
   * @return a List<Map<String, Object>> with the data for the failed jobs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  List<Map<String, Object>> getFailedReindexingJobsBefore(int maxJobCount,
      long beforeTime) throws DbException {
    final String DEBUG_HEADER = "getFailedReindexingJobsBefore(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "maxJobCount = " + maxJobCount);
      log.debug2(DEBUG_HEADER + "beforeTime = " + beforeTime);
    }

    List<Map<String, Object>> failedJobs = new ArrayList<Map<String, Object>>();
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    String sql = REINDEXING_JOBS_BEFORE_BY_STATUS_QUERY
	+ " and " + STATUS_MESSAGE_COLUMN + " != ?"
	+ " order by " + START_TIME_COLUMN + " desc";
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "sql = " + sql);

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Prepare the query.
      stmt = dbManager.prepareStatement(conn, sql);
      stmt.setLong(1, jobTypeSeqByName.get(JOB_TYPE_DELETE_AU));
      stmt.setLong(2, jobStatusSeqByName.get(JOB_STATUS_DONE));
      stmt.setLong(3, beforeTime);
      stmt.setString(4, "Success");
      stmt.setMaxRows(maxJobCount);

      // Make the query.
      resultSet = dbManager.executeQuery(stmt);

      while (resultSet.next()) {
	Map<String, Object> job = new HashMap<String, Object>();

	Long jobTypeSeq = resultSet.getLong(JOB_TYPE_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);
	job.put(JOB_TYPE_SEQ_COLUMN, jobTypeSeq);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);
	job.put(AU_KEY_COLUMN, auKey);

	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
	job.put(PLUGIN_ID_COLUMN, pluginId);

	Long startTime = resultSet.getLong(START_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "startTime = " + startTime);
	job.put(START_TIME_COLUMN, startTime);

	Long endTime = resultSet.getLong(END_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "endTime = " + endTime);
	job.put(END_TIME_COLUMN, endTime);

	Long priority = resultSet.getLong(PRIORITY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "priority = " + priority);
	job.put(PRIORITY_COLUMN, priority);

	String statusMessage = resultSet.getString(STATUS_MESSAGE_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "statusMessage = '" + statusMessage + "'");
	job.put(STATUS_MESSAGE_COLUMN, statusMessage);

	failedJobs.add(job);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the failed jobs";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      log.error("maxJobCount = " + maxJobCount);
      log.error("beforeTime = " + beforeTime);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot get the failed jobs";
      log.error(message, dbe);
      log.error("SQL = '" + sql + "'.");
      log.error("maxJobCount = " + maxJobCount);
      log.error("beforeTime = " + beforeTime);
      throw dbe;
    } finally {
      JobDbManager.safeCloseResultSet(resultSet);
      JobDbManager.safeCloseStatement(stmt);
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "failedJobs.size() = " + failedJobs.size());
    return failedJobs;
  }

  /**
   * Provides the map of job type database identifiers by name.
   * 
   * @return a Map<String, Long> with the map of job type database identifiers
   *         by name.
   */
  public Map<String, Long> getJobTypeSeqByName() {
    return jobTypeSeqByName;
  }
}
