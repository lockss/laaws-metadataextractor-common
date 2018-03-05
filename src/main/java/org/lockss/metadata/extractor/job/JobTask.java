/*

Copyright (c) 2016-2018 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.metadata.extractor.job.SqlConstants.*;
import java.sql.Connection;
import org.lockss.app.LockssDaemon;
import org.lockss.db.DbException;
import org.lockss.metadata.extractor.MetadataExtractorManager;
import org.lockss.scheduler.StepTask;
import org.lockss.util.Logger;

/**
 * Processor for a job processing the metadata of an Archival Unit.
 * 
 * @author Fernando García-Loygorri
 */
public class JobTask implements Runnable {
  private static final Logger log = Logger.getLogger(JobTask.class);

  private long sleepMs = 60000;
  private String baseTaskName;
  private String taskName;
  private Long jobSeq = null;
  private JobDbManager dbManager;
  private MetadataExtractorManager mdxManager;
  private JobManager jobManager;
  private boolean isJobFinished = false;
  private StepTask stepTask = null;

  /**
   * Constructor.
   * 
   * @param dbManager
   *          A JobDbManager with the Job SQL code executor.
   * @param mdxManager
   *          A MetadataExtractorManager with the metadata extractor manager.
   * @param jobManager
   *          A JobManager with the Job manager.
   */
  public JobTask(JobDbManager dbManager, MetadataExtractorManager mdxManager,
      JobManager jobManager) {
    this.dbManager = dbManager;
    this.mdxManager = mdxManager;
    this.jobManager = jobManager;
    sleepMs = jobManager.getSleepDelaySeconds() * 1000L;
  }

  /**
   * Main task process.
   */
  public void run() {
    final String DEBUG_HEADER = "run(): ";
    LockssDaemon daemon = LockssDaemon.getLockssDaemon();

    // Wait until the Archival Units have been started.
    if (!daemon.areAusStarted()) {
      log.debug(DEBUG_HEADER + "Waiting for Archival Units to start");

      while (!daemon.areAusStarted()) {
	try {
	  daemon.waitUntilAusStarted();
	} catch (InterruptedException ex) {
	}
      }
    }

    baseTaskName = Thread.currentThread().getName();
    taskName = baseTaskName;
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "Invoked task '" + baseTaskName + "'");

    // Infinite loop.
    while (jobSeq == null) {
      try {
	// Claim the next job.
	jobSeq = jobManager.claimNextJob(taskName);

	// Check whether a job was actually claimed.
	if (jobSeq != null) {
	  // Yes: Process it.
	  taskName = baseTaskName + " - jobSeq=" + jobSeq;
	  processJob(jobSeq);
	} else {
	  // No: Wait before the next attempt.
	  sleep(DEBUG_HEADER);
	}
      } catch (Exception e) {
	log.error("Exception caught claiming or processing job: ", e);
      } finally {
	jobSeq = null;
	stepTask = null;
	taskName = baseTaskName;
	isJobFinished = false;
      }
    }
  }

  /**
   * Processes a job.
   * 
   * @param jobSeq
   *          A Long with the database identifier of the job to be processed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void processJob(Long jobSeq) throws DbException {
    final String DEBUG_HEADER = "processJob() - " + taskName + ": ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    // Get the type of job.
    String jobType = jobManager.getJobType(jobSeq);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "jobType = " + jobType);

    // Check whether it's a full metadata extraction job.
    if (JOB_TYPE_PUT_AU.equals(jobType)) {
      // Yes: Extract the metadata.
      processPutAuJob(jobSeq, true);
      // No: Check whether it's an incremental metadata extraction job.
    } else if (JOB_TYPE_PUT_INCREMENTAL_AU.equals(jobType)) {
      // Yes: Extract the metadata.
      processPutAuJob(jobSeq, false);
      // No: Check whether it's a metadata removal job.
    } else if (JOB_TYPE_DELETE_AU.equals(jobType)) {
      // Yes: Remove the metadata.
      processDeleteAuJob(jobSeq);
    } else {
      // No: Handle a job of an unknown type.
      Connection conn = null;

      try {
        // Get a connection to the database.
        conn = dbManager.getConnection();

        jobManager.markJobAsDone(conn, jobSeq, "Unknown job type");
        JobDbManager.commitOrRollback(conn, log);
        log.error("Ignored job " + jobSeq + " of unknown type = " + jobType);
      } finally {
	JobDbManager.safeRollbackAndClose(conn);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Processes a job that extracts and stores the metadata of an Archival Unit.
   * 
   * @param jobSeq
   *          A Long with the database identifier of the job to be processed.
   * @param needFullReindex
   *          A boolean with the indication of whether a full extraction is to
   *          be performed or not.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void processPutAuJob(Long jobSeq, boolean needFullReindex)
      throws DbException {
    final String DEBUG_HEADER = "processPutAuJob() - " + taskName + ": ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);
      log.debug2(DEBUG_HEADER + "needFullReindex = " + needFullReindex);
    }

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      processPutAuJob(conn, jobSeq, needFullReindex);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Processes a job that extracts and stores the metadata of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the database identifier of the job to be processed.
   * @param needFullReindex
   *          A boolean with the indication of whether a full extraction is to
   *          be performed or not.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void processPutAuJob(Connection conn, Long jobSeq,
      boolean needFullReindex) throws DbException {
    final String DEBUG_HEADER = "processPutAuJob() - " + taskName + ": ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);
      log.debug2(DEBUG_HEADER + "needFullReindex = " + needFullReindex);
    }

    String auId = jobManager.getJobAuId(conn, jobSeq);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

    // Extract the metadata.
    stepTask = mdxManager.onDemandStartReindexing(auId, needFullReindex);

    // Wait until the process is done.
    while (!isJobFinished) {
      sleep(DEBUG_HEADER);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Processes a job that deletes the metadata of an Archival Unit.
   * 
   * @param jobSeq
   *          A Long with the database identifier of the job to be processed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void processDeleteAuJob(Long jobSeq) throws DbException {
    final String DEBUG_HEADER = "processDeleteAuJob() - " + taskName + ": ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      processDeleteAuJob(conn, jobSeq);
    } finally {
      JobDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Processes a job that deletes the metadata of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param jobSeq
   *          A Long with the database identifier of the job to be processed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void processDeleteAuJob(Connection conn, Long jobSeq)
      throws DbException {
    final String DEBUG_HEADER = "processDeleteAuJob() - " + taskName + ": ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "jobSeq = " + jobSeq);

    String auId = jobManager.getJobAuId(conn, jobSeq);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

    // Delete the metadata.
    stepTask = mdxManager.startMetadataRemoval(auId);

    // Wait until the process is done.
    while (!isJobFinished) {
      sleep(DEBUG_HEADER);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Terminates this task.
   * 
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void terminateTask() throws DbException {
    final String DEBUG_HEADER = "terminateTask() - " + taskName + ": ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    if (getJobId() != null) {
      Connection conn = null;

      try {
	// Get a connection to the database.
	conn = dbManager.getConnection();

	jobManager.markJobAsFinished(conn, jobSeq, JOB_STATUS_TERMINATED,
	    "Terminated by request");
	JobDbManager.commitOrRollback(conn, log);
      } finally {
	JobDbManager.safeRollbackAndClose(conn);
      }
    } else {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Nothing to do: jobSeq is null.");
    }

    if (stepTask != null) {
      if (log.isDebug3()) {
	log.debug3(DEBUG_HEADER
	    + "stepTask.isFinished() = " + stepTask.isFinished());
	log.debug3(DEBUG_HEADER + "Cancelling task " + stepTask);
      }

      stepTask.cancel();
    } else {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Nothing to do: stepTask is null.");
      notifyJobFinish();
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the job identifier of this task.
   * 
   * @return a Long with the job identifier of this task.
   */
  Long getJobId() {
    return jobSeq;
  }

  /**
   * Provides the underlying step task of this task.
   * 
   * @return a StepTask with the underlying step task of this task.
   */
  StepTask getStepTask() {
    return stepTask;
  }

  /**
   * Marks the job of this task as finished.
   */
  void notifyJobFinish() {
    isJobFinished = true;
  }

  /**
   * Waits for the standard polling amount of time to pass.
   * 
   * @param id
   *          A String with the name of the method requesting the wait.
   */
  private void sleep(String id) {
    if (log.isDebug3())
      log.debug3(id + "Going to sleep task '" + taskName + "'");

    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException ie) {}

    if (log.isDebug3())
      log.debug3(id + "Back from sleep task '" + taskName + "'");
  }

  @Override
  public String toString() {
    return "[JobTask taskname=" + taskName + ", jobSeq=" + jobSeq + "]";
  }
}
