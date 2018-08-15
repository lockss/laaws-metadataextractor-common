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
package org.lockss.metadata.extractor;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import org.lockss.app.LockssApp;
import org.lockss.app.LockssDaemon;
import org.lockss.config.CurrentConfig;
import org.lockss.daemon.LockssWatchdog;
import org.lockss.db.JdbcContext;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.extractor.MetadataExtractorManager.ReindexingStatus;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.scheduler.SchedulableTask;
import org.lockss.scheduler.Schedule;
import org.lockss.scheduler.StepTask;
import org.lockss.scheduler.TaskCallback;
import org.lockss.util.Constants;
import org.lockss.util.Logger;
import org.lockss.util.time.TimeBase;
import org.lockss.util.TimeInterval;

/**
 * Implements a task that removes from the database the metadata of the
 * specified Archival Unit.
 * 
 * @author Fernando Garcia-Loygorri
 */
public class DeleteMetadataTask extends StepTask {
  private static Logger log = Logger.getLogger(DeleteMetadataTask.class);

  // The Archival Unit for this task.
  private final ArchivalUnit au;

  // The status of this task.
  private volatile ReindexingStatus status = ReindexingStatus.Running;

  // Archival Unit properties.
  private final String auName;
  private final String auId;

  // ThreadMXBean times.
  private volatile long startCpuTime = 0;
  private volatile long startUserTime = 0;
  private volatile long startClockTime = 0;

  private volatile long endCpuTime = 0;
  private volatile long endUserTime = 0;
  private volatile long endClockTime = 0;

  // The database manager.
  private final MetadataDbManager dbManager;

  // The metadata extractor manager.
  private final MetadataExtractorManager mdxManager;

  // The metadata extractor manager SQL executor.
  private final MetadataExtractorManagerSql mdxManagerSql;

  private LockssWatchdog watchDog;

  private static ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();

  static {
    log.debug3("current thread CPU time supported? "
        + tmxb.isCurrentThreadCpuTimeSupported());

    if (tmxb.isCurrentThreadCpuTimeSupported()) {
      tmxb.setThreadCpuTimeEnabled(true);
    }
  }

  // The JDBC context through which it's possible to abort the current query.
  private JdbcContext jdbcCtxt;

  /**
   * Constructor.
   * 
   * @param au
   *          An ArchivalUnit with the AU for the task.
   */
  public DeleteMetadataTask(ArchivalUnit au) {
    // NOTE: estimated window time interval duration not currently used.
    super(
          new TimeInterval(TimeBase.nowMs(), TimeBase.nowMs() + Constants.HOUR),
          0, // estimatedDuration.
          null, // TaskCallback.
          null); // Object cookie.

    this.au = au;
    this.auName = au.getName();
    this.auId = au.getAuId();
    dbManager = LockssDaemon.getLockssDaemon().getMetadataDbManager();
    mdxManager =
	LockssApp.getManagerByTypeStatic(MetadataExtractorManager.class);

    mdxManagerSql = mdxManager.getMetadataExtractorManagerSql();

    // Set the task event handler callback after construction to ensure that the
    // instance is initialized.
    callback = new MetadataRemovalEventHandler();
  }

  void setWDog(LockssWatchdog watchDog) {
    this.watchDog = watchDog;
  }

  void pokeWDog() {
    watchDog.pokeWDog();
  }

  /**
   * Cancels the current task without rescheduling it.
   */
  @Override
  public void cancel() {
    final String DEBUG_HEADER = "cancel(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    if (log.isDebug3()) {
      log.debug3(DEBUG_HEADER + "isFinished() = " + isFinished());
      log.debug3(DEBUG_HEADER + "status = " + status);
    }

    // Abort any JDBC operation in progress.
    jdbcCtxt.abort();

    if (!isFinished() && (status == ReindexingStatus.Running)) {
      status = ReindexingStatus.Failed;
      super.cancel();
      setFinished();
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Deletes from the database the Archival Unit metadata.
   * 
   * @param n
   *          An int with the amount of work to do.
   */
  @Override
  public int step(int n) {
    final String DEBUG_HEADER = "step(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "n = " + n);

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

    // Check whether the metadata should be deleted via a REST web service.
    if (CurrentConfig.getParam(MetadataExtractorManager
	.PARAM_MD_REST_SERVICE_LOCATION) != null) {
      // Yes: Invoke the REST web service operation.
      try {
	Integer removedArticleCount =
	    new DeleteAuItemsClient().deleteAuItems(auId);
	log.info("Metadata removal task for AU '" + auName + "' removed "
	    + removedArticleCount + " database items.");
      } catch (Exception ex) {
	e = ex;
	log.warning("Error removing metadata", e);
	setFinished();
	if (status == ReindexingStatus.Running) {
	  status = ReindexingStatus.Failed;
	}
      }
    } else {
      // No: Delete the metadata from the database.
      Connection conn = null;

      try {
	conn = dbManager.getConnection();
	jdbcCtxt = new JdbcContext(conn);

	// Remove from the database the Archival Unit metadata.
	int removedArticleCount =
	    mdxManagerSql.removeAuMetadataItems(jdbcCtxt, auId);
	log.info("Metadata removal task for AU '" + auName + "' removed "
	    + removedArticleCount + " database items.");

	// Complete the database transaction.
	MetadataDbManager.commitOrRollback(conn, log);
      } catch (Exception ex) {
	e = ex;
	log.warning("Error removing metadata", e);
	setFinished();
	if (status == ReindexingStatus.Running) {
	  status = ReindexingStatus.Failed;
	}
      } finally {
	MetadataDbManager.safeRollbackAndClose(conn);
      }
    }

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "isFinished() = " + isFinished());

    if (!isFinished()) {
      setFinished();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "isFinished() = " + isFinished());
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    return 0;
  }

  /**
   * Returns the task AU.
   * 
   * @return an ArchivalUnit with the AU of this task.
   */
  ArchivalUnit getAu() {
    return au;
  }

  /**
   * Returns the auid of the task AU.
   * 
   * @return a String with the auid of the task AU.
   */
  public String getAuId() {
    return auId;
  }

  /**
   * Temporary
   * 
   * @param evt A Schedule.EventType with the event type.
   */
  protected void handleEvent(Schedule.EventType evt) {
    callback.taskEvent(this, evt);
  }

  /**
   * The handler for metadata removal lifecycle events.
   */
  private class MetadataRemovalEventHandler implements TaskCallback {
    private final Logger log =
	Logger.getLogger(MetadataRemovalEventHandler.class);

    /**
     * Handles an event.
     * 
     * @param task
     *          A SchedulableTask with the task that has changed state.
     * @param type
     *          A Schedule.EventType indicating the type of event.
     */
    @Override
    public void taskEvent(SchedulableTask task, Schedule.EventType type) {
      long threadCpuTime = 0;
      long threadUserTime = 0;
      long currentClockTime = TimeBase.nowMs();

      if (tmxb.isCurrentThreadCpuTimeSupported()) {
        threadCpuTime = tmxb.getCurrentThreadCpuTime();
        threadUserTime = tmxb.getCurrentThreadUserTime();
      }

      // Check whether it's a start event.
      if (type == Schedule.EventType.START) {
        // Yes: Handle the start event.
        handleStartEvent(threadCpuTime, threadUserTime, currentClockTime);
        // No: Check whether it's a finish event.
      } else if (type == Schedule.EventType.FINISH) {
        // Yes: Handle the finish event.
        handleFinishEvent(task, threadCpuTime, threadUserTime,
                          currentClockTime);
      } else {
	// No: Handle an unknown event.
        log.error("Received unknown metadata removal lifecycle event type '"
            + type + "' for AU '" + auName + "' - Ignored.");
      }
    }

    /**
     * Handles a starting event.
     * 
     * @param threadCpuTime
     *          A long with the thread CPU time.
     * @param threadUserTime
     *          A long with the thread user time.
     * @param currentClockTime
     *          A long with the current clock time.
     */
    private void handleStartEvent(long threadCpuTime, long threadUserTime,
        long currentClockTime) {
      final String DEBUG_HEADER = "handleStartEvent(): ";
      log.info("Starting metadata removal task for AU '" + auName + "'");

      // Remember the times at startup.
      startCpuTime = threadCpuTime;
      startUserTime = threadUserTime;
      startClockTime = currentClockTime;

      try {
        mdxManager.notifyStartAuMetadataRemoval(au);
      } catch (Exception e) {
        log.error("Failed to set up metadata removal for AU '" + auName + "'",
            e);
        setFinished();
        if (status == ReindexingStatus.Running) {
          status = ReindexingStatus.Failed;
        }
      }

      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    }

    /**
     * Handles a finishing event.
     * 
     * @param task
     *          A SchedulableTask with the task that has finished.
     * @param threadCpuTime
     *          A long with the thread CPU time.
     * @param threadUserTime
     *          A long with the thread user time.
     * @param currentClockTime
     *          A long with the current clock time.
     */
    private void handleFinishEvent(SchedulableTask task, long threadCpuTime,
        long threadUserTime, long currentClockTime) {
      final String DEBUG_HEADER = "handleFinishEvent(): ";
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "task = " + task);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "AU '" + auName
	  + "': status = " + status);

      if (status == ReindexingStatus.Running) {
        status = ReindexingStatus.Success;
      }

      endClockTime = TimeBase.nowMs();

      if (tmxb.isCurrentThreadCpuTimeSupported()) {
        endCpuTime = tmxb.getCurrentThreadCpuTime();
        endUserTime = tmxb.getCurrentThreadUserTime();
      }

      // Display timings.
      long elapsedCpuTime = threadCpuTime - startCpuTime;
      long elapsedUserTime = threadUserTime - startUserTime;
      long elapsedClockTime = currentClockTime - startClockTime;

      log.info("Finished metadata removal task for AU '" + auName
	  + "': status = " + status + ", CPU time: " + elapsedCpuTime / 1.0e9
	  + " (" + endCpuTime / 1.0e9 + "), User time: "
	  + elapsedUserTime / 1.0e9 + " (" + endUserTime / 1.0e9
	  + "), Clock time: " + elapsedClockTime / 1.0e3 + " ("
	  + endClockTime / 1.0e3 + ")");

      mdxManager.notifyFinishAuMetadataRemoval(au, status, task.getException());

      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    }
  }
}
