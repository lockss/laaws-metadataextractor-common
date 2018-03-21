/*

Copyright (c) 2013-2018 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.*;
import java.lang.management.*;
import java.sql.*;
import java.util.*;
import org.lockss.app.LockssApp;
import org.lockss.app.LockssDaemon;
import org.lockss.config.TdbAu;
import org.lockss.daemon.*;
import org.lockss.db.DbException;
import org.lockss.extractor.*;
import org.lockss.extractor.ArticleMetadataExtractor.Emitter;
import org.lockss.extractor.MetadataException.ValidationException;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.extractor.ArticleMetadataBuffer.ArticleMetadataInfo;
import org.lockss.metadata.extractor.MetadataExtractorManager.ReindexingStatus;
import org.lockss.plugin.*;
import org.lockss.scheduler.*;
import org.lockss.util.*;

/**
 * Implements a reindexing task that extracts metadata from all the articles in
 * the specified AU.
 */
public class ReindexingTask extends StepTask {
  private static Logger log = Logger.getLogger(ReindexingTask.class);

  // The default number of steps for this task.
  private static final int default_steps = 10;

  // The archival unit for this task.
  private final ArchivalUnit au;

  // The article metadata extractor for this task.
  private final ArticleMetadataExtractor ae;

  // The article iterator for this task.
  private Iterator<ArticleFiles> articleIterator = null;

  // The hashes of the text strings of the log messages already emitted for this
  // task's AU. Used to prevent duplicate messages from being logged.
  private final HashSet<Integer> auLogTable = new HashSet<Integer>();

  // An indication of whether the AU being indexed is new to the index.
  private volatile boolean isNewAu = true;
  
  // An indication of whether the AU being indexed needs a full reindex
  private volatile boolean needFullReindex = false;

  // The time from which to extract new metadata.
  private long lastExtractTime = 0;

  // The status of the task: successful if true.
  private volatile ReindexingStatus status = ReindexingStatus.Running;

  // The number of articles indexed by this task.
  private volatile long indexedArticleCount = 0;

  // The number of articles updated by this task.
  private volatile long updatedArticleCount = 0;

  // ThreadMXBean times.
  private volatile long startCpuTime = 0;
  private volatile long startUserTime = 0;
  private volatile long startClockTime = 0;

  private volatile long startUpdateClockTime = 0;

  private volatile long endCpuTime = 0;
  private volatile long endUserTime = 0;
  private volatile long endClockTime = 0;

  // Archival unit properties.
  private final String auName;
  private final String auId;
  private final boolean auNoSubstance;

  private ArticleMetadataBuffer articleMetadataInfoBuffer = null;

  // The database manager.
  private final MetadataDbManager dbManager;

  // The metadata extractor manager.
  private final MetadataExtractorManager mdxManager;

  // The metadata extractor manager SQL executor.
  private final MetadataExtractorManagerSql mdxManagerSql;

  private final Emitter emitter;
  private int extractedCount = 0;

  private LockssWatchdog watchDog;

  private static ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();

  static {
    log.debug3("current thread CPU time supported? "
        + tmxb.isCurrentThreadCpuTimeSupported());

    if (tmxb.isCurrentThreadCpuTimeSupported()) {
      tmxb.setThreadCpuTimeEnabled(true);
    }
  }

  private boolean cancelled = false;

  // The indication of whether the content of an archival unit should be
  // obtained from a web service instead of the repository.
  private boolean isAuContentFromWs = false;

  /**
   * Constructor.
   * 
   * @param theAu
   *          An ArchivalUnit with the AU for the task.
   * @param theAe
   *          An ArticleMetadataExtractor with the article metadata extractor to
   *          be used.
   */
  public ReindexingTask(ArchivalUnit theAu, ArticleMetadataExtractor theAe) {
    // NOTE: estimated window time interval duration not currently used.
    super(
          new TimeInterval(TimeBase.nowMs(), TimeBase.nowMs() + Constants.HOUR),
          0, // estimatedDuration.
          null, // TaskCallback.
          null); // Object cookie.

    final String DEBUG_HEADER = "ReindexingTask(): ";
    this.au = theAu;
    this.ae = theAe;
    this.auName = au.getName();
    this.auId = au.getAuId();

    isAuContentFromWs =
	LockssDaemon.getLockssDaemon().getPluginManager().isAuContentFromWs();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "isAuContentFromWs = " + isAuContentFromWs);

    if (isAuContentFromWs) {
      this.auNoSubstance = false;
    } else {
      this.auNoSubstance = AuUtil.getAuState(au).hasNoSubstance();
    }

    dbManager = LockssDaemon.getLockssDaemon().getMetadataDbManager();
    mdxManager =
	LockssApp.getManagerByTypeStatic(MetadataExtractorManager.class);
    mdxManager.notifyStartReindexingAu(theAu);
    mdxManagerSql = mdxManager.getMetadataExtractorManagerSql();

    // The accumulator of article metadata.
    emitter = new ReindexingEmitter();

    // Set the task event handler callback after construction to ensure that the
    // instance is initialized.
    callback = new ReindexingEventHandler();
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
    if (log.isDebug3()) {
      log.debug3(DEBUG_HEADER + "isFinished() = " + isFinished());
      log.debug3(DEBUG_HEADER + "status = " + status);
    }

    cancelled = true;

    if (!isFinished() && (status == ReindexingStatus.Running)) {
      status = ReindexingStatus.Failed;
      super.cancel();
      setFinished();
    }
  }

  /**
   * Extracts the metadata from the next group of articles.
   * 
   * @param n
   *          An int with the amount of work to do.
   * TODO: figure out what the amount of work means
   */
  @Override
  public int step(int n) {
    final String DEBUG_HEADER = "step(): ";
    int steps = (n <= 0) ? default_steps : n;
    log.debug3(DEBUG_HEADER + "step: " + steps + ", has articles: "
        + articleIterator.hasNext());

    while (!isFinished() && (extractedCount <= steps)
        && articleIterator.hasNext()) {
      log.debug3(DEBUG_HEADER + "Getting the next ArticleFiles...");
      ArticleFiles af = articleIterator.next();
      try {
        ae.extract(MetadataTarget.OpenURL(), af, emitter);
      } catch (IOException ex) {
        log.error("Failed to index metadata for full text URL: "
                      + af.getFullTextUrl(), ex);
        setFinished();
        if (status == ReindexingStatus.Running) {
          status = ReindexingStatus.Rescheduled;
          indexedArticleCount = 0;
        }
      } catch (PluginException ex) {
        log.error("Failed to index metadata for full text URL: "
                      + af.getFullTextUrl(), ex);
        setFinished();
        if (status == ReindexingStatus.Running) {
          status = ReindexingStatus.Failed;
          indexedArticleCount = 0;
        }
      } catch (RuntimeException ex) {
        log.error(" Caught unexpected Throwable for full text URL: "
                      + af.getFullTextUrl(), ex);
        setFinished();
        if (status == ReindexingStatus.Running) {
          status = ReindexingStatus.Failed;
          indexedArticleCount = 0;
        }
      }
      
      pokeWDog();
    }

    log.debug3(DEBUG_HEADER + "isFinished() = " + isFinished());
    if (!isFinished()) {
      // finished if all articles handled
      if (!articleIterator.hasNext()) {
        setFinished();
        log.debug3(DEBUG_HEADER + "isFinished() = " + isFinished());
      }
    }

    log.debug3(DEBUG_HEADER + "extractedCount = " + extractedCount);
    return extractedCount;
  }

  /**
   * Cancels and marks the current task for rescheduling.
   */
  void reschedule() {
    if (!isFinished() && (status == ReindexingStatus.Running)) {
      status = ReindexingStatus.Rescheduled;
      super.cancel();
      setFinished();
    }
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
   * Returns the name of the task AU.
   * 
   * @return a String with the name of the task AU.
   */
  String getAuName() {
    return auName;
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
   * Returns the substance state of the task AU.
   * 
   * @return <code>true</code> if AU has no substance, <code>false</code>
   *         otherwise.
   */
  boolean hasNoAuSubstance() {
    return auNoSubstance;
  }

  /**
   * Returns an indication of whether the AU has not yet been indexed.
   * 
   * @return <code>true</code> if the AU has not yet been indexed,
   * <code>false</code> otherwise.
   */
  boolean isNewAu() {
    return isNewAu;
  }

  /**
   * Returns an indication of whether the AU needs a full reindex.
   * 
   * @return <code>true</code> if the AU needs a full reindex,
   * <code>false</code> otherwise.
   */
  boolean needsFullReindex() {
    return needFullReindex;
  }

  /**
   * Provides the start time for indexing.
   * 
   * @return a long with the start time in miliseconds since epoch (0 if not
   *         started).
   */
  long getStartTime() {
    return startClockTime;
  }

  /**
   * Provides the update start time.
   * 
   * @return a long with the update start time in miliseconds since epoch (0 if
   *         not started).
   */
  long getStartUpdateTime() {
    return startUpdateClockTime;
  }

  /**
   * Provides the end time for indexing.
   * 
   * @return a long with the end time in miliseconds since epoch (0 if not
   *         finished).
   */
  long getEndTime() {
    return endClockTime;
  }

  /**
   * Returns the reindexing status of this task.
   * 
   * @return a ReindexingStatus with the reindexing status.
   */
  ReindexingStatus getReindexingStatus() {
    return status;
  }

  /**
   * Returns the number of articles extracted by this task.
   * 
   * @return a long with the number of articles extracted by this task.
   */
  long getIndexedArticleCount() {
    return indexedArticleCount;
  }

  /**
   * Returns the number of articles updated by this task.
   * 
   * @return a long with the number of articles updated by this task.
   */
  long getUpdatedArticleCount() {
    return updatedArticleCount;
  }

  /**
   * Increments by one the number of articles updated by this task.
   */
  void incrementUpdatedArticleCount() {
    this.updatedArticleCount++;
  }

  /**
   * Temporary
   * 
   * @param evt
   *          A Schedule.EventType with the event type.
   */
  protected void handleEvent(Schedule.EventType evt) {
    callback.taskEvent(this, evt);
  }

  /**
   * Issues a warning for this re-indexing task.
   * 
   * @param s
   *          A String with the warning message.
   */
  void taskWarning(String s) {
    int hashcode = s.hashCode();
    if (auLogTable.add(hashcode)) {
        log.warning(s);
    }
  }

  /**
   * Accumulator of article metadata.
   */
  private class ReindexingEmitter implements Emitter {
    private final Logger log = Logger.getLogger(ReindexingEmitter.class);

    @Override
    public void emitMetadata(ArticleFiles af, ArticleMetadata md) {
      final String DEBUG_HEADER = "emitMetadata(): ";
      
      if (log.isDebug3()) {
        log.debug3(DEBUG_HEADER+"\n"+md.ppString(2));
      }

      Map<String, String> roles = new HashMap<String, String>();

      for (String key : af.getRoleMap().keySet()) {
        String value = af.getRoleAsString(key);
        if (log.isDebug3()) {
          log.debug3(DEBUG_HEADER + "af.getRoleMap().key = " + key
              + ", af.getRoleUrl(key) = " + value);
        }
        roles.put(key, value);
      }

      if (log.isDebug3()) {
        log.debug3(DEBUG_HEADER + "field access url: "
            + md.get(MetadataField.FIELD_ACCESS_URL));
      }

      if (md.get(MetadataField.FIELD_ACCESS_URL) == null) {
        // temporary -- use full text url if not set
        // (should be set by metadata extractor)
        md.put(MetadataField.FIELD_ACCESS_URL, af.getFullTextUrl());
      }

      md.putRaw(MetadataField.FIELD_FEATURED_URL_MAP.getKey(), roles);

      // Check whether the the archival unit content must be obtained from the
      // repository.
      if (!isAuContentFromWs) {
	// Yes: Get the earliest fetch time of the metadata items URLs.
	long fetchTime = AuUtil.getAuUrlsEarliestFetchTime(au, roles.values());
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "fetchTime = " + fetchTime);

	md.put(MetadataField.FIELD_FETCH_TIME, String.valueOf(fetchTime));
      }

      try {
        validateDataAgainstTdb(new ArticleMetadataInfo(md), au);
        articleMetadataInfoBuffer.add(md);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      extractedCount++;
      indexedArticleCount++;
    }

    /**
     * Validate data against TDB information.
     * 
     * @param mdinfo
     *          the ArticleMetadataInfo
     * @param au
     *          An ArchivalUnit with the archival unit.
     * @throws ValidationException
     *           if field is invalid
     */
    private void validateDataAgainstTdb(ArticleMetadataInfo mdinfo,
        ArchivalUnit au) {
      HashSet<String> isbns = new HashSet<String>();
      if (mdinfo.isbn != null) {
        isbns.add(mdinfo.isbn);
      }

      if (mdinfo.eisbn != null) {
        isbns.add(mdinfo.eisbn);
      }

      HashSet<String> issns = new HashSet<String>();
      if (mdinfo.issn != null) {
        issns.add(mdinfo.issn);
      }

      if (mdinfo.eissn != null) {
        issns.add(mdinfo.eissn);
      }

      TdbAu tdbau = au.getTdbAu();
      boolean isTitleInTdb = !au.isBulkContent();
      String tdbauName = (tdbau == null) ? null : tdbau.getName();
      String tdbauStartYear = (tdbau == null) ? auName : tdbau.getStartYear();
      String tdbauYear = (tdbau == null) ? null : tdbau.getYear();
      String tdbauIsbn = null;
      String tdbauIssn = null;
      String tdbauEissn = null;
      String tdbauJournalTitle = null;

      // Check whether the TDB has title information.
      if (isTitleInTdb && (tdbau != null)) {
        // Yes: Get the title information from the TDB.
        tdbauIsbn = tdbau.getIsbn();
        tdbauIssn = tdbau.getPrintIssn();
        tdbauEissn = tdbau.getEissn();
        tdbauJournalTitle = tdbau.getPublicationTitle();
      }

      if (tdbau != null) {
        // Validate journal title against the TDB journal title.
        if (tdbauJournalTitle != null) {
          if (!tdbauJournalTitle.equals(mdinfo.publicationTitle)) {
            if (mdinfo.publicationTitle == null) {
              taskWarning("tdb title  is " + tdbauJournalTitle + " for "
                  + tdbauName + " -- metadata title is missing");
            } else {
              taskWarning("tdb title " + tdbauJournalTitle + " for "
                  + tdbauName + " -- does not match metadata journal title "
                  + mdinfo.publicationTitle);
            }
          }
        }

        // Validate ISBN against the TDB ISBN.
        if (tdbauIsbn != null) {
          if (!tdbauIsbn.equals(mdinfo.isbn)) {
            isbns.add(tdbauIsbn);
            if (mdinfo.isbn == null) {
              taskWarning("using tdb isbn " + tdbauIsbn + " for " + tdbauName
                  + " -- metadata isbn missing");
            } else {
              taskWarning("also using tdb isbn " + tdbauIsbn + " for "
                  + tdbauName + " -- different than metadata isbn: "
                  + mdinfo.isbn);
            }
          } else if (mdinfo.isbn != null) {
            taskWarning("tdb isbn missing for " + tdbauName + " -- should be: "
                + mdinfo.isbn);
          }
        } else if (mdinfo.isbn != null) {
          if (isTitleInTdb) {
            taskWarning("tdb isbn missing for " + tdbauName + " -- should be: "
                + mdinfo.isbn);
          }
        }

        // validate ISSN against the TDB ISSN.
        if (tdbauIssn != null) {
          if (tdbauIssn.equals(mdinfo.eissn) && (mdinfo.issn == null)) {
            taskWarning("tdb print issn " + tdbauIssn + " for " + tdbauName
                + " -- reported by metadata as eissn");
          } else if (!tdbauIssn.equals(mdinfo.issn)) {
            // add both ISSNs so it can be found either way
            issns.add(tdbauIssn);
            if (mdinfo.issn == null) {
              taskWarning("using tdb print issn " + tdbauIssn + " for "
                  + tdbauName + " -- metadata print issn is missing");
            } else {
              taskWarning("also using tdb print issn " + tdbauIssn + " for "
                  + tdbauName + " -- different than metadata print issn: "
                  + mdinfo.issn);
            }
          }
        } else if (mdinfo.issn != null) {
          if (mdinfo.issn.equals(tdbauEissn)) {
            taskWarning("tdb eissn " + tdbauEissn + " for " + tdbauName
                + " -- reported by metadata as print issn");
          } else if (isTitleInTdb) {
            taskWarning("tdb issn missing for " + tdbauName + " -- should be: "
                + mdinfo.issn);
          }
        }

        // Validate EISSN against the TDB EISSN.
        if (tdbauEissn != null) {
          if (tdbauEissn.equals(mdinfo.issn) && (mdinfo.eissn == null)) {
            taskWarning("tdb eissn " + tdbauEissn + " for " + tdbauName
                + " -- reported by metadata as print issn");
          } else if (!tdbauEissn.equals(mdinfo.eissn)) {
            // Add both ISSNs so that they can be found either way.
            issns.add(tdbauEissn);
            if (mdinfo.eissn == null) {
              taskWarning("using tdb eissn " + tdbauEissn + " for " + tdbauName
                  + " -- metadata eissn is missing");
            } else {
              taskWarning("also using tdb eissn " + tdbauEissn + " for "
                  + tdbauName + " -- different than metadata eissn: "
                  + mdinfo.eissn);
            }
          }
        } else if (mdinfo.eissn != null) {
          if (mdinfo.eissn.equals(tdbauIssn)) {
            taskWarning("tdb print issn " + tdbauIssn + " for " + tdbauName
                + " -- reported by metadata as print eissn");
          } else if (isTitleInTdb) {
            taskWarning("tdb eissn missing for " + tdbauName
                + " -- should be: " + mdinfo.eissn);
          }
        }

        // Validate publication date against the TDB year.
        String pubYear = mdinfo.pubYear;
        if (pubYear != null) {
          if (!tdbau.includesYear(mdinfo.pubYear)) {
            if (tdbauYear != null) {
              taskWarning("tdb year " + tdbauYear + " for " + tdbauName
                  + " -- does not match metadata year " + pubYear);
            } else {
              taskWarning("tdb year missing for " + tdbauName
                  + " -- should include year " + pubYear);
            }
          }
        } else {
          pubYear = tdbauStartYear;
          if (mdinfo.pubYear != null) {
            taskWarning("using tdb start year " + mdinfo.pubYear + " for "
                + tdbauName + " -- metadata year is missing");
          }
        }
      }
    }
  }

  /**
   * The handler for reindexing lifecycle events.
   */
  private class ReindexingEventHandler implements TaskCallback {
    private final Logger log = Logger.getLogger(ReindexingEventHandler.class);

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

      // TODO: handle task Success vs. failure?
      if (type == Schedule.EventType.START) {
        // Handle the start event.
        handleStartEvent(threadCpuTime, threadUserTime, currentClockTime);
      } else if (type == Schedule.EventType.FINISH) {
        // Handle the finish event.
        handleFinishEvent(task, threadCpuTime, threadUserTime,
                          currentClockTime);
      } else {
        log.error("Received unknown reindexing lifecycle event type '" + type
            + "' for AU '" + auName + "' - Ignored.");
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
      log.info("Starting reindexing task for AU '" + auName + "': isNewAu = "
	  + isNewAu + ", needFullReindex = " + needFullReindex + "...");

      // Remember the times at startup.
      startCpuTime = threadCpuTime;
      startUserTime = threadUserTime;
      startClockTime = currentClockTime;

      MetadataTarget target = MetadataTarget.OpenURL();
        
      // Only allow incremental extraction if the AU is not new and not doing a
      // full reindex.
      if (!isNewAu && !needFullReindex) {
	if (log.isDebug2())
	  log.debug2(DEBUG_HEADER + "lastExtractTime = " + lastExtractTime);
	// Indicate that only new metadata after the last extraction is to be
	// included.
	target.setIncludeFilesChangedAfter(lastExtractTime);
      }

      // The article iterator won't be null because only AUs with article
      // iterators are queued for processing.
      articleIterator = au.getArticleIterator(target);

      if (log.isDebug3()) {
        long articleIteratorInitTime = TimeBase.nowMs() - startClockTime;
        log.debug3(DEBUG_HEADER + "Reindexing task for AU '" + auName
            + "': has articles? " + articleIterator.hasNext()
            + ", initializing iterator took " + articleIteratorInitTime + "ms");
      }

      try {
        articleMetadataInfoBuffer =
	  new ArticleMetadataBuffer(new File(PlatformUtil.getSystemTempDir()));
        mdxManager.notifyStartReindexingAu(au);
      } catch (IOException ioe) {
        log.error("Failed to set up pending AU '" + auName
            + "' for re-indexing", ioe);
        setFinished();
        if (status == ReindexingStatus.Running) {
          status = ReindexingStatus.Rescheduled;
        }
      }
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
      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "AU '" + auName + "': status = " + status);

      if (mdxManager.isOnDemandMetadataExtractionOnly()) {
	// Get any exception thrown while getting the archival unit URLs.
	e = GetAuUrlsClient.getAndDeleteAnyException(auId);
        if (log.isDebug3()) log.debug3(DEBUG_HEADER + "e = " + e);

        // Check whether an exception was thrown.
        if (e != null) {
          // Yes: If the URLs could not be obtained successfully, the process
          // failed. This could be because, for example, the AU was never
          // crawled.
          status = ReindexingStatus.Failed;
	}
      }

      if (status == ReindexingStatus.Running) {
        status = ReindexingStatus.Success;
      }

      Connection conn = null;
      startUpdateClockTime = currentClockTime;

      switch (status) {
        case Success:

          try {
            long removedArticleCount = 0L;

            // Get a connection to the database.
            conn = dbManager.getConnection();

            if (log.isDebug3())
              log.debug3(DEBUG_HEADER + "needFullReindex = " + needFullReindex);

            // Check whether the reindexing task is not incremental.
            if (needFullReindex) { 
              // Yes: Remove the old Archival Unit metadata before adding the
              // new metadata.
              removedArticleCount =
        	  mdxManagerSql.removeAuMetadataItems(conn, auId);
              log.info("Reindexing task for AU '" + auName + "' removed "
        	  + removedArticleCount + " database items.");
            }

            Iterator<ArticleMetadataInfo> mditr =
                articleMetadataInfoBuffer.iterator();

            // Check whether the reindexing task is not incremental and no
            // items were extracted.
            if (needFullReindex && !mditr.hasNext()) { 
              // Yes: Report the problem.
              log.warning("Non-incremental reindexing task for AU '" + auName
        	  + "' failed to extract any items.");
            }

            // Check whether there is any metadata to record.
            if (mditr.hasNext()) {
              // Yes: Write the AU metadata to the database.
              new AuMetadataRecorder((ReindexingTask) task, mdxManager, au)
              .recordMetadata(conn, mditr);
              
              pokeWDog();
            } else {
              // No: Record the extraction in the database.
              new AuMetadataRecorder((ReindexingTask) task, mdxManager, au)
              .recordMetadataExtraction(conn);
            }

            // Remove the AU just re-indexed from the list of AUs pending to be
            // re-indexed.
            mdxManagerSql.removeFromPendingAus(conn, auId);
            mdxManager.updatePendingAusCount(conn);

            // Complete the database transaction.
            MetadataDbManager.commitOrRollback(conn, log);

            // Update the successful re-indexing count.
            mdxManager.addToSuccessfulReindexingTasks();
            
            // Update the total article count.
            mdxManager.addToMetadataArticleCount(updatedArticleCount
        	- removedArticleCount);

            // Check whether the reindexing task is not incremental.
            if (needFullReindex) { 
              log.info("Reindexing task for AU '" + auName + "' added "
        	  + updatedArticleCount + " database articles.");
            } else {
              log.info("Reindexing task for AU '" + auName + "' updated "
        	  + updatedArticleCount + " database articles.");
            }

            break;
          } catch (MetadataIndexingException me) {
            e = me;
            log.warning("Error updating metadata at FINISH for " + status
                + " -- NOT rescheduling", e);
            log.warning("ArticleMetadataInfo = " + me.getArticleMetadataInfo());
            status = ReindexingStatus.Failed;
          } catch (DbException dbe) {
            e = dbe;
            String message = "Error updating metadata at FINISH for " + status;

            if (mdxManager.isOnDemandMetadataExtractionOnly()) {
              status = ReindexingStatus.Failed;
              message = message + " -- NOT rescheduling";
            } else {
              status = ReindexingStatus.Rescheduled;
              message = message + " -- rescheduling";
            }

            log.warning(message, e);
          } catch (RuntimeException re) {
            e = re;
            log.warning("Error updating metadata at FINISH for " + status
                + " -- NOT rescheduling", e);
            status = ReindexingStatus.Failed;
          } finally {
            MetadataDbManager.safeRollbackAndClose(conn);
          }

          // Fall through if SQL exception occurred during update.
        case Failed:
        case Rescheduled:
          if (!mdxManager.isOnDemandMetadataExtractionOnly()) {
            // Reindexing not successful, so try again later if status indicates
            // the operation should be rescheduled.
            if (log.isDebug3()) log.debug3(DEBUG_HEADER
        	+ "Reindexing task for AU '" + auName
        	+ "' was unsuccessful: status = " + status);

            mdxManager.addToFailedReindexingTasks(ReindexingTask.this);

            try {
              // Get a connection to the database.
              conn = dbManager.getConnection();

              mdxManagerSql.removeFromPendingAus(conn, au.getAuId());
              mdxManager.updatePendingAusCount(conn);

              if (status == ReindexingStatus.Failed) {
        	if (log.isDebug3()) log.debug3(DEBUG_HEADER
        	    + "Marking as failed the reindexing task for AU '" + auName
        	    + "'");

        	// Add the failed AU to the pending list with the right priority
        	// to avoid processing it again before the underlying problem is
        	// fixed.
        	mdxManagerSql.addFailedIndexingAuToPendingAus(conn,
        	    au.getAuId());
              } else if (status == ReindexingStatus.Rescheduled) {
        	if (log.isDebug3()) log.debug3(DEBUG_HEADER
        	    + "Rescheduling the reindexing task AU '" + auName + "'");

        	// Add the re-schedulable AU to the end of the pending list.
        	mdxManager.addToPendingAusIfNotThere(conn,
        	    Collections.singleton(au), needFullReindex);
              }

              // Complete the database transaction.
              MetadataDbManager.commitOrRollback(conn, log);
            } catch (DbException dbe) {
              log.warning("Error updating pending queue at FINISH for AU '"
        	  + auName + "', status = " + status, dbe);
            } finally {
              MetadataDbManager.safeRollbackAndClose(conn);
            }
          }

          break;
        default:
          log.warning("Unexpected status '" + status + "'");
      }

      articleIterator = null;
      endClockTime = TimeBase.nowMs();

      if (tmxb.isCurrentThreadCpuTimeSupported()) {
        endCpuTime = tmxb.getCurrentThreadCpuTime();
        endUserTime = tmxb.getCurrentThreadUserTime();
      }

      // Display timings.
      long elapsedCpuTime = threadCpuTime - startCpuTime;
      long elapsedUserTime = threadUserTime - startUserTime;
      long elapsedClockTime = currentClockTime - startClockTime;

      log.info("Finished reindexing task for AU '" + auName + "': status = "
	  + status + ", CPU time: " + elapsedCpuTime / 1.0e9 + " ("
	  + endCpuTime / 1.0e9 + "), User time: " + elapsedUserTime / 1.0e9
	  + " (" + endUserTime / 1.0e9 + "), Clock time: "
	  + elapsedClockTime / 1.0e3 + " (" + endClockTime / 1.0e3 + ")");

      // Release collected metadata info once finished.
      articleMetadataInfoBuffer.close();
      articleMetadataInfoBuffer = null;

      if (!mdxManager.isOnDemandMetadataExtractionOnly()) {
	synchronized (mdxManager.activeReindexingTasks) {
	  mdxManager.activeReindexingTasks.remove(au.getAuId());
	  mdxManager.notifyFinishReindexingAu(au, status, task.getException());

	  try {
            // Get a connection to the database.
            conn = dbManager.getConnection();

            // Schedule another task if available.
            mdxManager.startReindexing(conn);

            // Complete the database transaction.
            MetadataDbManager.commitOrRollback(conn, log);
          } catch (DbException dbe) {
            log.error("Cannot restart indexing", dbe);
          } finally {
            MetadataDbManager.safeRollbackAndClose(conn);
          }
        }
      } else {
	mdxManager.notifyFinishReindexingAu(au, status, task.getException());
      }
    }
  }

  /**
   * Sets the indication of whether the AU being indexed is new to the index.
   * 
   * @param isNew
   *          A boolean with the indication of whether the AU being indexed is
   *          new to the index.
   */
  void setNewAu(boolean isNew) {
    isNewAu = isNew;
  }

  /**
   * Sets the full re-indexing state of this task.
   * 
   * @param enable
   *          A boolean with the full re-indexing state of this task.
   */
  void setFullReindex(boolean enable) {
    needFullReindex = enable;
  }

  /**
   * Sets the last extraction time for the AU of this task.
   * 
   * @param time
   *          A boolean with the full re-indexing state of this task.
   */
  void setLastExtractTime(long time) {
    lastExtractTime = time;
  }

  /**
   * Provides an indication of whether the task has been cancelled.
   * 
   * @return a boolean with <code>true</code> if the task has been cancelled,
   *         <code>false</code> otherwise.
   */
  boolean isCancelled() {
    return cancelled;
  }
}
