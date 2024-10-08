/*

Copyright (c) 2013-2020 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.metadata.SqlConstants.*;
import static org.lockss.metadata.extractor.job.SqlConstants.AU_KEY_COLUMN;
import static org.lockss.metadata.extractor.job.SqlConstants.END_TIME_COLUMN;
import static org.lockss.metadata.extractor.job.SqlConstants.JOB_TYPE_SEQ_COLUMN;
import static org.lockss.metadata.extractor.job.SqlConstants.PLUGIN_ID_COLUMN;
import static org.lockss.metadata.extractor.job.SqlConstants.START_TIME_COLUMN;
import static org.lockss.metadata.extractor.job.SqlConstants.STATUS_MESSAGE_COLUMN;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lockss.app.BaseLockssManager;
import org.lockss.app.ConfigurableManager;
import org.lockss.app.LockssApp;
import org.lockss.config.Configuration;
import org.lockss.config.Configuration.Differences;
import org.lockss.daemon.LockssRunnable;
import org.lockss.daemon.status.StatusService;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.extractor.ArticleMetadata;
import org.lockss.extractor.ArticleMetadataExtractor;
import org.lockss.extractor.BaseArticleMetadataExtractor;
import org.lockss.extractor.MetadataField;
import org.lockss.extractor.MetadataTarget;
import org.lockss.metadata.Isbn;
import org.lockss.metadata.Issn;
import org.lockss.metadata.ItemMetadata;
import org.lockss.metadata.MetadataConstants;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.MetadataManager;
import org.lockss.metadata.ArticleMetadataBuffer;
import org.lockss.metadata.ArticleMetadataBuffer.ArticleMetadataInfo;
import org.lockss.metadata.AuMetadataRecorder;
import org.lockss.metadata.extractor.job.JobAuStatus;
import org.lockss.metadata.extractor.job.JobManager;
import org.lockss.metadata.extractor.job.SqlConstants;
import org.lockss.metadata.query.MetadataQueryManager;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuUtil;
import org.lockss.plugin.Plugin;
import org.lockss.plugin.Plugin.Feature;
import org.lockss.plugin.PluginManager;
import org.lockss.scheduler.Schedule;
import org.lockss.state.AuStateBean;
import org.lockss.state.StateManager;
import org.lockss.state.SubstanceChecker;
import org.lockss.util.Constants;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.lockss.util.time.TimeBase;
import org.lockss.util.PatternIntMap;
import org.lockss.util.os.PlatformUtil;

/**
 * Implementation of a manager for extracting metadata from Archival Units.
 */
public class MetadataExtractorManager extends BaseLockssManager implements
    ConfigurableManager {

  private static Logger log = Logger.getLogger(MetadataExtractorManager.class);

  /** prefix for config properties */
  public static final String PREFIX = MetadataConstants.PREFIX;

  /**
   * Determines whether MedataExtractor specified by plugin should be used if it
   * is available. If <code>false</code>, a MetaDataExtractor is created that
   * returns data from the TDB rather than from the content metadata. This is
   * faster than extracting metadata form content, but less complete. Use only
   * when minimal article info is required.
   */
  public static final String PARAM_USE_METADATA_EXTRACTOR = PREFIX
      + "use_metadata_extractor";

  /**
   * Default value of MetadataExtractorManager use_metadata_extractor
   * configuration parameter; <code>true</code> to use specified
   * MetadataExtractor.
   */
  public static final boolean DEFAULT_USE_METADATA_EXTRACTOR = true;

  /**
   * Determines whether indexing should be enabled. If indexing is not enabled,
   * AUs are queued for indexing, but the AUs are not reindexed until the
   * process is re-enabled. This parameter can be changed at runtime.
   */
  public static final String PARAM_INDEXING_ENABLED = PREFIX
      + "indexing_enabled";

  /**
   * Default value of MetadataExtractorManager indexing enabled configuration
   * parameter; <code>false</code> to disable, <code>true</code> to enable.
   */
  public static final boolean DEFAULT_INDEXING_ENABLED = false;

  /**
   * The maximum number of concurrent reindexing tasks. This property can be
   * changed at runtime
   */
  public static final String PARAM_MAX_REINDEXING_TASKS = PREFIX
      + "maxReindexingTasks";

  /** Default maximum concurrent reindexing tasks */
  public static final int DEFAULT_MAX_REINDEXING_TASKS = 1;

  /** Disable allowing crawl to interrupt reindexing tasks */
  public static final String PARAM_DISABLE_CRAWL_RESCHEDULE_TASK = PREFIX
      + "disableCrawlRescheduleTask";

  /** Default disable allowing crawl to interrupt reindexing tasks */
  public static final boolean DEFAULT_DISABLE_CRAWL_RESCHEDULE_TASK = false;

  /**
   * The maximum number reindexing task history. This property can be changed at
   * runtime
   */
  public static final String PARAM_HISTORY_MAX = PREFIX + "historySize";

  /** Indexing task watchdog name */
  static final String WDOG_PARAM_INDEXER = "MetadataIndexer";
  /** Indexing task watchdog default timeout */
  static final long WDOG_DEFAULT_INDEXER = 6 * Constants.HOUR;

  /** Default maximum reindexing tasks history */
  public static final int DEFAULT_HISTORY_MAX = 200;

  /**
   * The maximum size of pending AUs list returned by 
   * {@link #getPendingReindexingAus()}.
   */
  private static final String PARAM_PENDING_AU_LIST_SIZE = PREFIX
      + "maxPendingAuListSize";

  /** 
   * The default maximum size of pending AUs list returned by 
   * {@link #getPendingReindexingAus()}.
   */
  private static final int DEFAULT_PENDING_AU_LIST_SIZE = 200;
  
  /**
   * Determines whether indexing new AUs is prioritized ahead of 
   * reindexing exiting AUs.
   */
  public static final String PARAM_PRIORTIZE_INDEXING_NEW_AUS = PREFIX
      + "prioritizeIndexingNewAus";

  /**
   * The default for prioritizing indexing of new AUs ahead of 
   * reindexing existing AUs
   */
  public static final boolean DEFAULT_PRIORTIZE_INDEXING_NEW_AUS = true;

  /** Map of AUID regexp to priority.  If set, AUs are assigned the
   * corresponding priority of the first regexp that their AUID matches.
   * Priority must be an integer; priorities <= -10000 mean "do not index
   * matching AUs", priorities <= -20000 mean "abort running indexes of
   * matching AUs". (Priorities are not yet implemented - only "do not
   * index" and "abort" are supported.)  */
  static final String PARAM_INDEX_PRIORITY_AUID_MAP =
    PREFIX + "indexPriorityAuidMap";
  static final List<String> DEFAULT_INDEX_PRIORITY_AUID_MAP = null;

  static final int FAILED_INDEX_PRIORITY = -1000;
  static final int MIN_INDEX_PRIORITY = -10000;
  private static final int ABORT_INDEX_PRIORITY = -20000;

  /** Maximum number of AUs to be re-indexed to batch before writing them to the
   * database. */
  public static final String PARAM_MAX_PENDING_TO_REINDEX_AU_BATCH_SIZE =
    PREFIX + "maxPendingToReindexAuBatchSize";
  private static final int DEFAULT_MAX_PENDING_TO_REINDEX_AU_BATCH_SIZE = 1000;

  /**
   * Mandatory metadata fields.
   */
  static final String PARAM_MANDATORY_FIELDS = PREFIX + "mandatoryFields";
  static final List<String> DEFAULT_MANDATORY_FIELDS = null;

  /**
   * Determines whether metadata indexing happens only "on-demand". Set this to
   * <code>true</code> for the metadata extraction via REST web service.
   */
  static final String PARAM_ON_DEMAND_METADATA_EXTRACTION_ONLY = PREFIX
      + "onDemandMetadataExtractionOnly";
  static final boolean DEFAULT_ON_DEMAND_METADATA_EXTRACTION_ONLY = false;

  /**
   * The Metadata REST web service parameters.
   */
  static final String PARAM_MD_REST_TIMEOUT_VALUE =
      MetadataConstants.MD_REST_PREFIX + "timeoutValue";
  static final int DEFAULT_MD_REST_TIMEOUT_VALUE = 600;
  static final String PARAM_MD_REST_USER_NAME =
      MetadataConstants.MD_REST_PREFIX + "userName";
  static final String PARAM_MD_REST_PASSWORD =
      MetadataConstants.MD_REST_PREFIX + "password";

  /**
   * The interval in milliseconds between consecutive runs of the metadata
   * extraction check.
   */
  static final String PARAM_METADATA_EXTRACTION_CHECK_INTERVAL =
      PREFIX + "metadataExtractionCheckInterval";

  /**
   * The default interval in milliseconds between consecutive runs of the
   * metadata extraction check (Daily).
   */
  static final long DEFAULT_METADATA_EXTRACTION_CHECK_INTERVAL = Constants.DAY;

  /**
   * Map of running reindexing tasks keyed by their AuIds
   */
  final Map<String, ReindexingTask> activeReindexingTasks =
      new HashMap<String, ReindexingTask>();

  /**
   * List of reindexing tasks in order from most recent (0) to least recent.
   */
  final List<ReindexingTask> reindexingTaskHistory =
      new LinkedList<ReindexingTask>();
  
  /**
   * List of reindexing tasks that have failed or been rescheduled,
   * from most recent (0) to least recent. Only the most recent failed
   * task for a given AU is retained
   */
  final List<ReindexingTask> failedReindexingTasks = 
      new LinkedList<ReindexingTask>();

  /**
   * Metadata manager indexing enabled flag.  Initial value should always
   * be false, independent of DEFAULT_INDEXING_ENABLED, so
   * setIndexingEnabled() sees a transition.
   */
  boolean reindexingEnabled = false;

  /**
   * XXX temporary one-time startup
   */
  boolean everEnabled = false;

  /**
   * Metadata manager use metadata extractor flag. Note: set this to false only
   * where specific metadata from the metadata extractor are not needed.
   */
  boolean useMetadataExtractor = DEFAULT_USE_METADATA_EXTRACTOR;

  /** Maximum number of reindexing tasks */
  int maxReindexingTasks = DEFAULT_MAX_REINDEXING_TASKS;

  /** Disable crawl completion rescheduling a running task for same AU */
  boolean disableCrawlRescheduleTask = DEFAULT_DISABLE_CRAWL_RESCHEDULE_TASK;

  // The number of articles currently in the metadata database.
  private long metadataArticleCount = 0;
  
  // The number of publishers currently in the metadata database
  // (-1 indicates needs recalculation)
  private long metadataPublisherCount = -1;
  
  // The number of providers currently in the metadata database
  // (-1 indicates needs recalculation)
  private long metadataProviderCount = -1;
  
  // The number of AUs pending to be reindexed.
  private long pendingAusCount = 0;

  // the maximum size of the pending AUs list returned by 
  // {@link #getPendingReindexingAus()}
  private int pendingAuListSize = DEFAULT_PENDING_AU_LIST_SIZE;
  
  private boolean prioritizeIndexingNewAus = 
      DEFAULT_PRIORTIZE_INDEXING_NEW_AUS;
  
  // The number of successful reindexing operations.
  private long successfulReindexingCount = -1;

  // The number of failed reindexing operations.
  private long failedReindexingCount = -1;

  private int maxReindexingTaskHistory = DEFAULT_HISTORY_MAX;

  // The plugin manager.
  private PluginManager pluginMgr = null;

  // The database manager.
  private MetadataDbManager dbManager = null;

  private MetadataManager mdManager;
  private JobManager jobMgr;
  private StateManager stateManager;

  private PatternIntMap indexPriorityAuidMap;

  private int maxPendingAuBatchSize =
      DEFAULT_MAX_PENDING_TO_REINDEX_AU_BATCH_SIZE;

  private int pendingAuBatchCurrentSize = 0;

  /** enumeration status for reindexing tasks */
  public enum ReindexingStatus {
    /**
     * The reindexing task is running.
     */
    Running,
    /**
     * The reindexing task was successful.
     */
    Success,
    /**
     * The reindexing task failed.
     */
    Failed,
    /**
     * The reindexing task was rescheduled.
     */
    Rescheduled
  };

  // The metadata extractor manager SQL code executor.
  private MetadataExtractorManagerSql mdxManagerSql;

  private List<String> mandatoryMetadataFields = DEFAULT_MANDATORY_FIELDS;

  private boolean onDemandMetadataExtractionOnly =
      DEFAULT_ON_DEMAND_METADATA_EXTRACTION_ONLY;

  private long metadataExtractionCheckInterval =
      DEFAULT_METADATA_EXTRACTION_CHECK_INTERVAL;

  /**
   * No-argument constructor.
   */
  public MetadataExtractorManager() {
  }

  /**
   * Constructor used for generating a testing database.
   *
   * @param dbManager
   *          A MetadataDbManager with the database manager to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public MetadataExtractorManager(MetadataDbManager dbManager)
      throws DbException {
    pluginMgr = new PluginManager();
    this.dbManager = dbManager;
    mdManager = new MetadataManager(dbManager);
    mdxManagerSql = new MetadataExtractorManagerSql(dbManager, this);
  }

  /**
   * Starts the MetadataExtractorManager service.
   */
  @Override
  public void startService() {
    super.startService();
    final String DEBUG_HEADER = "startService(): ";
    log.debug(DEBUG_HEADER + "Starting MetadataExtractorManager");

    pluginMgr = getManagerByType(PluginManager.class);
    dbManager = getManagerByType(MetadataDbManager.class);
    mdManager = getManagerByType(MetadataManager.class);
    jobMgr = getManagerByType(JobManager.class);
    stateManager = getManagerByType(StateManager.class);

    try {
      mdxManagerSql = new MetadataExtractorManagerSql(dbManager, this);
    } catch (DbException dbe) {
      log.error("Cannot obtain MetadataManagerSql", dbe);
      return;
    }

    // Initialize the counts of articles and pending AUs from database.
    try {
      pendingAusCount = mdxManagerSql.getEnabledPendingAusCount();
      metadataArticleCount = mdxManagerSql.getArticleCount();
      metadataPublisherCount = mdxManagerSql.getPublisherCount();
      metadataProviderCount = mdxManagerSql.getProviderCount();
    } catch (DbException dbe) {
      log.error("Cannot get pending AUs and counts", dbe);
    }

    StatusService statusServ = getApp().getManagerByType(StatusService.class);
    statusServ.registerStatusAccessor(
	MetadataManager.METADATA_STATUS_TABLE_NAME,
        new MetadataManagerStatusAccessor(this));
    statusServ.registerOverviewAccessor(
	MetadataManager.METADATA_STATUS_TABLE_NAME,
        new MetadataIndexingOverviewAccessor(this));

    resetConfig();
    log.debug(DEBUG_HEADER
	+ "MetadataExtractorManager service successfully started");
  }

  /** Start the starter thread, which waits for AUs to be started,
   * registers AuEvent handler and performs initial scan of AUs
   */
  void startStarter() {
    MetadataIndexingStarter starter = new MetadataIndexingStarter(dbManager,
	this, pluginMgr, jobMgr, metadataExtractionCheckInterval);
    new Thread(starter).start();
  }

  /**
   * Handles new configuration.
   * 
   * @param config
   *          the new configuration
   * @param prevConfig
   *          the previous configuration
   * @param changedKeys
   *          the configuration keys that changed
   */
  @Override
  public void setConfig(Configuration config, Configuration prevConfig,
      Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      useMetadataExtractor =
	  config.getBoolean(PARAM_USE_METADATA_EXTRACTOR,
			    DEFAULT_USE_METADATA_EXTRACTOR);
      maxReindexingTasks =
	  Math.max(0, config.getInt(PARAM_MAX_REINDEXING_TASKS,
				    DEFAULT_MAX_REINDEXING_TASKS));
      disableCrawlRescheduleTask =
	  config.getBoolean(PARAM_DISABLE_CRAWL_RESCHEDULE_TASK,
			    DEFAULT_DISABLE_CRAWL_RESCHEDULE_TASK);
      pendingAuListSize = 
          Math.max(0, config.getInt(PARAM_PENDING_AU_LIST_SIZE, 
                                    DEFAULT_PENDING_AU_LIST_SIZE));
      prioritizeIndexingNewAus =
          config.getBoolean(PARAM_PRIORTIZE_INDEXING_NEW_AUS,
                            DEFAULT_PRIORTIZE_INDEXING_NEW_AUS);

      if (changedKeys.contains(PARAM_ON_DEMAND_METADATA_EXTRACTION_ONLY)) {
	onDemandMetadataExtractionOnly =
	    config.getBoolean(PARAM_ON_DEMAND_METADATA_EXTRACTION_ONLY,
		DEFAULT_ON_DEMAND_METADATA_EXTRACTION_ONLY);

	if (log.isDebug3())
	  log.debug3("onDemandMetadataExtractionOnly = "
	      + onDemandMetadataExtractionOnly);
      }

      if (isAppInited() && !onDemandMetadataExtractionOnly) {
	metadataExtractionCheckInterval =
	    config.getLong(PARAM_METADATA_EXTRACTION_CHECK_INTERVAL,
		DEFAULT_METADATA_EXTRACTION_CHECK_INTERVAL);
	boolean doEnable =
	  config.getBoolean(PARAM_INDEXING_ENABLED, DEFAULT_INDEXING_ENABLED);
	setIndexingEnabled(doEnable);
      }

      if (changedKeys.contains(PARAM_HISTORY_MAX)) {
	int histSize = config.getInt(PARAM_HISTORY_MAX, DEFAULT_HISTORY_MAX);
	setMaxHistory(histSize);
      }

      if (changedKeys.contains(PARAM_INDEX_PRIORITY_AUID_MAP)) {
	installIndexPriorityAuidMap((List<String>) (config
	    .getList(PARAM_INDEX_PRIORITY_AUID_MAP,
		     DEFAULT_INDEX_PRIORITY_AUID_MAP)));

	if (isAppInited() && !onDemandMetadataExtractionOnly) {
	  processAbortPriorities();
	  // process queued AUs in case any are newly eligible if initialized.
	  if (dbManager != null) {
	    startReindexing();
	  }
	}
      }

      if (changedKeys.contains(PARAM_MAX_PENDING_TO_REINDEX_AU_BATCH_SIZE)) {
	maxPendingAuBatchSize =
	    config.getInt(PARAM_MAX_PENDING_TO_REINDEX_AU_BATCH_SIZE,
			  DEFAULT_MAX_PENDING_TO_REINDEX_AU_BATCH_SIZE);
      }

      if (changedKeys.contains(PARAM_MANDATORY_FIELDS)) {
	mandatoryMetadataFields =
	    (List<String>)config.getList(PARAM_MANDATORY_FIELDS,
		DEFAULT_MANDATORY_FIELDS);

	if (log.isDebug3())
	  log.debug3("mandatoryMetadataFields = " + mandatoryMetadataFields);
      }
    }
  }

  /**
   * Sets the indexing enabled state of this manager.
   * 
   * @param enable
   *          A boolean with the new enabled state of this manager.
   */
  void setIndexingEnabled(boolean enable) {
    final String DEBUG_HEADER = "setIndexingEnabled(): ";
    log.debug(DEBUG_HEADER + "enabled: " + enable);

    // Start or stop reindexing if initialized.
    if (dbManager != null) {
      if (!reindexingEnabled && enable) {
	if (everEnabled) {
        // Start reindexing
	  startReindexing();
	} else {
	  // start first-time startup thread (XXX needs to be periodic?)
	  // This is used also in testing.
	  startStarter();
	  everEnabled = true;
	}
	reindexingEnabled = enable;
      } else if (reindexingEnabled && !enable) {
        // Stop any pending reindexing operations
        stopReindexing();
      }
      reindexingEnabled = enable;
    }
  }

  /**
   * Sets the maximum reindexing task history list size.
   * 
   * @param maxSize
   *          An int with the maximum reindexing task history list size.
   */
  private void setMaxHistory(int maxSize) {
    maxReindexingTaskHistory = maxSize;

    synchronized (reindexingTaskHistory) {
      while (reindexingTaskHistory.size() > maxReindexingTaskHistory) {
        reindexingTaskHistory.remove(maxReindexingTaskHistory);
      }
    }
    synchronized(failedReindexingTasks) {
      while (failedReindexingTasks.size() > maxReindexingTaskHistory) {
        failedReindexingTasks.remove(maxReindexingTaskHistory);
      }
    }
  }

  /**
   * Sets up the index priority map.
   * 
   * @param patternPairs A List<String> with the patterns.
   */
  private void installIndexPriorityAuidMap(List<String> patternPairs) {
    if (patternPairs == null) {
      log.debug("Installing empty index priority map");
      indexPriorityAuidMap = PatternIntMap.EMPTY;
    } else {
      try {
	indexPriorityAuidMap = new PatternIntMap(patternPairs);
	log.debug("Installing index priority map: " + indexPriorityAuidMap);
      } catch (IllegalArgumentException e) {
	log.error("Illegal index priority map, ignoring", e);
	log.error("Index priority map unchanged: " + indexPriorityAuidMap);
      }
    }
  }

  /**
   * For all the entries in indexPriorityAuidMap whose priority is less than
   * ABORT_INDEX_PRIORITY, collect those that are new (not in
   * prevIndexPriorityAuidMap), then abort all indexes matching any of those
   * patterns.
   */
  private void processAbortPriorities() {
    List<ArchivalUnit> abortAus = new ArrayList<ArchivalUnit>();

    synchronized (activeReindexingTasks) {
      for (ReindexingTask task : activeReindexingTasks.values()) {
	ArchivalUnit au = task.getAu();

	if (indexPriorityAuidMap.getMatch(au.getAuId()) <
	    ABORT_INDEX_PRIORITY) {
	  abortAus.add(au);
	}
      }
    }

    for (ArchivalUnit au : abortAus) {
      log.info("Aborting indexing: " + au);
      cancelAuTask(au.getAuId());
    }
  }

  /**
   * Ensures that as many reindexing tasks as possible are running if the
   * manager is enabled.
   * 
   * @return an int with the number of reindexing tasks started.
   */
  private int startReindexing() {
    final String DEBUG_HEADER = "startReindexing(): ";

    Connection conn = null;
    int count = 0;
    try {
      conn = dbManager.getConnection();
      count = startReindexing(conn);
      MetadataDbManager.commitOrRollback(conn, log);
    } catch (DbException dbe) {
      log.error("Cannot start reindexing", dbe);
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
    }

    log.debug3(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Stops any pending reindexing operations.
   */
  private void stopReindexing() {
    final String DEBUG_HEADER = "stopReindexing(): ";
    log.debug(DEBUG_HEADER + "Number of reindexing tasks being stopped: "
        + activeReindexingTasks.size());

    // Quit any running reindexing tasks.
    synchronized (activeReindexingTasks) {
      for (ReindexingTask task : activeReindexingTasks.values()) {
        task.cancel();
      }

      activeReindexingTasks.clear();
    }
  }

  /**
   * Cancels the reindexing task for the specified AU.
   * 
   * @param auId
   *          A String with the AU identifier.
   * @return a boolean with <code>true</code> if task was canceled,
   *         <code>false</code> otherwise.
   */
  private boolean cancelAuTask(String auId) {
    final String DEBUG_HEADER = "cancelAuTask(): ";
    ReindexingTask task = activeReindexingTasks.get(auId);

    if (task != null) {
      // task cancellation will remove task and schedule next one
      log.debug2(DEBUG_HEADER + "Canceling pending reindexing task for auId "
	  + auId);
      task.cancel();
      return true;
    }

    return false;
  }

  /**
   * Ensures that as many re-indexing tasks as possible are running if the
   * manager is enabled.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return an int with the number of reindexing tasks started.
   */
  int startReindexing(Connection conn) {
    final String DEBUG_HEADER = "startReindexing(): ";
    if (log.isDebug2()) log.debug2("Starting...");

    if (!isAppInited()) {
      if (log.isDebug()) log.debug(DEBUG_HEADER
	  + "Daemon not initialized: No reindexing tasks.");
      return 0;
    }

    // Don't run reindexing tasks run if reindexing is disabled.
    if (!reindexingEnabled) {
      if (log.isDebug()) log.debug(DEBUG_HEADER
	  + "Metadata manager reindexing is disabled: No reindexing tasks.");
      return 0;
    }

    int reindexedTaskCount = 0;

    synchronized (activeReindexingTasks) {
      // Try to add more concurrent reindexing tasks as long as the maximum
      // number of them is not reached.
      while (activeReindexingTasks.size() < maxReindexingTasks) {
	if (log.isDebug3()) {
	  log.debug3("activeReindexingTasks.size() = "
	      + activeReindexingTasks.size());
	  log.debug3("maxReindexingTasks = " + maxReindexingTasks);
	}
        // Get the list of pending AUs to reindex.
	List<PrioritizedAuId> auIdsToReindex = new ArrayList<PrioritizedAuId>();

	if (pluginMgr != null) {
	  auIdsToReindex = mdxManagerSql.getPrioritizedAuIdsToReindex(conn,
	      maxReindexingTasks - activeReindexingTasks.size(),
	      prioritizeIndexingNewAus);
	  if (log.isDebug3()) log.debug3("auIdsToReindex.size() = "
	      + auIdsToReindex.size());
	}

	// Nothing more to do if there are no pending AUs to reindex.
        if (auIdsToReindex.isEmpty()) {
          break;
        }

        // Loop through all the pending AUs. 
        for (PrioritizedAuId auIdToReindex : auIdsToReindex) {
	  if (log.isDebug3())
	    log.debug3("auIdToReindex.auId = " + auIdToReindex.auId);

          // Get the next pending AU.
          ArchivalUnit au = pluginMgr.getAuFromId(auIdToReindex.auId);
	  if (log.isDebug3()) log.debug3("au = " + au);

          // Check whether it does not exist.
          if (au == null) {
	    // Yes: Cancel any running tasks associated with the AU and delete
	    // the AU metadata.
            try {
              deleteAu(conn, auIdToReindex.auId);
            } catch (DbException dbe) {
	      log.error("Error removing AU for auId " + auIdToReindex.auId
		  + " from the database", dbe);
            }
          } else {
            // No: Get the metadata extractor.
            ArticleMetadataExtractor ae = getMetadataExtractor(au);

            // Check whether it does not exist.
            if (ae == null) {
	      // Yes: It shouldn't happen because it was checked before adding
	      // the AU to the pending AUs list.
	      log.debug(DEBUG_HEADER + "Not running reindexing task for AU '"
		  + au.getName() + "' because it nas no metadata extractor");

	      // Remove it from the table of pending AUs.
              try {
        	pendingAusCount =
        	    mdxManagerSql.removeFromPendingAus(conn, au.getAuId());
              } catch (DbException dbe) {
                log.error("Error removing AU " + au.getName()
                          + " from the table of pending AUs", dbe);
                break;
              }
            } else {
              // No: Schedule the pending AU.
              if (log.isDebug3()) log.debug3(DEBUG_HEADER
        	  + "Creating the reindexing task for AU: " + au.getName());
              ReindexingTask task = new ReindexingTask(au, ae);

              // Get the AU database identifier, if any.
              Long auSeq = null;

              try {
        	auSeq = findAuSeq(conn, au.getAuId());
        	if (log.isDebug3())
        	  log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);
              } catch (DbException dbe) {
                log.error("Error trying to locate in the database AU "
                    + au.getName(), dbe);
                break;
              }

              task.setNewAu(auSeq == null);

              if (auSeq != null) {
                // Only allow incremental extraction if not doing full reindex
                boolean fullReindex = true;

        	try {
        	  fullReindex = mdxManagerSql.needAuFullReindexing(conn, au);
        	  if (log.isDebug3())
        	    log.debug3(DEBUG_HEADER + "fullReindex = " + fullReindex);
                } catch (DbException dbe) {
                  log.warning("Error getting from the database the full "
                      + "re-indexing flag for AU " + au.getName()
                      + ": Doing full re-index", dbe);
                }

        	if (fullReindex) {
        	  task.setFullReindex(fullReindex);
        	} else {
        	  long lastExtractTime = 0;

        	  try {
        	    lastExtractTime =
        		mdxManagerSql.getAuExtractionTime(conn, au);
        	    if (log.isDebug3()) log.debug3(DEBUG_HEADER
        		+ "lastExtractTime = " + lastExtractTime);
        	  } catch (DbException dbe) {
        	    log.warning("Error getting the last extraction time for AU "
        		+ au.getName() + ": Doing a full re-index", dbe);
        	  }

        	  task.setLastExtractTime(lastExtractTime);
        	}
              }

              activeReindexingTasks.put(au.getAuId(), task);

              // Add the reindexing task to the history; limit history list
              // size.
              addToIndexingTaskHistory(task);

	      log.debug(DEBUG_HEADER + "Running the reindexing task for AU: "
		  + au.getName());
              runReindexingTask(task);
              reindexedTaskCount++;
            }
          }
        }
      }
    }

    log.debug(DEBUG_HEADER + "Started " + reindexedTaskCount
              + " AU reindexing tasks");
    return reindexedTaskCount;
  }

  /**
   * This class returns the information about an AU to reindex.
   */
  public static class PrioritizedAuId {
    /**
     * The identifier of the AU.
     */
    public String auId;
    long priority;
    boolean isNew;
    boolean needFullReindex;
  }
  
  /**
   * Provides a list of AuIds that require reindexing sorted by priority.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param maxAuIds
   *          An int with the maximum number of AuIds to return.
   * @return a List<String> with the list of AuIds that require reindexing
   *         sorted by priority.
   */
  List<PrioritizedAuId> getPrioritizedAuIdsToReindex(Connection conn,
      int maxAuIds, boolean prioritizeIndexingNewAus) {
    return mdxManagerSql.getPrioritizedAuIdsToReindex(conn, maxAuIds,
	prioritizeIndexingNewAus);
  }

  /**
   * Cancels any running tasks associated with an AU and deletes the AU
   * metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @return an int with the number of articles deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int deleteAu(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "deleteAu(): ";
    log.debug3(DEBUG_HEADER + "auid = " + auId);
    cancelAuTask(auId);

    // Remove from the history list
    removeFromIndexingTaskHistory(auId);
    removeFromFailedIndexingTasks(auId);

    // Remove the metadata for this AU.
    int articleCount = mdxManagerSql.removeAuMetadataItems(conn, auId);
    log.debug3(DEBUG_HEADER + "articleCount = " + articleCount);

    mdxManagerSql.removeAu(conn, auId);

    // Remove pending reindexing operations for this AU.
    pendingAusCount = mdxManagerSql.removeFromPendingAus(conn, auId);

    notifyDeletedAu(auId, articleCount);

    return articleCount;
  }

  /**
   * Notify listeners that an AU has been removed.
   * 
   * @param auId the AuId of the AU that was removed
   * @param articleCount the number of articles deleted
   */
  protected void notifyDeletedAu(String auId, int articleCount) {
  }

  /**
   * Provides the ArticleMetadataExtractor for the specified AU.
   * 
   * @param au
   *          An ArchivalUnit with the AU.
   * @return an ArticleMetadataExtractor with the article metadata extractor.
   */
  private ArticleMetadataExtractor getMetadataExtractor(ArchivalUnit au) {
    ArticleMetadataExtractor ae = null;

    if (useMetadataExtractor) {
      Plugin plugin = au.getPlugin();
      ae = plugin.getArticleMetadataExtractor(MetadataTarget.OpenURL(), au);
    }

    if (ae == null) {
      ae = new BaseArticleMetadataExtractor(null);
    }

    return ae;
  }

  /**
   * Adds a task to the history.
   * 
   * @param task
   *          A ReindexingTask with the task.
   */
  private void addToIndexingTaskHistory(ReindexingTask task) {
    synchronized (reindexingTaskHistory) {
      reindexingTaskHistory.add(0, task);
      setMaxHistory(maxReindexingTaskHistory);
    }
  }

  /**
   * Runs the specified reindexing task.
   * <p>
   * Temporary implementation runs as a LockssRunnable in a thread rather than
   * using the SchedService.
   * 
   * @param task A ReindexingTask with the reindexing task.
   */
  private void runReindexingTask(final ReindexingTask task) {
    /*
     * Temporarily running task in its own thread rather than using SchedService
     * 
     * @todo Update SchedService to handle this case
     */
    LockssRunnable runnable =
	new LockssRunnable(AuUtil.getThreadNameFor("Reindexing",
	                                           task.getAu())) {
	  public void lockssRun() {
	    startWDog(WDOG_PARAM_INDEXER, WDOG_DEFAULT_INDEXER);
            triggerWDogOnExit(true);
	    task.setWDog(this);

	    task.handleEvent(Schedule.EventType.START);

	    while (!task.isFinished()) {
	      task.step(Integer.MAX_VALUE);
	    }

	    task.handleEvent(Schedule.EventType.FINISH);
	    stopWDog();
            triggerWDogOnExit(false);
	  }
	};

    Thread runThread = new Thread(runnable);
    runThread.start();
  }

  /**
   * Removes from history indexing tasks for a specified AU.
   * 
   * @param auId
   *          A String with the AU identifier.
   * @return an int with the number of items removed.
   */
  private int removeFromIndexingTaskHistory(String auId) {
    int count = 0;

    synchronized (reindexingTaskHistory) {
      // Remove tasks with this auid from task history list.
      for (Iterator<ReindexingTask> itr = reindexingTaskHistory.iterator();
	  itr.hasNext();) {
        ReindexingTask task = itr.next();

        if (auId.equals(task.getAu().getAuId())) {
          itr.remove();
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Removes from failed reindexing tasks for a specified AU.
   * 
   * @param auId
   *          A String with the AU identifier.
   * @return an int with the number of items removed.
   */
  private int removeFromFailedIndexingTasks(String auId) {
    int count = 0;

    synchronized (failedReindexingTasks) {
      // Remove tasks with this auid from task history list.
      for (Iterator<ReindexingTask> itr = failedReindexingTasks.iterator();
          itr.hasNext();) {
        ReindexingTask task = itr.next();

        if (auId.equals(task.getAu().getAuId())) {
          itr.remove();
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Provides an indication of whether an Archival Unit is eligible for
   * reindexing.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit involved.
   * @return a boolean with <code>true</code> if the Archival Unit is eligible
   *         for reindexing, <code>false</code> otherwise.
   */
  boolean isEligibleForReindexing(ArchivalUnit au) {
    return isEligibleForReindexing(au.getAuId());
  }

  /**
   * Provides an indication of whether an Archival Unit is eligible for
   * reindexing.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a boolean with <code>true</code> if the Archival Unit is eligible
   *         for reindexing, <code>false</code> otherwise.
   */
  boolean isEligibleForReindexing(String auId) {
    return indexPriorityAuidMap == null
      || indexPriorityAuidMap.getMatch(auId, 0) >= 0;
  }

  /**
   * Provides the number of active reindexing tasks.
   * 
   * @return a long with the number of active reindexing tasks.
   */
  long getActiveReindexingCount() {
    return activeReindexingTasks.size();
  }

  /**
   * Provides the number of successful reindexing operations.
   * 
   * @return a long with the number of successful reindexing operations.
   */
  synchronized long getSuccessfulReindexingCount() {
    if (successfulReindexingCount < 0) {
      try {
	successfulReindexingCount = jobMgr.getSuccessfulReindexingJobsCount();
      } catch (DbException ex) {
        log.error("getSuccessfulReindexingCount", ex);
      }
    }
    return (successfulReindexingCount < 0) ? 0 : successfulReindexingCount;
  }

  /**
   * Provides the number of unsuccessful reindexing operations.
   * 
   * @return a long the number of unsuccessful reindexing operations.
   */
  synchronized long getFailedReindexingCount() {
    if (failedReindexingCount < 0) {
      try {
	failedReindexingCount = jobMgr.getFailedReindexingJobsCount();
      } catch (DbException ex) {
        log.error("getFailedReindexingCount", ex);
      }
    }
    return (failedReindexingCount < 0) ? 0 : failedReindexingCount;
  }

  /**
   * Provides the list of reindexing tasks.
   * 
   * @return a List<ReindexingTask> of reindexing tasks.
   */
  List<DisplayReindexingTask> getReindexingTasks() {
    log.debug2("Invoked");
    List<DisplayReindexingTask> tasks = new ArrayList<>();
    int taskCount = 0;

    log.debug3("Running tasks:");

    // Loop through all the tasks in the current history in memory.
    for (ReindexingTask reindexingTask : reindexingTaskHistory) {
      // Check whether the task is running.
      if (reindexingTask.hasStarted() && !reindexingTask.isFinished()) {
	// Yes: Add it to the list.
	DisplayReindexingTask task = new DisplayReindexingTask(reindexingTask);
	if (log.isDebug3()) log.debug3("task = " + task);
	tasks.add(task);

	// Count the task.
	taskCount++;
	if (taskCount == maxReindexingTaskHistory) {
	  return tasks;
	}
      }
    }

    log.debug3("Pending tasks:");

    try {
      // Loop through all the pending tasks in the job database.
      for (Map<String, Object> job : jobMgr.getNotStartedReindexingJobs(
	  maxReindexingTaskHistory - taskCount)) {
	// Add it to the list.
	DisplayReindexingTask task = new DisplayReindexingTask();
	String auId = PluginManager.generateAuId((
	    String)job.get(PLUGIN_ID_COLUMN), (String)job.get(AU_KEY_COLUMN));
	if (log.isDebug3()) log.debug3("auId = " + auId);
	task.setAuId(auId);
	String auName = getAuName(auId);
	task.setAuName(auName);
	Long jobTypeSeq = (Long) job.get(JOB_TYPE_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3("jobTypeSeq = " + jobTypeSeq);
	task.setNeedFullReindex(jobMgr.isFullReindexJob(jobTypeSeq));
	if (log.isDebug3()) log.debug3("task = " + task);
	tasks.add(task);

	// Count the task.
	taskCount++;
      }
    } catch (DbException dbe) {
      log.error("jobMgr.getNotStartedReindexingJobs() threw", dbe);
    } catch (Exception e) {
      log.error("getAuName() threw", e);
    }

    // Done if the list is full.
    if (taskCount == maxReindexingTaskHistory) {
      return tasks;
    }

    log.debug3("Current finished tasks:");

    // Loop through all the finished tasks in the current history in memory.
    for (ReindexingTask reindexingTask : reindexingTaskHistory) {
      // Check whether the task is finished.
      if (reindexingTask.isFinished()) {
	// Yes: Add it to the list.
	DisplayReindexingTask task = new DisplayReindexingTask(reindexingTask);
	tasks.add(task);
	if (log.isDebug3()) log.debug3("task = " + task);

	// Count the task.
	taskCount++;
	if (taskCount == maxReindexingTaskHistory) {
	  return tasks;
	}
      }
    }

    // Get the database cutoff timestamp to avoid overlapping with in-memory
    // tasks.
    long startTime = theApp.getStartTime();

    log.debug3("Older finished tasks:");

    try {
      // Loop through all the finished tasks in the job database before the
      // timestamp cutoff.
      for (Map<String, Object> job : jobMgr.getFinishedReindexingJobsBefore(
	  maxReindexingTaskHistory - taskCount, startTime)) {
	// Add it to the list.
	DisplayReindexingTask task = new DisplayReindexingTask();
	String auId = PluginManager.generateAuId((
	    String)job.get(PLUGIN_ID_COLUMN), (String)job.get(AU_KEY_COLUMN));
	if (log.isDebug3()) log.debug3("auId = " + auId);
	task.setAuId(auId);
	String auName = getAuName(auId);
	task.setAuName(auName);
	Long jobTypeSeq = (Long) job.get(JOB_TYPE_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3("jobTypeSeq = " + jobTypeSeq);
	task.setNeedFullReindex(jobMgr.isFullReindexJob(jobTypeSeq));
	task.setStartTime((Long) job.get(START_TIME_COLUMN));
	task.setEndTime((Long) job.get(END_TIME_COLUMN));
	String statusMessage = (String) job.get(STATUS_MESSAGE_COLUMN);
	if (log.isDebug3())
	  log.debug3("statusMessage = '" + statusMessage + "'");
	ReindexingStatus reindexingStatus = "Success".equals(statusMessage)
	    ? ReindexingStatus.Success : ReindexingStatus.Failed;
	if (log.isDebug3())
	  log.debug3("reindexingStatus = " + reindexingStatus);
	task.setReindexingStatus(reindexingStatus);

	// Check whether it was a successful run.
        if (reindexingStatus == ReindexingStatus.Success) {
          // Yes: Determine whether there is substance.
          AuStateBean auStateBean = stateManager.getAuStateBean(auId);
          if (log.isDebug3()) log.debug3("auStateBean = " + auStateBean);

          boolean auNoSubstance =
              auStateBean.getHasSubstance() == SubstanceChecker.State.No;
          if (log.isDebug3()) log.debug3("auNoSubstance = " + auNoSubstance);

          task.setAuNoSubstance(auNoSubstance);
        } else {
          // No: Set up the failure message.
          task.setE(new Exception(statusMessage));
        }

	if (log.isDebug3()) log.debug3("task = " + task);
	tasks.add(task);

	// Count the task.
	taskCount++;
      }
    } catch (DbException dbe) {
      log.error("jobMgr.getFinishedReindexingJobsBefore() threw", dbe);
    } catch (Exception e) {
      log.error("getAuName() threw", e);
    }

    if (log.isDebug2()) log.debug2("tasks.size = " + tasks.size());
    return tasks;
  }

  /**
   * Provides a collection of the most recent failed reindexing task
   * for each task AU.
   * 
   * @return a Collection<ReindexingTask> of failed reindexing tasks
   */
  List<DisplayReindexingTask> getFailedReindexingTasks() {
    List<DisplayReindexingTask> tasks = new ArrayList<>();
    int taskCount = 0;

    log.debug3("Current failed tasks:");

    // Loop through all the tasks in the current history in memory.
    for (ReindexingTask reindexingTask : failedReindexingTasks) {
      // Add it to the list.
      DisplayReindexingTask task = new DisplayReindexingTask(reindexingTask);
      tasks.add(task);

      // Count the task.
      taskCount++;
      if (taskCount == maxReindexingTaskHistory) {
	return tasks;
      }
    }

    // Get the database cutoff timestamp to avoid overlapping with in-memory
    // tasks.
    long startTime = theApp.getStartTime();

    log.debug3("Older failed tasks:");

    try {
      // Loop through all the failed tasks in the job database before the
      // timestamp cutoff.
      for (Map<String, Object> job : jobMgr.getFailedReindexingJobsBefore(
	  maxReindexingTaskHistory - taskCount, startTime)) {
	// Add it to the list.
	DisplayReindexingTask task = new DisplayReindexingTask();
	String auId = PluginManager.generateAuId((
	    String)job.get(PLUGIN_ID_COLUMN), (String)job.get(AU_KEY_COLUMN));
	if (log.isDebug3()) log.debug3("auId = " + auId);
	task.setAuId(auId);
	String auName = getAuName(auId);
	task.setAuName(auName);
	Long jobTypeSeq = (Long) job.get(JOB_TYPE_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3("jobTypeSeq = " + jobTypeSeq);
	task.setNeedFullReindex(jobMgr.isFullReindexJob(jobTypeSeq));
	task.setStartTime((Long) job.get(START_TIME_COLUMN));
	task.setEndTime((Long) job.get(END_TIME_COLUMN));
	task.setReindexingStatus(ReindexingStatus.Failed);
	String statusMessage = (String) job.get(STATUS_MESSAGE_COLUMN);
	if (log.isDebug3())
	  log.debug3("statusMessage = '" + statusMessage + "'");
        task.setE(new Exception(statusMessage));
	tasks.add(task);

	// Count the task.
	taskCount++;
      }
    } catch (DbException dbe) {
      log.error("jobMgr.getFailedReindexingJobsBefore() threw", dbe);
    } catch (Exception e) {
      log.error("getAuName() threw", e);
    }

    return tasks;
  }

  /**
   * Provides a collection of auids for AUs pending reindexing.
   * The number of elements returned is controlled by a definable
   * parameter {@link #PARAM_PENDING_AU_LIST_SIZE}.
   * 
   * @return default auids for AUs pending reindexing
   */
  List<PrioritizedAuId> getPendingReindexingAus() { 
    return getPendingReindexingAus(pendingAuListSize);
  }

  /**
   * Provides a collection of auids for AUs pending reindexing.
   * 
   * @param maxAuIds
   *          An int with the maximum number of auids to return.
   * @return a List<PrioritizedAuId> with the auids for AUs pending reindexing.
   */
  private List<PrioritizedAuId> getPendingReindexingAus(int maxAuIds) {
    final String DEBUG_HEADER = "getPendingReindexingAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "maxAuIds = " + maxAuIds);

    List<PrioritizedAuId> auidsToReindex = new ArrayList<PrioritizedAuId>();

    if (pluginMgr != null) {
      try {
	List<Map<String, Object>> notStartedJobs =
	    jobMgr.getNotStartedReindexingJobs(maxAuIds);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "notStartedJobs.size() = "
	    + notStartedJobs.size());

	for (Map<String, Object> job : notStartedJobs) {
	  PrioritizedAuId auToReindex = new PrioritizedAuId();

	  String pluginId = (String)job.get(SqlConstants.PLUGIN_ID_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

	  String auKey = (String)job.get(SqlConstants.AU_KEY_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	  String auId = PluginManager.generateAuId(pluginId, auKey);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);
	  auToReindex.auId = auId;

	  long priority = (Long)job.get(SqlConstants.PRIORITY_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "priority = " + priority);
	  auToReindex.priority = priority;

	  auToReindex.isNew = false;

	  Long jobTypeSeq = (Long) job.get(JOB_TYPE_SEQ_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "jobTypeSeq = " + jobTypeSeq);

	  boolean needFullReindex = jobMgr.isFullReindexJob(jobTypeSeq);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "needFullReindex = " + needFullReindex);
	  auToReindex.needFullReindex = needFullReindex;

	  auidsToReindex.add(auToReindex);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Added auId = " + auId
	      + " to reindex list");
	}
      } catch (DbException dbe) {
	log.error("Cannot get pending AU ids for reindexing", dbe);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "auidsToReindex.size() = " + auidsToReindex.size());
    return auidsToReindex;
  }
  
  /**
   * Provides the number of distinct articles in the metadata database.
   * 
   * @return a long with the number of distinct articles in the metadata
   *         database.
   */
  long getArticleCount() {
    return metadataArticleCount;
  }

  /**
   * Provides the number of distict publishers in the metadata database.
   * 
   * @return the number of distinct publishers in the metadata database
   */
  long getPublisherCount() {
    if (metadataPublisherCount < 0) {
      try {
        metadataPublisherCount = mdxManagerSql.getPublisherCount();
      } catch (DbException ex) {
        log.error("getPublisherCount", ex);
      }
    }
    return (metadataPublisherCount < 0) ? 0 : metadataPublisherCount;
  }

  /**
   * Provides the number of distict providers in the metadata database.
   * 
   * @return the number of distinct providers in the metadata database
   */
  long getProviderCount() {
    if (metadataProviderCount < 0) {
      try {
        metadataProviderCount = mdxManagerSql.getProviderCount();
      } catch (DbException ex) {
	log.error("getProviderCount", ex);
      }
    }
    return (metadataProviderCount < 0) ? 0 : metadataProviderCount;
  }

  /**
   * Provides the number of AUs pending to be reindexed.
   * 
   * @return a long with the number of AUs pending to be reindexed.
   */
  long getPendingAusCount() {
    try {
      return jobMgr.getNotStartedReindexingJobsCount();
    } catch (DbException dbe) {
      log.error("getPendingAusCount", dbe);
    }
    return 0;
  }

  /**
   * Re-calculates the number of AUs pending to be reindexed.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void updatePendingAusCount(Connection conn) throws DbException {
    final String DEBUG_HEADER = "updatePendingAusCount(): ";
    pendingAusCount = mdxManagerSql.getEnabledPendingAusCount(conn);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "pendingAusCount = " + pendingAusCount);
  }

  /**
   * Provides the indexing enabled state of this manager.
   * 
   * @return a boolean with the indexing enabled state of this manager.
   */
  public boolean isIndexingEnabled() {
    return reindexingEnabled;
  }

  /**
   * Updates the timestamp of the last extraction of an Archival Unit metadata.
   * 
   * @param au
   *          The ArchivalUnit whose time to update.
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auMdSeq
   *          A Long with the identifier of the Archival Unit metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void updateAuLastExtractionTime(ArchivalUnit au, Connection conn,
      Long auMdSeq) throws DbException {
    final String DEBUG_HEADER = "updateAuLastExtractionTime(): ";

    long now = TimeBase.nowMs();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "now = " + now);

    mdxManagerSql.updateAuLastExtractionTime(conn, auMdSeq, now);
    pendingAusCount = mdxManagerSql.getEnabledPendingAusCount(conn);
  }

  /**
   * Adds an AU to the list of AUs to be reindexed.
   * Does incremental reindexing if possible, unless full reindexing
   * is required because the plugin metadata version has changed.
   * 
   * @param au
   *          An ArchivalUnit with the AU to be reindexed.
   * @param conn
   *          A Connection with the database connection to be used.
   * @param insertPendingAuBatchStatement
   *          A PreparedStatement with the prepared statement used to insert
   *          pending AUs.
   * @param inBatch
   *          A boolean indicating whether the reindexing of this AU should be
   *          performed as part of a batch.
   * @return <code>true</code> if au was added for reindexing
   */
  boolean enableAndAddAuToReindex(ArchivalUnit au, Connection conn,
      PreparedStatement insertPendingAuBatchStatement, boolean inBatch) {
    boolean fullReindex = isAuMetadataForObsoletePlugin(au);
    return enableAndAddAuToReindex(au, conn, insertPendingAuBatchStatement,
	inBatch, fullReindex);
  }
  
  /**
   * Adds an AU to the list of AUs to be reindexed. Optionally causes
   * full reindexing by removing the AU from the database.
   * 
   * @param au
   *          An ArchivalUnit with the AU to be reindexed.
   * @param conn
   *          A Connection with the database connection to be used.
   * @param insertPendingAuBatchStatement
   *          A PreparedStatement with the prepared statement used to insert
   *          pending AUs.
   * @param inBatch
   *          A boolean indicating whether the reindexing of this AU should be
   *          performed as part of a batch.
   * @param fullReindex
   *          Causes a full reindex by ignoring the last extraction time and
   *          removing from the database the metadata of that AU. 
   * @return <code>true</code> if au was added for reindexing
   */
  boolean enableAndAddAuToReindex(ArchivalUnit au, Connection conn,
      PreparedStatement insertPendingAuBatchStatement, boolean inBatch,
      boolean fullReindex) {
    final String DEBUG_HEADER = "enableAndAddAuToReindex(): ";

    synchronized (activeReindexingTasks) {

      try {
        // If disabled crawl completion rescheduling
        // a running task, have this function report false;
        if (disableCrawlRescheduleTask
            && activeReindexingTasks.containsKey(au.getAuId())) {
          log.debug2(DEBUG_HEADER + "Not adding AU to reindex: "
              + au.getName());
          return false;
        }

        log.debug2(DEBUG_HEADER + "Adding AU to reindex: " + au.getName());

        // Remove it from the list if it was marked as disabled.
        removeDisabledFromPendingAus(conn, au.getAuId());

        // If it's not possible to reschedule the current task, add the AU to
        // the pending list.
        if (!rescheduleAuTask(au.getAuId())) {
          addToPendingAusIfNotThere(conn, Collections.singleton(au),
              insertPendingAuBatchStatement, inBatch, fullReindex);
        }
        
        startReindexing(conn);

        MetadataDbManager.commitOrRollback(conn, log);
        return true;
      } catch (DbException dbe) {
        log.error("Cannot add au to pending AUs: " + au.getName(), dbe);
        return false;
      }
    }
  }

  /**
   * Removes an AU with disabled indexing from the table of pending AUs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archiva lUnit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void removeDisabledFromPendingAus(Connection conn, String auId)
      throws DbException {
    mdxManagerSql.removeDisabledFromPendingAus(conn, auId);
    pendingAusCount = mdxManagerSql.getEnabledPendingAusCount(conn);
  }

  /**
   * Reschedules a reindexing task for a specified AU.
   * 
   * @param auId
   *          A String with the Archiva lUnit identifier.
   * @return <code>true</code> if task was rescheduled, <code>false</code>
   *         otherwise.
   */
  private boolean rescheduleAuTask(String auId) {
    final String DEBUG_HEADER = "rescheduleAuTask(): ";
    ReindexingTask task = activeReindexingTasks.get(auId);

    if (task != null) {
      log.debug2(DEBUG_HEADER
	  + "Rescheduling pending reindexing task for auId " + auId);
      // Task rescheduling will remove the task, and cause it to be rescheduled.
      task.reschedule();
      return true;
    }

    return false;
  }

  /**
   * Disables the indexing of an AU.
   * 
   * @param au
   *          An ArchivalUnit with the AU for which indexing is to be disabled.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void disableAuIndexing(ArchivalUnit au) throws DbException {
    final String DEBUG_HEADER = "disableAuIndexing(): ";

    synchronized (activeReindexingTasks) {
      Connection conn = null;

      try {
        log.debug2(DEBUG_HEADER + "Disabling indexing for AU " + au.getName());
        conn = dbManager.getConnection();

        if (conn == null) {
          log.error("Cannot disable indexing for AU '" + au.getName()
              + "' - Cannot connect to database");
          throw new DbException("Cannot connect to database");
        }

        String auId = au.getAuId();
        log.debug2(DEBUG_HEADER + "auId " + auId);

        if (activeReindexingTasks.containsKey(auId)) {
          ReindexingTask task = activeReindexingTasks.get(auId);
          task.cancel();
          activeReindexingTasks.remove(auId);
        }

        // Remove the AU from the list of pending AUs if it is there.
        pendingAusCount = mdxManagerSql.removeFromPendingAus(conn, auId);

        // Add it marked as disabled.
        mdxManagerSql.addDisabledAuToPendingAus(conn, auId);
        MetadataDbManager.commitOrRollback(conn, log);
      } catch (DbException dbe) {
        String errorMessage = "Cannot disable indexing for AU '"
            + au.getName() +"'";
        log.error(errorMessage, dbe);
        throw dbe;
      } finally {
	MetadataDbManager.safeRollbackAndClose(conn);
      }
    }
  }

  /**
   * Adds AUs to the table of pending AUs to reindex if they are not there
   * already.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param aus
   *          A Collection<ArchivalUnit> with the AUs to add.
   * @param fullReindex
   *          A boolean indicating whether a full reindex of the Archival Unit
   *          is required.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addToPendingAusIfNotThere(Connection conn, Collection<ArchivalUnit> aus,
      boolean fullReindex) throws DbException {
    PreparedStatement insertPendingAuBatchStatement = null;

    try {
      insertPendingAuBatchStatement =
	  mdxManagerSql.getInsertPendingAuBatchStatement(conn);
      addToPendingAusIfNotThere(conn, aus, insertPendingAuBatchStatement, false,
	  fullReindex);
    } finally {
      MetadataDbManager.safeCloseStatement(insertPendingAuBatchStatement);
    }
  }

  /**
   * Adds AUs to the table of pending AUs to reindex if they are not there
   * already.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param aus
   *          A Collection<ArchivalUnit> with the AUs to add.
   * @param insertPendingAuBatchStatement
   *          A PreparedStatement with the prepared statement used to insert
   *          pending AUs.
   * @param inBatch
   *          A boolean indicating whether adding these AUs to the list of
   *          pending AUs to reindex should be performed as part of a batch.
   * @param fullReindex
   *          A boolean indicating whether a full reindex of the Archival Unit
   *          is required.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addToPendingAusIfNotThere(Connection conn, Collection<ArchivalUnit> aus,
      PreparedStatement insertPendingAuBatchStatement, boolean inBatch,
      boolean fullReindex) throws DbException {
    final String DEBUG_HEADER = "addToPendingAusIfNotThere(): ";

    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "Number of pending aus to add: " + aus.size());
      log.debug2(DEBUG_HEADER + "inBatch = " + inBatch);
      log.debug2(DEBUG_HEADER + "fullReindex = " + fullReindex);
    }

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "maxPendingAuBatchSize = "
	  + maxPendingAuBatchSize);

    try {
      // Loop through all the AUs.
      for (ArchivalUnit au : aus) {
        // Only add for extraction iff it has article metadata.
        if (!hasArticleMetadata(au)) {
          log.debug3(DEBUG_HEADER + "Not adding au " + au.getName()
              + " to pending list because it has no metadata");
        } else {
          String auid = au.getAuId();
          String pluginKey = PluginManager.pluginKeyFromAuId(auid);
          String auKey = PluginManager.auKeyFromAuId(auid);

          if (!mdxManagerSql.isAuPending(conn, pluginKey, auKey)) {
            // Only insert if entry does not exist.
	    log.debug3(DEBUG_HEADER + "Adding au " + au.getName()
		+ " to pending list");
            mdxManagerSql.addAuToPendingAusBatch(pluginKey, auKey, fullReindex,
        	insertPendingAuBatchStatement);
            pendingAuBatchCurrentSize++;
	    log.debug3(DEBUG_HEADER + "pendingAuBatchCurrentSize = "
		+ pendingAuBatchCurrentSize);

	    // Check whether the maximum batch size has been reached.
	    if (pendingAuBatchCurrentSize >= maxPendingAuBatchSize) {
	      // Yes: Perform the insertion of all the AUs in the batch.
	      addAuBatchToPendingAus(insertPendingAuBatchStatement);
	    }
          } else {
            if (fullReindex) {
              mdxManagerSql.updateAuFullReindexing(conn, au, true);
            } else {
              log.debug3(DEBUG_HEADER+ "Not adding au " + au.getName()
        	  + " to pending list because it is already on the list");
            }
          }
	}
      }

      // Check whether there are no more AUs to be batched and the batch is not
      // empty.
      if (!inBatch && pendingAuBatchCurrentSize > 0) {
	// Yes: Perform the insertion of all the AUs in the batch.
	addAuBatchToPendingAus(insertPendingAuBatchStatement);
      }
    } catch (SQLException sqle) {
      throw new DbException("Cannot add pending AUs", sqle);
    }

    pendingAusCount = mdxManagerSql.getEnabledPendingAusCount(conn);
  }

  /**
   * Provides an indication of whether an AU has article metadata.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   * @return <code>true</code> if the AU has article metadata,
   *         <code>false</code> otherwise.
   */
  private boolean hasArticleMetadata(ArchivalUnit au) {
    if (au.getArticleIterator(MetadataTarget.OpenURL()) == null) {
      return false;
    }

    // It has article metadata if there is a metadata extractor.
    if (useMetadataExtractor) {
      Plugin p = au.getPlugin();

      if (p.getArticleMetadataExtractor(MetadataTarget.OpenURL(), au) != null) {
        return true;
      }
    }

    // Otherwise, it has metadata if it can be created from the TdbAu.
    return (au.getTdbAu() != null);
  }

  private void addAuBatchToPendingAus(PreparedStatement
      insertPendingAuBatchStatement) throws SQLException {
    final String DEBUG_HEADER = "addAuBatchToPendingAus(): ";
    mdxManagerSql.addAuBatchToPendingAus(insertPendingAuBatchStatement);
    pendingAuBatchCurrentSize = 0;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pendingAuBatchCurrentSize = "
	+ pendingAuBatchCurrentSize);
  }

  /**
   * Notifies listeners that an AU is being reindexed.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   */
  protected void notifyStartReindexingAu(ArchivalUnit au) {
    final String DEBUG_HEADER = "notifyStartReindexingAu(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "au = " + au);
    jobMgr.handlePutAuJobStartEvent(au.getAuId());
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done");
  }

  /**
   * Notifies listeners that an AU is finished being reindexed.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   * @param status
   *          A ReindexingStatus with the status of the reindexing process.
   * @param exception
   *          An Exception with any exception that occurred.
   */
  protected void notifyFinishReindexingAu(ArchivalUnit au,
      ReindexingStatus status, Exception exception) {
    final String DEBUG_HEADER = "notifyFinishReindexingAu(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "au = " + au);
      log.debug2(DEBUG_HEADER + "status = " + status);
      log.debug2(DEBUG_HEADER + "exception = " + exception);
    }

    jobMgr.handlePutAuJobFinishEvent(au.getAuId(), status, exception);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done");
  }

  /**
   * Notifies listeners that the metadata of an AU is starting to be deleted.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   */
  protected void notifyStartAuMetadataRemoval(ArchivalUnit au) {
    final String DEBUG_HEADER = "notifyStartAuMetadataRemoval(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "au = " + au);
    jobMgr.handleDeleteAuJobStartEvent(au.getAuId());
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done");
  }

  /**
   * Notifies listeners that the metadata of an AU has been deleted.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   * @param status
   *          A ReindexingStatus with the status of the metadata removal
   *          process.
   * @param exception
   *          An Exception with any exception that occurred.
   */
  protected void notifyFinishAuMetadataRemoval(ArchivalUnit au,
      ReindexingStatus status, Exception exception) {
    final String DEBUG_HEADER = "notifyFinishAuMetadataRemoval(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "au = " + au);
      log.debug2(DEBUG_HEADER + "status = " + status);
      log.debug2(DEBUG_HEADER + "exception = " + exception);
    }

    jobMgr.handleDeleteAuJobFinishEvent(au.getAuId(), status, exception);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done");
  }

  /**
   * Deletes an AU and starts the next reindexing task.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   * @return <code>true</code> if the AU was deleted, <code>false</code>
   *         otherwise.
   */
  boolean deleteAuAndReindex(ArchivalUnit au) {
    final String DEBUG_HEADER = "deleteAuAndReindex(): ";

    synchronized (activeReindexingTasks) {
      Connection conn = null;

      try {
        log.debug2(DEBUG_HEADER + "Removing au to reindex: " + au.getName());
        // add pending AU
        conn = dbManager.getConnection();

        if (conn == null) {
          log.error("Cannot connect to database"
              + " -- cannot add aus to pending aus");
          return false;
        }

        deleteAu(conn, au.getAuId());

        // Force reindexing to start next task.
        startReindexing(conn);
        MetadataDbManager.commitOrRollback(conn, log);

        return true;
      } catch (DbException dbe) {
        log.error("Cannot remove au: " + au.getName(), dbe);
        return false;
      } finally {
	MetadataDbManager.safeRollbackAndClose(conn);
      }
    }
  }

  /**
   * Provides the metadata version of a plugin.
   * 
   * @param plugin
   *          A Plugin with the plugin.
   * @return an int with the plugin metadata version.
   */
  public int getPluginMetadataVersionNumber(Plugin plugin) {
    final String DEBUG_HEADER = "getPluginMetadataVersionNumber(): ";
  
    int version = 1;
    String pluginVersion = plugin.getFeatureVersion(Feature.Metadata);
    if (log.isDebug3()) {
      log.debug3(DEBUG_HEADER + "Metadata Feature version: " + pluginVersion
		 + " for " + plugin.getPluginName());
    }
    if (StringUtil.isNullString(pluginVersion)) {
      log.debug2("Plugin version not found: Using " + version);
      return version;
    }

    String prefix = Feature.Metadata + "_";

    if (!pluginVersion.startsWith(prefix)) {
      log.error("Plugin version '" + pluginVersion + "' does not start with '"
	  + prefix + "': Using " + version);
      return version;
    }

    try {
      version = Integer.valueOf(pluginVersion.substring(prefix.length()));
    } catch (NumberFormatException nfe) {
      log.error("Plugin version '" + pluginVersion + "' does not end with a "
	  + "number after '" + prefix + "': Using " + version);
    }
    
    log.debug3(DEBUG_HEADER + "version = " + version);
    return version;
  }

  /**
   * Increments the count of successful reindexing tasks. 
   */
  void addToSuccessfulReindexingTasks() {
    successfulReindexingCount = getSuccessfulReindexingCount() + 1;
  }

  synchronized void addToMetadataArticleCount(long count) {
    this.metadataArticleCount += count;
    mdManager.resetPublicationCount();  // needs recalculation
    this.metadataPublisherCount = -1;    // needs recalculation
    this.metadataProviderCount = -1;     // needs recalculation
  }

  /**
   * Receives notification that a reindexing task has failed 
   * or has been rescheduled.
   * 
   * @param task the reindexing task
   */
  void addToFailedReindexingTasks(ReindexingTask task) {
    failedReindexingCount = getFailedReindexingCount() + 1;
    
    String taskAuId = task.getAuId();
    synchronized (failedReindexingTasks) {
      removeFromFailedIndexingTasks(taskAuId);
      failedReindexingTasks.add(0, task);
      setMaxHistory(maxReindexingTaskHistory);
    }
  }

  /**
   * Provides an indication of whether the version of the metadata of an AU
   * stored in the database has been obtained with an obsolete version of the
   * plugin.
   * 
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @return <code>true</code> if the metadata was obtained with a version of
   *         the plugin previous to the current version, <code>false</code>
   *         otherwise.
   */
  boolean isAuMetadataForObsoletePlugin(ArchivalUnit au) {
    final String DEBUG_HEADER = "isAuMetadataForObsoletePlugin(): ";

    // Get the plugin version of the stored AU metadata. 
    int auVersion = mdxManagerSql.getAuMetadataVersion(au);
    log.debug(DEBUG_HEADER + "auVersion = " + auVersion);

    // Get the current version of the plugin. 
    int pVersion = getPluginMetadataVersionNumber(au.getPlugin());
    log.debug(DEBUG_HEADER + "pVersion = " + pVersion);

    return pVersion > auVersion;
  }

  /**
   * Provides an indication of whether the version of the metadata of an AU
   * stored in the database has been obtained with an obsolete version of the
   * plugin.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @return <code>true</code> if the metadata was obtained with a version of
   *         the plugin previous to the current version, <code>false</code>
   *         otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean isAuMetadataForObsoletePlugin(Connection conn, ArchivalUnit au)
      throws DbException {
    final String DEBUG_HEADER = "isAuMetadataForObsoletePlugin(): ";

    // Get the plugin version of the stored AU metadata. 
    int auVersion = mdxManagerSql.getAuMetadataVersion(conn, au);
    log.debug2(DEBUG_HEADER + "auVersion = " + auVersion);

    // Get the current version of the plugin. 
    int pVersion = getPluginMetadataVersionNumber(au.getPlugin());
    log.debug2(DEBUG_HEADER + "pVersion = " + pVersion);

    return pVersion > auVersion;
  }

  /**
   * Provides an indication of whether the metadata of an AU has not been saved
   * in the database after the last successful crawl of the AU.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @return <code>true</code> if the metadata of the AU has not been saved in
   *         the database after the last successful crawl of the AU,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean isAuCrawledAndNotExtracted(Connection conn, ArchivalUnit au)
      throws DbException {
    final String DEBUG_HEADER = "isAuCrawledAndNotExtracted(): ";

    // Get the time of the last successful crawl of the AU. 
    long lastCrawlTime = AuUtil.getAuState(au).getLastCrawlTime();
    log.debug2(DEBUG_HEADER + "lastCrawlTime = " + lastCrawlTime);

    long lastExtractionTime = mdxManagerSql.getAuExtractionTime(conn, au);
    log.debug2(DEBUG_HEADER + "lastExtractionTime = " + lastExtractionTime);

    return lastCrawlTime > lastExtractionTime;
  }

  /**
   * Utility method to provide the metadata manager.
   * 
   * @return a MetadataManager with the metadata manager.
   */
  MetadataManager getMetadataManager() {
    return mdManager;
  }

  /**
   * Provides the prepared statement used to insert pending AUs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a PreparedStatement with the prepared statement used to insert
   *         pending AUs.
   */
  PreparedStatement getInsertPendingAuBatchStatement(Connection conn)
      throws DbException {
    return mdxManagerSql.getInsertPendingAuBatchStatement(conn);
  }

  /**
   * Provides the prepared statement used to insert pending AUs with the
   * highest priority.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a PreparedStatement with the prepared statement used to insert
   *         pending AUs with the highest priority.
   */
  PreparedStatement getPrioritizedInsertPendingAuBatchStatement(Connection conn)
      throws DbException {
    return mdxManagerSql.getPrioritizedInsertPendingAuBatchStatement(conn);
  }

  /**
   * Provides the identifiers of pending Archival Units that have been disabled.
   * 
   * @return a Collection<String> with the identifiers of disabled pending
   *         Archival Units.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> findDisabledPendingAus() throws DbException {
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      return findDisabledPendingAus(conn);
    } finally {
      dbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the identifiers of pending Archival Units that have been disabled.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<String> with the identifiers of disabled pending
   *         Archival Units.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> findDisabledPendingAus(Connection conn)
      throws DbException {
    return mdxManagerSql.findPendingAusWithPriority(conn, MIN_INDEX_PRIORITY);
  }

  /**
   * Provides the identifiers of pending Archival Units that failed during
   * metadata indexing.
   * 
   * @return a Collection<String> with the identifiers of pending Archival Units
   *         with failed metadata indexing processes.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> findFailedIndexingPendingAus() throws DbException {
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      return findFailedIndexingPendingAus(conn);
    } finally {
      dbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the identifiers of pending Archival Units that failed during
   * metadata indexing.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<String> with the identifiers of pending Archival Units
   *         with failed metadata indexing processes.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> findFailedIndexingPendingAus(Connection conn)
      throws DbException {
    return mdxManagerSql.findPendingAusWithPriority(conn,
	FAILED_INDEX_PRIORITY);
  }

  /**
   * Provides the identifier of the publisher of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the identifier of the Archival Unit.
   * @return a Long with the identifier of the publisher.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findAuPublisher(Connection conn, Long auSeq) throws DbException {
    return mdxManagerSql.findAuPublisher(conn, auSeq);
  }

  /**
   * Adds to the database the URLs of a metadata item, if they are new.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param featuredUrlMap
   *          A Map<String, String> with the URL/feature pairs to be added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void addNewMdItemUrls(Connection conn, Long mdItemSeq,
      Map<String, String> featuredUrlMap) throws DbException {
    final String DEBUG_HEADER = "addNewMdItemUrls(): ";

    // Initialize the collection of URLs to be added.
    Map<String, String> newUrls = new HashMap<String, String>(featuredUrlMap);

    Map<String, String> oldUrls = dbManager.getMdItemUrls(conn, mdItemSeq);
    String url;

    // Loop through all the URLs already linked to the metadata item.
    for (String feature : oldUrls.keySet()) {
      url = oldUrls.get(feature);
      log.debug3(DEBUG_HEADER + "Found feature = " + feature + ", URL = "
	  + url);

      // Remove it from the collection to be added if it exists already.
      if (newUrls.containsKey(feature) && newUrls.get(feature).equals(url)) {
	log.debug3(DEBUG_HEADER + "Feature = " + feature + ", URL = " + url
	    + " already exists: Not adding it.");

	newUrls.remove(feature);
      }
    }

    // Add the URLs that are new.
    addMdItemUrls(conn, mdItemSeq, null, newUrls);
  }

  /**
   * Adds to the database the authors of a metadata item, if they are new.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param authors
   *          A Collection<String> with the authors of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void addNewMdItemAuthors(Connection conn, Long mdItemSeq,
      Collection<String> authors) throws DbException {
    if (authors == null || authors.size() == 0) {
      return;
    }

    // Initialize the collection of authors to be added.
    List<String> newAuthors = new ArrayList<String>(authors);

    // Get the existing authors.
    Collection<String> oldAuthors =
	mdxManagerSql.getMdItemAuthors(conn, mdItemSeq);

    // Remove them from the collection to be added.
    newAuthors.removeAll(oldAuthors);

    // Add the authors that are new.
    mdxManagerSql.addMdItemAuthors(conn, mdItemSeq, newAuthors);
  }

  /**
   * Adds to the database the keywords of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param keywords
   *          A Collection<String> with the keywords of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void addNewMdItemKeywords(Connection conn, Long mdItemSeq,
      Collection<String> keywords) throws DbException {
    if (keywords == null || keywords.size() == 0) {
      return;
    }

    // Initialize the collection of keywords to be added.
    ArrayList<String> newKeywords = new ArrayList<String>(keywords);

    Collection<String> oldKeywords =
	mdxManagerSql.getMdItemKeywords(conn, mdItemSeq);

    // Remove them from the collection to be added.
    newKeywords.removeAll(oldKeywords);

    // Add the keywords that are new.
    mdxManagerSql.addMdItemKeywords(conn, mdItemSeq, newKeywords);
  }

  /**
   * Adds to the database the URLs of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param accessUrl
   *          A String with the access URL to be added.
   * @param featuredUrlMap
   *          A Map<String, String> with the URL/feature pairs to be added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addMdItemUrls(Connection conn, Long mdItemSeq, String accessUrl,
      Map<String, String> featuredUrlMap) throws DbException {
    final String DEBUG_HEADER = "addMdItemUrls(): ";

    if (!StringUtil.isNullString(accessUrl)) {
      // Add the access URL.
      mdManager.addMdItemUrl(conn, mdItemSeq,
	  MetadataManager.ACCESS_URL_FEATURE, accessUrl);
      log.debug3(DEBUG_HEADER + "Added feature = "
	  + MetadataManager.ACCESS_URL_FEATURE + ", URL = " + accessUrl);
    }

    // Loop through all the featured URLs.
    for (String feature : featuredUrlMap.keySet()) {
      // Add the featured URL.
      mdManager.addMdItemUrl(conn, mdItemSeq, feature,
	  featuredUrlMap.get(feature));
      log.debug3(DEBUG_HEADER + "Added feature = " + feature + ", URL = "
	  + featuredUrlMap.get(feature));
    }
  }

  /**
   * Merges the properties of a child metadata item into another child metadata
   * item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void mergeChildMdItemProperties(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeChildMdItemProperties(): ";
    log.debug3(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
    log.debug3(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);

    // Do not merge a metadata item into itself.
    if (!sourceMdItemSeq.equals(targetMdItemSeq)) {
      // Merge the names.
      mergeMdItemNames(conn, sourceMdItemSeq, targetMdItemSeq);

      // Merge the authors.
      mergeMdItemAuthors(conn, sourceMdItemSeq, targetMdItemSeq);

      // Merge the keywords.
      mergeMdItemKeywords(conn, sourceMdItemSeq, targetMdItemSeq);

      // Merge the URLs.
      mergeMdItemUrls(conn, sourceMdItemSeq, targetMdItemSeq);
    }

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the names of a metadata item into another metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void mergeMdItemNames(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeMdItemNames(): ";
    log.debug3(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
    log.debug3(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);

    Map<String, String> sourceMdItemNames =
	mdManager.getMdItemNames(conn, sourceMdItemSeq);

    for (String mdItemName : sourceMdItemNames.keySet()) {
      mdManager.addNewMdItemName(conn, targetMdItemSeq, mdItemName);
    }

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the authors of a metadata item into another metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void mergeMdItemAuthors(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeMdItemAuthors(): ";
    log.debug3(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
    log.debug3(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);

    Collection<String> sourceMdItemAuthors =
	mdxManagerSql.getMdItemAuthors(conn, sourceMdItemSeq);

    addNewMdItemAuthors(conn, targetMdItemSeq, sourceMdItemAuthors);

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the keywords of a metadata item into another metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void mergeMdItemKeywords(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeMdItemKeywords(): ";
    log.debug3(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
    log.debug3(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);

    Collection<String> sourceMdItemKeywords = 
	mdxManagerSql.getMdItemKeywords(conn, sourceMdItemSeq);

    addNewMdItemKeywords(conn, targetMdItemSeq, sourceMdItemKeywords);

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the URLs of a metadata item into another metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void mergeMdItemUrls(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeMdItemUrls(): ";
    log.debug3(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
    log.debug3(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);

    Map<String, String> sourceMdItemUrls =
	dbManager.getMdItemUrls(conn, sourceMdItemSeq);

    addNewMdItemUrls(conn, targetMdItemSeq, sourceMdItemUrls);

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the properties of a parent metadata item into another parent
   * metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void mergeParentMdItemProperties(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeParentMdItemProperties(): ";
    log.debug3(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
    log.debug3(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);

    // Do not merge a metadata item into itself.
    if (!sourceMdItemSeq.equals(targetMdItemSeq)) {
      // Merge the names.
      mergeMdItemNames(conn, sourceMdItemSeq, targetMdItemSeq);

      // Merge the ISBNs.
      mergeMdItemIsbns(conn, sourceMdItemSeq, targetMdItemSeq);

      // Merge the ISSNs.
      mergeMdItemIssns(conn, sourceMdItemSeq, targetMdItemSeq);

      // Merge the proprietary identifiers.
      mergeMdItemProprietaryIds(conn, sourceMdItemSeq, targetMdItemSeq);
    }

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the ISBNs of a metadata item into another metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void mergeMdItemIsbns(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeMdItemIsbns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
      log.debug2(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);
    }

    // Find the existing ISBNs for the source metadata item.
    Set<Isbn> sourceMdItemIsbns =
	mdManager.getMdItemIsbns(conn, sourceMdItemSeq);

    String isbnType;
    String isbnValue;

    // Loop through all the ISBNs found.
    for (Isbn isbn : sourceMdItemIsbns) {
      // Get the ISBN value.
      isbnValue = isbn.getValue();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbnValue = " + isbnValue);

      // Get the ISBN type.
      isbnType = isbn.getType();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbnType = " + isbnType);

      if (P_ISBN_TYPE.equals(isbnType)) {
	mdManager.addNewMdItemIsbns(conn, targetMdItemSeq, isbnValue, null);
      } else if (E_ISBN_TYPE.equals(isbnType)) {
	mdManager.addNewMdItemIsbns(conn, targetMdItemSeq, null, isbnValue);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the ISSNs of a metadata item into another metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void mergeMdItemIssns(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeMdItemIssns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
      log.debug2(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);
    }

    // Find the existing ISSNs for the source metadata item.
    Set<Issn> sourceMdItemIssns =
	mdManager.getMdItemIssns(conn, sourceMdItemSeq);

    String issnType;
    String issnValue;

    // Loop through all the ISSNs found.
    for (Issn issn : sourceMdItemIssns) {
      // Get the ISSN value.
      issnValue = issn.getValue();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issnValue = " + issnValue);

      // Get the ISSN type.
      issnType = issn.getType();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issnType = " + issnType);

      if (P_ISSN_TYPE.equals(issnType)) {
	mdManager.addNewMdItemIssns(conn, targetMdItemSeq, issnValue, null);
      } else if (E_ISSN_TYPE.equals(issnType)) {
	mdManager.addNewMdItemIssns(conn, targetMdItemSeq, null, issnValue);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Merges the propritary identifiers of a metadata item into another metadata
   * item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sourceMdItemSeq
   *          A Long with the identifier of the source metadata item.
   * @param targetMdItemSeq
   *          A Long with the identifier of the target metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void mergeMdItemProprietaryIds(Connection conn, Long sourceMdItemSeq,
      Long targetMdItemSeq) throws DbException {
    final String DEBUG_HEADER = "mergeMdItemProprietaryIds(): ";
    log.debug3(DEBUG_HEADER + "sourceMdItemSeq = " + sourceMdItemSeq);
    log.debug3(DEBUG_HEADER + "targetMdItemSeq = " + targetMdItemSeq);

    Collection<String> sourceMdItemProprietaryIds = 
        mdManager.getMdItemProprietaryIds(conn, sourceMdItemSeq);

    mdManager.addNewMdItemProprietaryIds(conn, targetMdItemSeq,
	sourceMdItemProprietaryIds);

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Adds an Archival Unit to the table of unconfigured Archival Units.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   */
  void persistUnconfiguredAu(ArchivalUnit au) {
    persistUnconfiguredAu(au.getAuId());
  }

  /**
   * Adds an Archival Unit to the table of unconfigured Archival Units.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   */
  void persistUnconfiguredAu(String auId) {
    final String DEBUG_HEADER = "persistUnconfiguredAu(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    Connection conn = null;

    try {
      conn = dbManager.getConnection();

      if (conn == null) {
	log.error("Cannot connect to database - Cannot insert archival unit "
	    + auId + " in unconfigured table");
	return;
      }

      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

      if (!mdManager.isAuInUnconfiguredAuTable(conn, auId)) {
	mdManager.persistUnconfiguredAu(conn, auId);
	MetadataDbManager.commitOrRollback(conn, log);
      }
    } catch (DbException dbe) {
      log.error("Cannot insert archival unit in unconfigured table", dbe);
      log.error("auId = " + auId);
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the metadata extractor manager SQL code executor.
   * 
   * @return a MetadataExtractorManagerSql with the SQL code executor.
   */
  public MetadataExtractorManagerSql getMetadataExtractorManagerSql() {
    return mdxManagerSql;
  }

  boolean isPrioritizeIndexingNewAus() {
    return prioritizeIndexingNewAus;
  }

  /**
   * Provides the database identifier of an AU.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Long with the identifier of the AU.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findAuSeq(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "findAuSeq(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    Long auSeq = null;

    // Find the plugin.
    Long pluginSeq =
	  mdManager.findPlugin(conn, PluginManager.pluginKeyFromAuId(auId));

    // Check whether the plugin exists.
    if (pluginSeq != null) {
      // Yes: Get the database identifier of the AU.
      String auKey = PluginManager.auKeyFromAuId(auId);

      auSeq = mdManager.findAu(conn, pluginSeq, auKey);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);
    return auSeq;
  }

  /**
   * Enables the indexing of an AU.
   * 
   * @param au
   *          An ArchivalUnit with the AU for which indexing is to be enabled.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void enableAuIndexing(ArchivalUnit au) throws DbException {
    final String DEBUG_HEADER = "disableAuIndexing(): ";

    Connection conn = null;

    try {
      log.debug2(DEBUG_HEADER + "Enabling indexing for AU " + au.getName());
      conn = dbManager.getConnection();

      if (conn == null) {
	log.error("Cannot enable indexing for AU '" + au.getName()
	    + "' - Cannot connect to database");
	throw new DbException("Cannot connect to database");
      }

      String auId = au.getAuId();
      log.debug2(DEBUG_HEADER + "auId " + auId);

      // Remove it from the list if it was marked as disabled.
      removeDisabledFromPendingAus(conn, auId);
      MetadataDbManager.commitOrRollback(conn, log);
    } catch (DbException dbe) {
      String errorMessage = "Cannot enable indexing for AU '" + au.getName()
	  + "'";
      log.error(errorMessage, dbe);
      throw dbe;
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the list of metadata fields that are mandatory.
   *
   * @return a List<String> with the metadata fields that are mandatory.
   */
  public List<String> getMandatoryMetadataFields() {
    return mandatoryMetadataFields;
  }

  /**
   * Provides the indication of whether only on-demand metadata extraction is
   * enabled.
   * 
   * @return a boolean with <code>true</code> if only on-demand metadata
   *         extraction is enabled, <code>false</code> otherwise.
   */
  boolean isOnDemandMetadataExtractionOnly() {
    return onDemandMetadataExtractionOnly;
  }

  /**
   * Starts the indexing of the metadata of an archival unit.
   * 
   * @param auId
   *          A String with the identifier of the archival unit.
   * @param needFullReindex
   *          A boolean with the indication of whether a full extraction is to
   *          be performed or not.
   * @return a ReindexingTask with the metadata indexing task.
   */
  public ReindexingTask onDemandStartReindexing(String auId,
      boolean needFullReindex) {
    final String DEBUG_HEADER = "onDemandStartReindexing(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    // Get the AU.
    ArchivalUnit au = pluginMgr.getAuFromId(auId);
    if (log.isDebug3()) log.debug3("au = " + au);

    // Check whether it does not exist.
    if (au == null) {
      // Yes: Report the problem.
      String message = "Cannot find Archival Unit for auId '" + auId + "'";
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    // Schedule the pending AU.
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "Creating the reindexing task for AU: " + au.getName());

    ReindexingTask task = new ReindexingTask(au, getMetadataExtractor(au));
    task.setNewAu(false);
    task.setFullReindex(needFullReindex);

    activeReindexingTasks.put(au.getAuId(), task);

    // Add the reindexing task to the history; limit history list
    // size.
    addToIndexingTaskHistory(task);

    log.debug(DEBUG_HEADER + "Running the reindexing task for AU: "
	+ au.getName());
    runReindexingTask(task);

    return task;
  }

  /**
   * Deletes from the database the metadata of an archival unit.
   * 
   * @param auId
   *          A String with the identifier of the archival unit.
   * @return a DeleteMetadataTask with the metadata removal task.
   */
  public DeleteMetadataTask startMetadataRemoval(String auId) {
    final String DEBUG_HEADER = "startMetadataRemoval(): ";
    if (log.isDebug2()) log.debug2("auId = " + auId);

    // Get the AU.
    ArchivalUnit au = pluginMgr.getAuFromId(auId);
    if (log.isDebug3()) log.debug3("au = " + au);

    // Check whether it does not exist.
    if (au == null) {
      // Yes: Report the problem.
      String message = "Cannot find Archival Unit for auId '" + auId + "'";
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    // Schedule the removal of the AU.
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "Creating the metadata removal task for AU: " + au.getName());

    DeleteMetadataTask task = new DeleteMetadataTask(au);

    log.debug(DEBUG_HEADER + "Running the metadata removal task for AU: "
	+ au.getName());
    runMetadataRemovalTask(task);

    return task;
  }

  /**
   * Runs the specified metadata removal task.
   * <p>
   * Temporary implementation runs as a LockssRunnable in a thread rather than
   * using the SchedService.
   * 
   * @param task A DeleteMetadataTask with the metadata removal task.
   */
  private void runMetadataRemovalTask(final DeleteMetadataTask task) {
    /*
     * Temporarily running task in its own thread rather than using SchedService
     * 
     * @todo Update SchedService to handle this case
     */
    LockssRunnable runnable =
	new LockssRunnable(AuUtil.getThreadNameFor("Removing_Metadata",
	                                           task.getAu())) {
	  public void lockssRun() {
	    startWDog(WDOG_PARAM_INDEXER, WDOG_DEFAULT_INDEXER);
	    task.setWDog(this);

	    task.handleEvent(Schedule.EventType.START);

	    while (!task.isFinished()) {
	      task.step(Integer.MAX_VALUE);
	    }

	    task.handleEvent(Schedule.EventType.FINISH);
	    stopWDog();
	  }
	};

    Thread runThread = new Thread(runnable);
    runThread.start();
  }

  /**
   * Stores in the database the metadata for an item belonging to an AU.
   * 
   * @param item
   *          An ItemMetadata with the AU item metadata.
   * @return a Long with the database identifier of the metadata item.
   * @throws Exception
   *           if any problem occurred.
   */
  public Long storeAuItemMetadata(ItemMetadata item) throws Exception {
    final String DEBUG_HEADER = "storeAuItemMetadata(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "item = " + item);

    String auId = item.getScalarMap().get("au_id");
    if (log.isDebug3()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    return storeAuItemMetadata(item, null, pluginMgr.getPluginFromAuId(auId),
	auId, 0);
 }

  /**
   * Stores in the database the metadata for an item belonging to an AU.
   * 
   * @param item
   *          An ItemMetadata with the AU item metadata.
   * @param au
   *          An ArchivalUnit with the AU to be written to the database.
   * @param plugin
   *          A Plugin with the AU plugin to be written to the database.
   * @param auId
   *          A String with the archival unit identifier.
   * @param creationTime
   *          A long with the archival unit creation time.
   * @return a Long with the database identifier of the metadata item.
   * @throws Exception
   *           if any problem occurred.
   */
  public Long storeAuItemMetadata(ItemMetadata item, ArchivalUnit au,
      Plugin plugin, String auId, long creationTime) throws Exception {
    final String DEBUG_HEADER = "storeAuItemMetadata(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "item = " + item);
      log.debug2(DEBUG_HEADER + "auId = " + auId);
    }

    Long mdItemSeq = null;
    Connection conn = null;

    ArticleMetadataBuffer articleMetadataInfoBuffer = null;
    try {
      articleMetadataInfoBuffer =
	  new ArticleMetadataBuffer(new File(PlatformUtil.getSystemTempDir()));

      ArticleMetadata md = populateArticleMetadata(item);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "md = " + md);

      articleMetadataInfoBuffer.add(md);

      Iterator<ArticleMetadataInfo> mditr =
          articleMetadataInfoBuffer.iterator();

      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the mandatory metadata fields.
      List<String> mandatoryFields = getMandatoryMetadataFields();
      if (log.isDebug3())
        log.debug3(DEBUG_HEADER + "mandatoryFields = " + mandatoryFields);

      // Write the AU metadata to the database.
      mdItemSeq = new AuMetadataRecorder(null,
	  LockssApp.getManagerByTypeStatic(MetadataQueryManager.class), this,
	  au, plugin, auId).recordMetadataItem(conn, mandatoryFields, mditr,
	      creationTime);

      // Complete the database transaction.
      MetadataDbManager.commitOrRollback(conn, log);
    } catch (Exception e) {
      log.error("Error storing AU item metadata", e);
      log.error("item = " + item);
      throw e;
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
      articleMetadataInfoBuffer.close();
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
 }

  /**
   * Populates an ArticleMetadata with data provided in an ItemMetadata object.
   * 
   * @param item
   *          An ItemMetadata with the source of the data.
   * @return an ArticleMetadata populated with the source data.
   */
  private ArticleMetadata populateArticleMetadata(ItemMetadata item) {
    Map<String, String> scalarMap = item.getScalarMap();

    if (scalarMap == null) {
      scalarMap = new HashMap<String, String>();
    }

    Map<String, Set<String>> setMap = item.getSetMap();

    if (setMap == null) {
      setMap = new HashMap<String, Set<String>>();
    }

    Map<String, List<String>> listMap = item.getListMap();

    if (listMap == null) {
      listMap = new HashMap<String, List<String>>();
    }

    Map<String, Map<String, String>> mapMap = item.getMapMap();

    if (mapMap == null) {
      mapMap = new HashMap<String, Map<String, String>>();
    }

    ArticleMetadata am = new ArticleMetadata();

    am.put(MetadataField.FIELD_PUBLISHER, scalarMap.get(PUBLISHER_NAME_COLUMN));
    am.put(MetadataField.FIELD_PROVIDER, scalarMap.get(PROVIDER_NAME_COLUMN));
    am.put(MetadataField.FIELD_SERIES_TITLE,
	scalarMap.get("series_title_name"));
    am.put(MetadataField.FIELD_PROPRIETARY_SERIES_IDENTIFIER,
	scalarMap.get("proprietary_series_identifier"));
    am.put(MetadataField.FIELD_PUBLICATION_TITLE,
	scalarMap.get("publication_name"));

    Map<String, String> isbnMap = mapMap.get(ISBN_COLUMN);

    if (isbnMap != null && isbnMap.size() > 0) {
      am.put(MetadataField.FIELD_ISBN, isbnMap.get(P_ISBN_TYPE));
      am.put(MetadataField.FIELD_EISBN, isbnMap.get(E_ISBN_TYPE));
    }

    Map<String, String> issnMap = mapMap.get(ISSN_COLUMN);

    if (issnMap != null && issnMap.size() > 0) {
      am.put(MetadataField.FIELD_ISSN, issnMap.get(P_ISSN_TYPE));
      am.put(MetadataField.FIELD_EISSN, issnMap.get(E_ISSN_TYPE));
    }

    am.put(MetadataField.FIELD_VOLUME, scalarMap.get(VOLUME_COLUMN));
    am.put(MetadataField.FIELD_ISSUE, scalarMap.get(ISSUE_COLUMN));
    am.put(MetadataField.FIELD_START_PAGE, scalarMap.get(START_PAGE_COLUMN));
    am.put(MetadataField.FIELD_END_PAGE, scalarMap.get(END_PAGE_COLUMN));
    am.put(MetadataField.FIELD_DATE, scalarMap.get(DATE_COLUMN));
    am.put(MetadataField.FIELD_ARTICLE_TITLE, scalarMap.get("item_title"));

    List<String> authors = listMap.get(AUTHOR_NAME_COLUMN);

    if (authors != null) {
      for (String author : authors) {
	am.put(MetadataField.FIELD_AUTHOR, author);
      }
    }

    am.put(MetadataField.FIELD_DOI, scalarMap.get(DOI_COLUMN));

    Map<String, String> urlMap = mapMap.get(URL_COLUMN);

    if (urlMap != null && urlMap.size() > 0) {
      am.put(MetadataField.FIELD_ACCESS_URL, urlMap.get("Access"));
      am.putRaw(MetadataField.FIELD_FEATURED_URL_MAP.getKey(), urlMap);
    }

    Set<String> keywords = setMap.get(KEYWORD_COLUMN);

    if (keywords != null) {
      for (String keyword : keywords) {
	am.put(MetadataField.FIELD_KEYWORDS, keyword);
      }
    }

    am.put(MetadataField.FIELD_COVERAGE, scalarMap.get(COVERAGE_COLUMN));
    am.put(MetadataField.FIELD_ITEM_NUMBER, scalarMap.get(ITEM_NO_COLUMN));

    Set<String> pis = setMap.get(PROPRIETARY_ID_COLUMN);

    if (pis != null && pis.size() > 0) {
      am.put(MetadataField.FIELD_PROPRIETARY_IDENTIFIER, pis.iterator().next());
    }

    am.put(MetadataField.FIELD_FETCH_TIME, scalarMap.get(FETCH_TIME_COLUMN));

    return am;
  }

  /**
   * Deletes from the database an Archival Unit given its identifier.
   * 
   * @param auId
   *          A String with the AU identifier.
   * @return an int with the number of articles deleted.
   * @throws IllegalArgumentException
   *           if the Archival Unit cannot be found in the database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public int deleteAu(String auId) throws DbException {
    final String DEBUG_HEADER = "deleteAu(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auid = " + auId);

    Connection conn = null;
    int itemCount = 0;

    try {
      conn = dbManager.getConnection();

      if (conn == null) {
	String message = "Cannot delete Archival Unit for auid '" + auId
	    + "' - Cannot connect to database";

	log.error(message);
	throw new DbException(message);
      }

      Long auSeq = mdxManagerSql.findAuByAuId(conn, auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

      if (auSeq == null) {
	throw new IllegalArgumentException("AuId not found in DB: " + auId);
      }

      // Remove the AU from the database.
      itemCount = deleteAu(conn, auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "itemCount = " + itemCount);

      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "itemCount = " + itemCount);
    return itemCount;
  }

  /**
   * Provides the name of an Archival Unit.
   * 
   * @param auId
   *          a String with the Archival Unit identifier.
   * @return a String with the Archival Unit name.
   * @throws IllegalArgumentException
   *           if the Archival Unit does not exist.
   * @throws Exception
   *           if there are problems getting the Archival Unit name.
   */
  private String getAuName(String auId)
      throws IllegalArgumentException, Exception {
    final String DEBUG_HEADER = "getAuName(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String message = "Cannot find Archival Unit for auId '" + auId + "'";

    try {
      // Get the Archival Unit.
      ArchivalUnit au = pluginMgr.getAuFromId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "au = " + au);

      // Check whether it does exist.
      if (au != null) {
	// Yes: Get its name.
	String auName = au.getName();
	if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auName = " + auName);
	return auName;
      }
    } catch (IllegalArgumentException iae) {
      log.error(message, iae);
      throw iae;
    } catch (Exception e) {
      log.error(message, e);
      throw e;
    }

    // It does not exist: Report the problem.
    log.error(message);
    throw new IllegalArgumentException(message);
  }

  /**
   * Schedules the extraction and storage of all or part of the metadata for an
   * Archival Unit.
   * 
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @throws Exception
   *           if there are problems scheduling the metadata extraction.
   */
  public void scheduleMetadataExtraction(ArchivalUnit au, String auId)
      throws Exception {
    final String DEBUG_HEADER = "scheduleMetadataExtraction(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "au = " + au);
      log.debug2(DEBUG_HEADER + "auId = " + auId);
    }

    try {
      boolean fullReindex = true;

      if (au != null) {
	fullReindex = isAuMetadataForObsoletePlugin(au);
	if (log.isDebug3()) log.debug3("fullReindex = " + fullReindex);
      }

      JobAuStatus jobAuStatus =
	  jobMgr.scheduleMetadataExtraction(auId, fullReindex);
      log.info("Scheduled metadata extraction job: " + jobAuStatus);
    } catch (Exception e) {
      log.error("Cannot reindex metadata for " + auId, e);
      throw e;
    }
  }
}
