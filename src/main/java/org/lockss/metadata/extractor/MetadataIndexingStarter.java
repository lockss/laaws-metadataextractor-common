/*

Copyright (c) 2013-2019 Board of Trustees of Leland Stanford Jr. University,
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

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.lockss.app.LockssDaemon;
import org.lockss.config.AuConfiguration;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.config.Configuration.Callback;
import org.lockss.config.RestConfigClient;
import org.lockss.config.rest.AuConfigPageInfo;
import org.lockss.daemon.LockssRunnable;
import org.lockss.db.DbException;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.extractor.job.JobAuStatus;
import org.lockss.metadata.extractor.job.JobManager;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuEvent;
import org.lockss.plugin.AuEventHandler;
import org.lockss.plugin.PluginManager;
import org.lockss.util.Logger;
import org.lockss.util.rest.config.PageInfo;
import org.lockss.util.rest.exception.LockssRestException;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;

/**
 * Starts the metadata indexing process.
 */
public class MetadataIndexingStarter extends LockssRunnable {
  private static Logger log = Logger.getLogger(MetadataIndexingStarter.class);

  private final MetadataDbManager dbManager;
  private final MetadataExtractorManager mdxManager;
  private final PluginManager pluginManager;
  private final JobManager jobManager;

  /**
   * Constructor.
   * 
   * @param dbManager
   *          A DbManager with the database manager.
   * @param mdxManager
   *          A MetadataExtractorManager with the metadata extractor manager.
   * @param pluginManager
   *          A PluginManager with the plugin manager.
   * @param jobManager
   *          A JobManager with the job manager.
   * @param metadataExtractionCheckInterval
   *          A long with the interval in milliseconds between consecutive runs
   *          of the metadata extraction check.
   */
  public MetadataIndexingStarter(MetadataDbManager dbManager,
      MetadataExtractorManager mdxManager, PluginManager pluginManager,
      JobManager jobManager, long metadataExtractionCheckInterval) {
    super("MetadataStarter");

    this.dbManager = dbManager;
    this.mdxManager = mdxManager;
    this.pluginManager = pluginManager;
    this.jobManager = jobManager;
  }

  /**
   * Entry point to start the metadata extraction process.
   */
  public void lockssRun() {
    final String DEBUG_HEADER = "lockssRun(): ";
    log.debug(DEBUG_HEADER + "Starting...");
    LockssDaemon daemon = LockssDaemon.getLockssDaemon();

    // Wait until the AUs have been started.
    if (!daemon.areAusStarted()) {
      log.debug(DEBUG_HEADER + "Waiting for aus to start");

      while (!daemon.areAusStarted()) {
	try {
	  daemon.waitUntilAusStarted();
	} catch (InterruptedException ex) {
	}
      }
    }

    // Register the event handler to receive archival unit content change
    // notifications and to be able to re-index the database content associated
    // with the archival unit.
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "Registering ArchivalUnitEventHandler...");
    pluginManager.registerAuEventHandler(new ArchivalUnitEventHandler());
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "Done registering ArchivalUnitEventHandler.");

    // Register the event handler to receive archival unit configuration change
    // notifications.
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "Registering ArchivalUnitConfigurationCallback...");
    ConfigManager.getConfigManager().registerConfigurationCallback(
	new ArchivalUnitConfigurationCallback());
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "Done registering ArchivalUnitConfigurationCallback.");

    // Loop indefinitely.
    while (true) {
      // Schedule the metadata extraction of Archival Units that require it.
      scheduleNeededMetadataExtractionJobs();

      // Amount of time to wait until the next check.
      Deadline deadline = Deadline.in(mdxManager.getMetadataExtractionCheckInterval());
      log.debug3("Sleeping until " + deadline);

      // Wait until the next metadata extraction check.
      try {
        deadline.sleep();
      } catch (InterruptedException e) {
        // Intentionally left blank
      }

      log.debug3("Back from sleep.");
    }
  }

  /**
   * Scans every configured Archival Unit and enqueues any that need
   * metadata extraction.
   *
   * <p>When a {@link RestConfigClient} is active (the typical metadata-
   * service deployment, where the config service is remote), the scan
   * pages through
   * {@link RestConfigClient#getArchivalUnitConfigurationsPage(String)} and
   * processes each page fully before fetching the next — so the entire
   * AuConfiguration list is never held in memory at once. AUs whose
   * configuration has {@code reserved.disabled = true} (i.e.
   * {@link PluginManager#AU_PARAM_DISABLED}) are skipped without any DB
   * predicate evaluation.
   *
   * <p>When the RestConfigClient is not active (single-node embedded
   * deployment, or unit tests), the scan falls back to
   * {@link PluginManager#getAllAus()}. That source is incomplete in the
   * presence of unstarted AUs, but in the embedded case there is no AU
   * that exists outside this JVM to miss.
   */
  private void scheduleNeededMetadataExtractionJobs() {
    final String DEBUG_HEADER = "scheduleNeededMetadataExtractionJobs(): ";
    log.debug2(DEBUG_HEADER + "Starting...");

    RestConfigClient restClient =
        ConfigManager.getConfigManager().getRestConfigClient();

    if (restClient != null && restClient.isActive()) {
      scanViaRestConfigClient(restClient);
    } else {
      scanViaPluginManager();
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Pages through the config service, processing each page fully before
   * fetching the next.
   */
  private void scanViaRestConfigClient(RestConfigClient restClient) {
    final String DEBUG_HEADER = "scanViaRestConfigClient(): ";

    String continuationToken = null;
    int pageNum = 0;
    do {
      AuConfigPageInfo pageInfo;
      try {
        pageInfo = restClient.getArchivalUnitConfigurationsPage(
            continuationToken);
      } catch (LockssRestException lre) {
        log.error("Failed to fetch AU configurations page " + pageNum
            + " (continuationToken = " + continuationToken + ")", lre);
        return;
      }
      pageNum++;

      if (pageInfo != null && pageInfo.getAuConfigs() != null
          && !pageInfo.getAuConfigs().isEmpty()) {
        processAuConfigurationPage(pageInfo.getAuConfigs(), pageNum);
      } else if (log.isDebug3()) {
        log.debug3(DEBUG_HEADER + "page " + pageNum + " is empty");
      }

      PageInfo pageInfoData = (pageInfo == null) ?
          null : pageInfo.getPageInfo();
      continuationToken = (pageInfoData == null) ?
          null : pageInfoData.getContinuationToken();
    } while (continuationToken != null);

    log.debug2(DEBUG_HEADER + "scanned " + pageNum + " page(s).");
  }

  /**
   * Embedded/test fallback: enumerate from in-process started AUs. Misses
   * unstarted AUs, but in a single-node deployment there are none.
   */
  private void scanViaPluginManager() {
    List<String> auIds = new ArrayList<String>();
    for (ArchivalUnit au : pluginManager.getAllAus()) {
      auIds.add(au.getAuId());
    }
    processAuIds(auIds);
  }

  /**
   * Processes a single page's worth of AuConfigurations: skips AUs disabled
   * via {@code reserved.disabled}, then defers to {@link #processAuIds}
   * for the rest.
   */
  private void processAuConfigurationPage(
      Collection<AuConfiguration> pageConfigs, int pageNum) {
    final String DEBUG_HEADER = "processAuConfigurationPage(): ";

    List<String> auIds = new ArrayList<String>(pageConfigs.size());
    for (AuConfiguration auc : pageConfigs) {
      String auId = auc.getAuId();
      if (auId == null) continue;

      Map<String, String> cfg = auc.getAuConfig();
      if (cfg != null
          && "true".equalsIgnoreCase(cfg.get(PluginManager.AU_PARAM_DISABLED))) {
        if (log.isDebug3())
          log.debug3(DEBUG_HEADER + "AU '" + auId + "' is reserved.disabled");
        continue;
      }
      auIds.add(auId);
    }
    processAuIds(auIds);
  }

  /**
   * Per-AU predicate evaluation and enqueue. Shared between the REST-paged
   * and the in-process fallback paths.
   */
  private void processAuIds(Collection<String> auIds) {
    final String DEBUG_HEADER = "processAuIds(): ";
    if (auIds.isEmpty()) return;

    Connection conn;
    try {
      conn = dbManager.getConnection();
    } catch (DbException dbe) {
      log.error("Cannot connect to database -- extraction not started", dbe);
      return;
    }

    List<String> toBeIndexed = new ArrayList<String>();
    try {
      for (String auId : auIds) {
        try {
          // isAuCrawledAndNotExtracted already returns false for never-
          // crawled AUs (lastCrawlTime == -1), so no separate hasCrawled
          // guard is needed.
          if (mdxManager.isAuMetadataForObsoletePlugin(conn, auId)
              || mdxManager.isAuCrawledAndNotExtracted(conn, auId)) {
            if (log.isDebug3())
              log.debug3(DEBUG_HEADER + "AU '" + auId + "' to be indexed");
            toBeIndexed.add(auId);
          }
        } catch (DbException dbe) {
          log.error("Cannot evaluate AU '" + auId + "' for indexing", dbe);
        }
      }
    } finally {
      dbManager.safeRollbackAndClose(conn);
    }

    for (String auId : toBeIndexed) {
      try {
        mdxManager.scheduleMetadataExtraction(auId);
      } catch (Exception e) {
        log.error("Cannot reindex metadata for " + auId, e);
        // Continue with the rest; one bad enqueue should not poison the
        // scan.
      }
    }
  }

  /**
   * Event handler to receive archival unit content change notifications and to
   * be able to re-index the database content associated with the archival unit.
   */
  private class ArchivalUnitEventHandler extends AuEventHandler.Base {

    /**
     * Called for archival unit creation events.
     * 
     * @param event An AuEvent with the archival unit creation event.
     * @param auId  A String with the identifier of the archival unit involved
     *              in the event.
     * @param au    An ArchivalUnit with the archival unit involved in the
     *              event.
     */
    @Override
    public void auCreated(AuEvent event, String auId, ArchivalUnit au) {
      if (log.isDebug2()) log.debug2("Ignored because it is handled by "
	  + "ArchivalUnitConfigurationCallback.auConfigChanged()");
    }

    /**
     * Called for archival unit removal events.
     * 
     * @param event An AuEvent with the archival unit removal event.
     * @param auId  A String with the identifier of the archival unit involved
     *              in the event.
     * @param au    An ArchivalUnit with the archival unit involved in the
     *              event.
     */
    @Override
    public void auDeleted(AuEvent event, String auId, ArchivalUnit au) {
      if (log.isDebug2()) log.debug2("Ignored");
    }

    /**
     * Called for archival unit content change events.
     * 
     * @param event An AuEvent with the archival unit content change event.
     * @param auId  A String with the identifier of the archival unit involved
     *              in the event.
     * @param au    An ArchivalUnit with the archival unit involved in the
     *              event.
     * @param info  An AuEvent.ContentChangeInfo with information about the
     *              event.
     */
    @Override
    public void auContentChanged(AuEvent event, String auId, ArchivalUnit au,
	AuEvent.ContentChangeInfo info) {
      if (log.isDebug2()) log.debug2("event = " + event + ", auId = " + auId
	  + ", au = " + au + ", info = " + info);

      switch (event.getType()) {
	case ContentChanged:
	  // This case occurs after a change to the AU's content after a crawl.
	  // This code assumes that a new crawl will simply add new metadata and
	  // not change existing metadata. Otherwise,
	  // deleteOrRestartAu(au, true) should be called.
  	  if (log.isDebug3()) {
  	    log.debug3("ContentChanged for auId: " + auId);
  	    log.debug3("info.isComplete() = " + info.isComplete());
  	  }

	  if (info.isComplete()) {
	    try {
	      mdxManager.scheduleMetadataExtraction(au, auId);
	    } catch (Exception e) {
	      log.error("Cannot reindex metadata for " + auId, e);
	    }
	  } else {
	    if (log.isDebug3())
	      log.debug3("Skipping because info.isComplete() is false");
	  }

	  break;
	default:
      }

      if (log.isDebug2()) log.debug2("Done.");
    }
  }

  /**
   * Callback handler to receive configuration change notifications.
   */
  private class ArchivalUnitConfigurationCallback implements Callback {
    /**
     * Called when something in the configuration has changed.
     * 
     * It is called after the new configuration is installed as current, as well
     * as upon registration (if there is a current configuration at the time).
     * 
     * @param newConfig A Configuration with the new (just installed)
     *                  <code>Configuration</code>.
     * @param oldConfig A Configuration with the previous
     *                  <code>Configuration</code>, or null if there was no
     *                  previous configuration.
     * @param changes   A Configuration.Differences with the set of
     *                  configuration keys whose value has changed.
     */
    public void configurationChanged(Configuration newConfig,
				     Configuration oldConfig,
				     Configuration.Differences changes) {
      if (log.isDebug2()) log.debug2("Ignored");
    }

    /**
     * Called when an archival unit configuration has been created anew or
     * changed.
     * 
     * @param auId A String with the identifier of the archival unit for which
     *             the configuration has been created anew or changed.
     */
    public void auConfigChanged(String auId) {
      if (log.isDebug2()) log.debug2("Ignored");
    }

    /**
     * Called when an archival unit configuration has been deleted.
     * 
     * @param auId A String with the identifier of the archival unit for which
     *             the configuration has been deleted.
     */
    public void auConfigRemoved(String auId) {
      if (log.isDebug2()) log.debug2("auId = " + auId);

      try {
	// Insert the AU in the table of unconfigured AUs.
	mdxManager.persistUnconfiguredAu(auId);

	// Schedule a job to remove the archival unit metadatafromthe database.
	JobAuStatus jobAuStatus = jobManager.scheduleMetadataRemoval(auId);
	log.info("Scheduled metadata removal job: " + jobAuStatus);
      } catch (Exception e) {
	log.error("Cannot delete metadata for " + auId, e);
      }

      if (log.isDebug2()) log.debug2("Done");
    }
  }
}
