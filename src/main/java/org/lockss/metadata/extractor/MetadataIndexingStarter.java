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
import org.lockss.app.LockssDaemon;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.config.Configuration.Callback;
import org.lockss.daemon.LockssRunnable;
import org.lockss.db.DbException;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.extractor.job.JobAuStatus;
import org.lockss.metadata.extractor.job.JobManager;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuEvent;
import org.lockss.plugin.AuEventHandler;
import org.lockss.plugin.AuUtil;
import org.lockss.plugin.PluginManager;
import org.lockss.util.CollectionUtil;
import org.lockss.util.Logger;
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
  private final long metadataExtractionCheckInterval;

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
    this.metadataExtractionCheckInterval = metadataExtractionCheckInterval;
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
      long beforeTimestamp = TimeBase.nowMs();
      scheduleNeededMetadataExtractionJobs();

      // Compute the amount of time to wait until the next check.
      long intervalLeft = metadataExtractionCheckInterval
	  - (TimeBase.nowMs() - beforeTimestamp);

      long delay = intervalLeft >= 0 ? intervalLeft : 0;
      if (log.isDebug3()) log.debug3("Sleeping for " + delay + " ms...");

      // Wait until the next metadata extraction check.
      try {
	Thread.sleep(delay);
      } catch (InterruptedException ie) {}

      if (log.isDebug3()) log.debug3("Back from sleep.");
    }
  }

  private void scheduleNeededMetadataExtractionJobs() {
    final String DEBUG_HEADER = "scheduleNeededMetadataExtractionJobs(): ";
    log.debug2(DEBUG_HEADER + "Starting...");

    // Get a connection to the database.
    Connection conn;

    try {
      conn = dbManager.getConnection();
    } catch (DbException dbe) {
      log.error("Cannot connect to database -- extraction not started", dbe);
      return;
    }

    log.debug2(DEBUG_HEADER + "Examining AUs");

    List<ArchivalUnit> toBeIndexed = new ArrayList<ArchivalUnit>();

    try {
      // Loop through all the AUs to see which need to be on the pending queue.
      for (ArchivalUnit au : pluginManager.getAllAus()) {
        if (log.isDebug3())
          log.debug3(DEBUG_HEADER + "Plugin AU = " + au.getName());

        // Check whether the AU has not been crawled.
        if (!AuUtil.hasCrawled(au)) {
          // Yes: Do not index it.
          if (log.isDebug3())
            log.debug3(DEBUG_HEADER + "AU has not been crawled: No indexing.");
          continue;
        } else {
          // No: Check whether the plugin's md extractor version is newer
          // than the version of the metadata already in the database or
          // whether the AU metadata hasn't been extracted since the last
          // successful crawl.
          try {
            if (mdxManager.isAuMetadataForObsoletePlugin(conn, au)
                || mdxManager.isAuCrawledAndNotExtracted(conn, au)) {
              // Yes: index it.
              if (log.isDebug3())
                log.debug3(DEBUG_HEADER + "AU is to be indexed");
              toBeIndexed.add(au);
            } else {
              // No.
              if (log.isDebug3())
                log.debug3(DEBUG_HEADER + "AU does not need to be indexed");
            }
          } catch (DbException dbe) {
            log.error("Cannot get AU metadata version: " + dbe);
          }
        }
      }
    } finally {
      dbManager.safeRollbackAndClose(conn);
    }

    log.debug2(DEBUG_HEADER + "Done examining AUs");

    // Loop in random order through all the AUs to to be added to the pending
    // queue.
    for (ArchivalUnit au : (Collection<ArchivalUnit>)
	    CollectionUtil.randomPermutation(toBeIndexed)) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Pending AU = " + au.getName());

      String auId = au.getAuId();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

      try {
	mdxManager.scheduleMetadataExtraction(au, auId);
      } catch (Exception e) {
	log.error("Cannot reindex metadata for " + auId, e);
	return;
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
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
