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

import static org.lockss.metadata.SqlConstants.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
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
   */
  public MetadataIndexingStarter(MetadataDbManager dbManager,
      MetadataExtractorManager mdxManager, PluginManager pluginManager,
      JobManager jobManager) {
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

    // Get a connection to the database.
    Connection conn;

    try {
      conn = dbManager.getConnection();
    } catch (DbException dbe) {
      log.error("Cannot connect to database -- extraction not started", dbe);
      return;
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

    log.debug2(DEBUG_HEADER + "Examining AUs");

    List<ArchivalUnit> toBeIndexed = new ArrayList<ArchivalUnit>();

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
    log.debug2(DEBUG_HEADER + "Done examining AUs");

    // Loop in random order through all the AUs to to be added the pending
    // queue.
    for (ArchivalUnit au : (Collection<ArchivalUnit>)
	    CollectionUtil.randomPermutation(toBeIndexed)) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Pending AU = " + au.getName());

      try {
      	// Determine whether the AU needs to be fully reindexed.
	boolean fullReindex = mdxManager.isAuMetadataForObsoletePlugin(conn, au);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "fullReindex = " + fullReindex);

   	// Add the AU to the table of pending AUs, if not already there.
	mdxManager.addToPendingAusIfNotThere(conn, Collections.singleton(au),
	    fullReindex);
	MetadataDbManager.commitOrRollback(conn, log);
	log.debug2(DEBUG_HEADER + "Queue updated");
      } catch (DbException dbe) {
	log.error("Cannot add to pending AUs table \"" + PENDING_AU_TABLE
	    + "\"", dbe);
	MetadataDbManager.safeRollbackAndClose(conn);
	return;
      }
    }

    log.debug2(DEBUG_HEADER + "Done updating the queue");

    // Start the reindexing process.
    try {
      log.info(DEBUG_HEADER + "Starting startReindexing...");
      mdxManager.startReindexing(conn);
      conn.commit();
    } catch (SQLException sqle) {
      log.error("Cannot start reindexing AUs", sqle);
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
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
      if (log.isDebug2())
	log.debug2("event = " + event + ", auId = " + auId + ", au = " + au);

      // TODO: Remove once the tests do not rely on this.
      if (au != null) {
	switch (event.getType()) {
	  case Delete:
	    // This case occurs when the AU is being deleted.
	    log.debug2("Delete for auId: " + auId);

	    // Insert the AU in the table of unconfigured AUs.
	    mdxManager.persistUnconfiguredAu(au);

	    // Delete the AU metadata.
	    mdxManager.deleteAuAndReindex(au);
	    break;
	  case RestartDelete:
	    // This case occurs when the plugin is about to restart. There is
	    // nothing to do in this case but wait for the plugin to be
	    // reactivated and see whether anything needs to be done.
	    break;
	  case Deactivate:
	    // This case occurs when the AU is being deactivated.
	    if (log.isDebug3()) log.debug3("Deactivate for auId: " + auId);

	    Connection conn = null;

	    try {
	      conn = dbManager.getConnection();

	      // Mark the AU as inactive in the database.
	      dbManager.updateAuActiveFlag(conn, auId, false);
	      MetadataDbManager.commitOrRollback(conn, log);
	    } catch (DbException dbe) {
	      log.error("Cannot deactivate AU " + au.getName(), dbe);
	    } finally {
	      MetadataDbManager.safeRollbackAndClose(conn);
	    }

	    break;
	  default:
	}
      } else {
	if (log.isDebug3()) log.debug3("Ignored because it is handled by "
	    + "ArchivalUnitConfigurationCallback.auConfigRemoved()");
      }

      if (log.isDebug2()) log.debug2("Done.");
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
	    Connection conn = null;
	    PreparedStatement insertPendingAuBatchStatement = null;

	    try {
  	      boolean fullReindex = true;

  	      if (au != null) {
  		fullReindex = mdxManager.isAuMetadataForObsoletePlugin(au);
  		if (log.isDebug3()) log.debug3("fullReindex = " + fullReindex);
  	      }

  	      JobAuStatus jobAuStatus =
  		  jobManager.scheduleMetadataExtraction(auId, fullReindex);
  	      log.info("Scheduled metadata extraction job: " + jobAuStatus);

  	      // TODO:Remove once the tests do not rely on this.
  	      if (au != null) {
  		conn = dbManager.getConnection();
  		insertPendingAuBatchStatement =
  		    mdxManager.getInsertPendingAuBatchStatement(conn);
  		mdxManager.enableAndAddAuToReindex(au, conn,
  		    insertPendingAuBatchStatement, event.isInBatch());
  	      }
	    } catch (DbException dbe) {
	      log.error("Cannot reindex metadata for " + au.getName(), dbe);
	    } catch (Exception e) {
	      log.error("Cannot reindex metadata for " + auId, e);
	    } finally {
  	      if (au != null) {
  		MetadataDbManager
  		.safeCloseStatement(insertPendingAuBatchStatement);
  		MetadataDbManager.safeRollbackAndClose(conn);
  	      }
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
      if (log.isDebug2()) log.debug2("auId = " + auId);

      Connection conn = null;
      PreparedStatement insertPendingAuBatchStatement = null;

      try {
        conn = dbManager.getConnection();

        // Mark the AU as active.
        dbManager.updateAuActiveFlag(conn, auId, true);
        MetadataDbManager.commitOrRollback(conn, log);

        // Remove the AU from the table of unconfigured AUs.
        mdxManager.getMetadataExtractorManagerSql()
        .removeFromUnconfiguredAus(conn, auId);

        // This case occurs when the user has added or reactivated an AU
        // through the GUI.
        // If this restores an existing AU that has already crawled, we
        // schedule it to be added to the metadata database now. Otherwise
        // it will be added through auContentChanged() once the crawl has
        // been completed.
        // TODO: How to determine the AuState or whether the AU has been crawled?
//        if (AuUtil.getAuState(au) != null && AuUtil.hasCrawled(au)) {
//          // TODO: Fix to pass AUId,not AU.
//          boolean fullReindex = mdxManager.isAuMetadataForObsoletePlugin(au);
//          if (log.isDebug3()) log.debug3("fullReindex = " + fullReindex);
//
//          JobAuStatus jobAuStatus =
//              jobManager.scheduleMetadataExtraction(auId, fullReindex);
//	    log.info("Scheduled metadata extraction job: " + jobAuStatus);
//        } else {
//          if (log.isDebug3()) log.debug3("Skipping because "
//              + "AuUtil.getAuState(au) == null || !AuUtil.hasCrawled(au)");
//        }
      } catch (DbException dbe) {
        log.error("Cannot reindex metadata for " + auId, dbe);
      } catch (Exception e) {
        log.error("Cannot reindex metadata for " + auId, e);
      } finally {
	MetadataDbManager.safeRollbackAndClose(conn);
      }

      if (log.isDebug2()) log.debug2("Done");
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
