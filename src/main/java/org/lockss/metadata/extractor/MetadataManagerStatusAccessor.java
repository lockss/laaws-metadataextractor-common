/*

Copyright (c) 2012-2019 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.metadata.MetadataManager.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lockss.daemon.status.ColumnDescriptor;
import org.lockss.daemon.status.StatusAccessor;
import org.lockss.daemon.status.StatusService.NoSuchTableException;
import org.lockss.daemon.status.StatusTable;
import org.lockss.metadata.ArticleMetadataBuffer.ArticleMetadataInfo;
import org.lockss.metadata.MetadataIndexingException;
import org.lockss.metadata.extractor.MetadataExtractorManager.PrioritizedAuId;
import org.lockss.metadata.extractor.MetadataExtractorManager.ReindexingStatus;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.Plugin;
import org.lockss.plugin.PluginManager;
import org.lockss.plugin.PluginStatus;
import org.lockss.state.ArchivalUnitStatus;
import org.lockss.util.CatalogueOrderComparator;
import org.lockss.util.ListUtil;
import org.lockss.util.Logger;
import org.lockss.util.time.TimeBase;

/**
 * This class displays the MetadataManager status for the current
 * and most recently run indexing operations, for pending reindexing
 * tasks (key: pending), and for reindexing task errors (key: errors).
 * 
 * @author Philip Gust
 * @version 1.0
 *
 */
public class MetadataManagerStatusAccessor implements StatusAccessor {
  static final String NEW_INDEX_TEXT = "New Index";
  static final String FULL_REINDEX_TEXT = "Full Reindex";
  static final String REINDEX_TEXT = "Reindex";

  private static Logger log =
      Logger.getLogger(MetadataManagerStatusAccessor.class);

  final MetadataExtractorManager mdxMgr;
  private String key = null;
  
  private static final String AU_COL_NAME = "au";
  private static final String INDEX_TYPE = "index_type";
  private static final String START_TIME_COL_NAME = "start";
  private static final String INDEX_DURATION_COL_NAME = "index_dur";
  private static final String UPDATE_DURATION_COL_NAME = "update_dur";
  private static final String INDEX_STATUS_COL_NAME = "status";
  private static final String NUM_INDEXED_COL_NAME = "num_indexed";
  private static final String NUM_UPDATED_COL_NAME = "num_updated";
  // Sort keys, not visible columns
  private static final String SORT_KEY1 = "sort1";
  private static final String SORT_KEY2 = "sort2";
  
  private static int SORT_BASE_INDEXING = 0;
  private static int SORT_BASE_WAITING = 1000000;
  private static int SORT_BASE_DONE = 2000000;

  final private List<ColumnDescriptor> colDescs =
      ListUtil.list(new ColumnDescriptor[] {
        new ColumnDescriptor(AU_COL_NAME, "Journal Volume",
                             ColumnDescriptor.TYPE_STRING)
        .setComparator(CatalogueOrderComparator.SINGLETON),
        new ColumnDescriptor(INDEX_TYPE, "Index Type",
                             ColumnDescriptor.TYPE_STRING,
                             "Indicates whether new content is being indexed "
                        	 + "or existing content index is being fully "
                        	 + "or partially updated"),
        new ColumnDescriptor(INDEX_STATUS_COL_NAME, "Status",
                             ColumnDescriptor.TYPE_STRING,
                             "Status of indexing operation."),
        new ColumnDescriptor(START_TIME_COL_NAME, "Start Time",
                             ColumnDescriptor.TYPE_DATE,
                             "Start date and time of indexing operation"),
        new ColumnDescriptor(INDEX_DURATION_COL_NAME, "Index Duration",
                             ColumnDescriptor.TYPE_TIME_INTERVAL,
                             "Duration of metadata indexing, including"
                             + " scanning articles and extracting metadata."),
        new ColumnDescriptor(NUM_INDEXED_COL_NAME, "Articles Indexed",
                             ColumnDescriptor.TYPE_INT),
        new ColumnDescriptor(UPDATE_DURATION_COL_NAME, "Update Duration",
                             ColumnDescriptor.TYPE_TIME_INTERVAL,
                             "Duration of updating stored metadata."),
        new ColumnDescriptor(NUM_UPDATED_COL_NAME, "Articles Updated",
                             ColumnDescriptor.TYPE_INT),
      });

  // ascending by category, descending start or end time
  private final List<StatusTable.SortRule> sortRules =
      (List<StatusTable.SortRule>) ListUtil
	  .list(new StatusTable.SortRule(SORT_KEY1, true),
		new StatusTable.SortRule(SORT_KEY2, false));

  /**
   * Creates new instance for a metadata extractor manager.
   * 
   * @param mdxMgr
   *          A MetadataExtractorManager with the metadata extractor manager.
   */
  public MetadataManagerStatusAccessor(MetadataExtractorManager mdxMgr) {
    this.mdxMgr = mdxMgr;
  }
  
  /**
   * Get an error summary for the task at the specified time.
   * @param taskTime the task time for locating the task
   * @return a list of SummaryInfo objects to display
   */
  private List<StatusTable.SummaryInfo> getErrorItemSummaryInfo(long taskTime) {
    DisplayReindexingTask task = null;
    List<StatusTable.SummaryInfo> res =
        new ArrayList<StatusTable.SummaryInfo>();

    if (taskTime == 0) {
      // debugging only -- select the first reindexing task to display
      List<DisplayReindexingTask> tasks = mdxMgr.getReindexingTasks();
      if (tasks.size() > 0) {
        task = tasks.get(0);
      }
    } else {
      // select the failed reindexing task for the specified time 
      List<DisplayReindexingTask> tasks = mdxMgr.getFailedReindexingTasks();
      for (DisplayReindexingTask t : tasks) {
        if (taskTime == t.getStartTime()) {
          task = t;
          break;
        }
      }
    }

    // spacer to offset from title
    res.add(new StatusTable.SummaryInfo(
        null,
        ColumnDescriptor.TYPE_STRING,
        "\u00A0"));  // unicode non-breaking space

    if ( task == null) {
      // report specified task not available
      res.add(new StatusTable.SummaryInfo(
          null,
          ColumnDescriptor.TYPE_STRING,
          "Reindexing task no longer available"));
      res.add(new StatusTable.SummaryInfo(
          null,
          ColumnDescriptor.TYPE_STRING,
          new StatusTable.Reference("Back to Metadata Indexing Errors", 
              METADATA_STATUS_TABLE_NAME,
              "errors")));
    } else {
      // report information for specified task
      res.add(new StatusTable.SummaryInfo(
          "Volume",
          ColumnDescriptor.TYPE_STRING,
          task.getAuName()));
      PluginManager pluginMgr = mdxMgr.getApp().getPluginManager();
      Plugin plugin = pluginMgr.getPluginFromId(
	  PluginManager.pluginIdFromAuId(task.getAuId()));
      res.add(new StatusTable.SummaryInfo(
          "Plugin",
          ColumnDescriptor.TYPE_STRING,
          PluginStatus.makePlugRef(plugin.getPluginName(), plugin)));
      res.add(new StatusTable.SummaryInfo(
          "Index Type",
          ColumnDescriptor.TYPE_STRING,
          getIndexTypeDisplayString(task)));
  
      // show reindexing status string
      String status;
      ReindexingStatus reindexingStatus = task.getReindexingStatus();
      switch (reindexingStatus) {
        case Success:
          status = "Success";
          break;
        case Failed:
          status = "Failed";
          break;
        case Rescheduled:
          status = "Rescheduled";
          break;
        default:
          status = reindexingStatus.toString();
      }
  
      res.add(new StatusTable.SummaryInfo(
          "Status",
          ColumnDescriptor.TYPE_STRING,
          status));
  
      res.add(new StatusTable.SummaryInfo(
          "Has substance",
          ColumnDescriptor.TYPE_STRING,
          task.hasNoAuSubstance() ? "No" : "Yes"));
  
      res.add(new StatusTable.SummaryInfo(
          "Start time",
          ColumnDescriptor.TYPE_DATE,
          task.getStartTime()));

      // Check whether there are separate indexing and updating statistics.
      if (task.getStartUpdateTime() != 0) {
	res.add(new StatusTable.SummaryInfo(
	    "Index duration",
	    ColumnDescriptor.TYPE_TIME_INTERVAL,
	    task.getStartUpdateTime() - task.getStartTime()));
      
	res.add(new StatusTable.SummaryInfo(
	    "Articles indexed",
	    ColumnDescriptor.TYPE_INT,
	    task.getIndexedArticleCount()));
  
	res.add(new StatusTable.SummaryInfo(
	    "Update duration",
	    ColumnDescriptor.TYPE_TIME_INTERVAL,
	    task.getEndTime() - task.getStartUpdateTime()));
      
	res.add(new StatusTable.SummaryInfo(
	    "Articles updated",
	    ColumnDescriptor.TYPE_INT,
	    task.getUpdatedArticleCount()));
      } else {
	res.add(new StatusTable.SummaryInfo(
	    "Total duration",
	    ColumnDescriptor.TYPE_TIME_INTERVAL,
	    task.getEndTime() - task.getStartTime()));
      }

      res.add(new StatusTable.SummaryInfo(
          null,
          ColumnDescriptor.TYPE_STRING,
          new StatusTable.Reference("AU configuration", 
              ArchivalUnitStatus.AU_DEFINITION_TABLE_NAME, 
              task.getAuId())));
  
      res.add(new StatusTable.SummaryInfo(
          null,
          ColumnDescriptor.TYPE_STRING,
          new StatusTable.Reference("Back to Metadata Indexing Errors", 
              METADATA_STATUS_TABLE_NAME,
              "errors")));
      
      Exception taskException = task.getException();
      if (taskException != null) {
        // spacer
        res.add(new StatusTable.SummaryInfo(
            null,
            ColumnDescriptor.TYPE_STRING,
            "\u00A0"));  // unicode non-breaking space

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        taskException.printStackTrace(pw);
        res.add(new StatusTable.SummaryInfo(
            "Exception",
            ColumnDescriptor.TYPE_STRING,
            sw.toString()));
        // show metadata info for a MedataException
        if (taskException instanceof MetadataIndexingException) {
          ArticleMetadataInfo info = ((MetadataIndexingException)taskException)
              .getArticleMetadataInfo();
          if (info != null) {
            res.add(new StatusTable.SummaryInfo(
                "MetadataInfo",
                ColumnDescriptor.TYPE_STRING,
                info.toString()));
          }
        }
      }
    }
    
    return res;
  }

  /**
   * Provides the index type text to be displayed.
   * 
   * @param task
   *          A ReindexingTask with the indexing task.
   * @return a String with the index type text to be displayed.
   */
  private String getIndexTypeDisplayString(DisplayReindexingTask task) {
    return task.isNewAu() ? NEW_INDEX_TEXT :
      (task.needsFullReindex() ? FULL_REINDEX_TEXT : REINDEX_TEXT);
  }

  /**
   * Get summary info that is displayed above a list of items.
   * @return a list of SummaryInfo objects to display
   */
  private List<StatusTable.SummaryInfo> getSummaryInfo() {
    List<StatusTable.SummaryInfo> res =
	new ArrayList<StatusTable.SummaryInfo>();
    long activeOps = mdxMgr.getActiveReindexingCount();
    long pendingOps = mdxMgr.getPendingAusCount();
    long successfulOps = mdxMgr.getSuccessfulReindexingCount();
    long failedOps = mdxMgr.getFailedReindexingCount();
    long articleCount = mdxMgr.getArticleCount();
    long publicationCount = mdxMgr.getMetadataManager().getPublicationCount();
    long publisherCount = mdxMgr.getPublisherCount();
    long providerCount = mdxMgr.getProviderCount();
    boolean indexingEnabled = mdxMgr.isIndexingEnabled();
    
    if (activeOps > 0 && !"indexing".equals(key)) {
      res.add(new StatusTable.SummaryInfo(
          "Active Indexing Operations",
          ColumnDescriptor.TYPE_INT,
          new StatusTable.Reference(activeOps,
              METADATA_STATUS_TABLE_NAME)));
    } else {
      res.add(new StatusTable.SummaryInfo(
          "Active Indexing Operations",
          ColumnDescriptor.TYPE_INT,
          activeOps));
    }

    if (pendingOps > 0 && !"pending".equals(key)) {
      res.add(new StatusTable.SummaryInfo(
          "Pending Indexing Operations",
        ColumnDescriptor.TYPE_INT,
        new StatusTable.Reference(pendingOps,
            METADATA_STATUS_TABLE_NAME,
            "pending")));
    } else {
      res.add(new StatusTable.SummaryInfo(
          "Pending Indexing Operations",
          ColumnDescriptor.TYPE_INT,
          pendingOps));
    }

    if (successfulOps > 0 && !"indexing".equals(key)) {
      res.add(new StatusTable.SummaryInfo(
          "Successful Indexing Operations",
          ColumnDescriptor.TYPE_INT,
          new StatusTable.Reference(successfulOps,
              METADATA_STATUS_TABLE_NAME)));
    } else {
      res.add(new StatusTable.SummaryInfo(
          "Successful Indexing Operations",
          ColumnDescriptor.TYPE_INT,
          successfulOps));
    }

    if (failedOps > 0 && !"errors".equals(key)) {
      res.add(new StatusTable.SummaryInfo(
        "Failed/Rescheduled Indexing Operations",
        ColumnDescriptor.TYPE_INT,
        new StatusTable.Reference(failedOps,
            METADATA_STATUS_TABLE_NAME,
            "errors")));
    } else {
      res.add(new StatusTable.SummaryInfo(
          "Failed/Rescheduled Indexing Operations",
          ColumnDescriptor.TYPE_INT,
          failedOps));
    }

    res.add(new StatusTable.SummaryInfo(
        "Total Articles in Index",
        ColumnDescriptor.TYPE_INT,
        articleCount));

    res.add(new StatusTable.SummaryInfo(
        "Total Publications in Index",
        ColumnDescriptor.TYPE_INT,
        publicationCount));

    res.add(new StatusTable.SummaryInfo(
        "Total Publishers in Index",
        ColumnDescriptor.TYPE_INT,
        publisherCount));

    res.add(new StatusTable.SummaryInfo(
        "Total Providers in Index",
        ColumnDescriptor.TYPE_INT,
        providerCount));

    res.add(new StatusTable.SummaryInfo(
        "Indexing Enabled",
        ColumnDescriptor.TYPE_STRING,
        indexingEnabled));
    
    return res;
  }

  
  List<Map<String,Object>> getRows() {
    return getTaskRows(mdxMgr.getReindexingTasks());
  }

  /**
   * Get status rows for pending AUs.
   * @param pendingAuIds the pending AU ids.
   * @return list of rows
   */
  private List<Map<String,Object>> getPrioritizedAus(
      Collection<PrioritizedAuId> pendingAuIds) {
    List<Map<String,Object>> rows = new ArrayList<Map<String,Object>>();
    PluginManager pluginMgr = mdxMgr.getApp().getPluginManager();
    for (PrioritizedAuId pendingAuId : pendingAuIds) {
      ArchivalUnit au = pluginMgr.getAuFromId(pendingAuId.auId);
      if (au == null) {
        // log error
        if (log.isDebug3()) {
          log.debug3("Unknown pending AU: " + pendingAuId.auId);
        }
      } else {
        String auName = au.getName();
        Map<String,Object> row = new HashMap<String,Object>();
        row.put(AU_COL_NAME,
                new StatusTable.Reference(auName,
                                          ArchivalUnitStatus.
                                          AU_STATUS_TABLE_NAME,
                                          pendingAuId.auId));
        row.put(INDEX_TYPE, getIndexTypeDisplayString(pendingAuId));
        row.put(INDEX_STATUS_COL_NAME, "Pending");
        rows.add(row);
      }
    }
    
    return rows;
  }

  /**
   * Provides the index type text to be displayed.
   * 
   * @param pAuId
   *          A PrioritizedAuId with the indexed Archival Unit.
   * @return a String with the index type text to be displayed.
   */
  String getIndexTypeDisplayString(PrioritizedAuId pAuId) {
    return pAuId.isNew ? NEW_INDEX_TEXT :
      pAuId.needFullReindex ? FULL_REINDEX_TEXT : REINDEX_TEXT;
  }

  /**
   * Get status rows for reindexing tasks.
   * @param tasks the reindexing tasks
   * @return list of rows
   */
  private List<Map<String,Object>>
  getTaskRows(Collection<DisplayReindexingTask> tasks) {
    List<Map<String,Object>> rows = new ArrayList<Map<String,Object>>();
    int rowNum = 0;
    for (DisplayReindexingTask task : tasks) {
      String auName = task.getAuName();
      String auId = task.getAuId();
      boolean auNoSubstance = task.hasNoAuSubstance();
      long startTime = task.getStartTime();
      long startUpdateTime = task.getStartUpdateTime();
      long endTime = task.getEndTime();
      ReindexingStatus indexStatus = task.getReindexingStatus();
      long numIndexed = task.getIndexedArticleCount();
      long numUpdated = task.getUpdatedArticleCount();
      long curTime = TimeBase.nowMs();
      
      Map<String,Object> row = new HashMap<String,Object>();
      row.put(AU_COL_NAME,
              new StatusTable.Reference(auName,
                                        ArchivalUnitStatus.AU_STATUS_TABLE_NAME,
                                        auId));
      row.put(INDEX_TYPE, getIndexTypeDisplayString(task));
      
      if (startTime == 0) {
        // task hasn't started yet
        row.put(INDEX_STATUS_COL_NAME, "Waiting");
        // invisible keys for sorting
        row.put(SORT_KEY1, SORT_BASE_WAITING);
        row.put(SORT_KEY2, rowNum);
      } else if (endTime == 0) {
        // task hasn't finished yet
	if (startUpdateTime == 0) {
	  // task is running but hasn't finished indexing yet
	  row.put(START_TIME_COL_NAME, startTime);
	  row.put(INDEX_DURATION_COL_NAME, curTime-startTime);
	  row.put(INDEX_STATUS_COL_NAME, "Indexing");
	  row.put(NUM_INDEXED_COL_NAME, numIndexed);
	  // invisible keys for sorting
	  row.put(SORT_KEY1, SORT_BASE_INDEXING);
	  row.put(SORT_KEY2, startTime);
	} else {
	  // task is finished indexing but hasn't finished updating yet
	  row.put(START_TIME_COL_NAME, startTime);
	  row.put(INDEX_DURATION_COL_NAME, startUpdateTime-startTime);
	  row.put(UPDATE_DURATION_COL_NAME, curTime-startUpdateTime);
	  row.put(INDEX_STATUS_COL_NAME, "Updating");
	  row.put(NUM_INDEXED_COL_NAME, numIndexed);
	  row.put(NUM_UPDATED_COL_NAME, numUpdated);
	  // invisible keys for sorting
	  row.put(SORT_KEY1, SORT_BASE_INDEXING);
	  row.put(SORT_KEY2, startTime);
	}
      } else {
        // task is finished
        row.put(START_TIME_COL_NAME, startTime);

        // Check whether there are separate indexing and updating statistics.
        if (startUpdateTime != 0) {
          // Yes: Use them.
          row.put(INDEX_DURATION_COL_NAME, startUpdateTime-startTime);
          row.put(UPDATE_DURATION_COL_NAME, endTime-startUpdateTime);

          row.put(NUM_INDEXED_COL_NAME, numIndexed);
          row.put(NUM_UPDATED_COL_NAME, numUpdated);
        }

        Object status;
        switch (indexStatus) {
          case Success:
            status = "Success";
            break;
          case Failed:
            status = "Failed";
            break;
          case Rescheduled:
            status = "Rescheduled";
            break;
          default:
            status = indexStatus.toString();
        }
        if (auId != null) {
          if ((indexStatus == ReindexingStatus.Success) && auNoSubstance) {
            status =
              new StatusTable.DisplayedValue(status).setFootnote(
                "Though metadata indexing finished successfully, no"
              + " article files containing substantial content were found");
          }
        }
        if (task.getException() != null) {
          row.put(INDEX_STATUS_COL_NAME,
                  new StatusTable.Reference(status,
                      METADATA_STATUS_TABLE_NAME,
                      Long.toString(task.getStartTime())));
          
        } else {
          row.put(INDEX_STATUS_COL_NAME, status);
        }

        // invisible keys for sorting
        row.put(SORT_KEY1, SORT_BASE_DONE);
        row.put(SORT_KEY2, endTime);
      }
      rows.add(row);
      rowNum++;
    }
    
    return rows;
  }

  private List<ColumnDescriptor> getColDescs() {
    List<ColumnDescriptor> res =
	new ArrayList<ColumnDescriptor>(colDescs.size());

    for (ColumnDescriptor desc : colDescs) {
      res.add(desc);
    }

    return res;
  }

  @Override
  public void populateTable(StatusTable table) throws NoSuchTableException {
    key = (table.getKey() == null) ? "indexing" : table.getKey();
    try {
      long taskTime = Long.parseLong(key);
      table.setTitle("Metadata Indexing Error Information");
      table.setSummaryInfo(getErrorItemSummaryInfo(taskTime));
    } catch (NumberFormatException ex) {
      if ("pending".equals(key)) {
        // list pending
        table.setTitle("Metadata Pending Index Status");
        table.setRows(getPrioritizedAus(mdxMgr.getPendingReindexingAus()));
      } else if ("errors".equals(key)) {
        // list errors
        table.setTitle("Metadata Indexing Errors");
        table.setRows(getTaskRows(mdxMgr.getFailedReindexingTasks()));
      } else {
        // list indexing status
        key = "indexing";
        table.setTitle("Metadata Indexing Status");
        table.setRows(getTaskRows(mdxMgr.getReindexingTasks()));
      }
      table.setDefaultSortRules(sortRules);
      table.setColumnDescriptors(getColDescs());
      table.setSummaryInfo(getSummaryInfo());
    }
  }

  @Override
  public String getDisplayName() {
    return "Metadata Indexing Status";
  }

  @Override
  public boolean requiresKey() {
    return false;
  }
  
}
