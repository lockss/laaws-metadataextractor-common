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

import static org.lockss.metadata.SqlConstants.*;
import static org.lockss.metadata.extractor.MetadataExtractorManager.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lockss.app.LockssDaemon;
import org.lockss.config.ConfigManager;
import org.lockss.config.CurrentConfig;
import org.lockss.db.DbException;
import org.lockss.exporter.counter.CounterReportsManager;
import org.lockss.extractor.MetadataField;
import org.lockss.laaws.mdq.model.ItemMetadata;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.MetadataManager;
import org.lockss.metadata.extractor.ArticleMetadataBuffer.ArticleMetadataInfo;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuUtil;
import org.lockss.plugin.Plugin;
import org.lockss.plugin.PluginManager;
import org.lockss.util.Logger;
import org.lockss.util.MetadataUtil;
import org.lockss.util.StringUtil;
import org.lockss.util.TimeBase;

/**
 * Writes to the database metadata related to an archival unit.
 * 
 * @author Fernando García-Loygorri
 */
public class AuMetadataRecorder {
  private static Logger log = Logger.getLogger(AuMetadataRecorder.class);

  static final String UNKNOWN_PUBLISHER_AU_PROBLEM = "UNKNOWN_PUBLISHER";

  // Query to update the primary name of a metadata item.
  private static final String UPDATE_MD_ITEM_PRIMARY_NAME_QUERY = "update "
      + MD_ITEM_NAME_TABLE
      + " set " + NAME_COLUMN + " = ?"
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?"
      + " and " + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'";

  // Query to delete a metadata item non-primary name.
  private static final String DELETE_NOT_PRIMARY_MDITEM_NAMES_QUERY = "delete "
      + "from " + MD_ITEM_NAME_TABLE
      + " where "
      + MD_ITEM_SEQ_COLUMN + " = ?"
      + " and " + NAME_COLUMN + " like '" + UNKNOWN_TITLE_NAME_ROOT + "%'"
      + " and " + NAME_TYPE_COLUMN + " = '" + NOT_PRIMARY_NAME_TYPE + "'";

  // Query to delete a metadata item non-primary name.
  private static final String DELETE_NOT_PRIMARY_MDITEM_NAME_QUERY = "delete "
      + "from " + MD_ITEM_NAME_TABLE
      + " where "
      + MD_ITEM_SEQ_COLUMN + " = ?"
      + " and " + NAME_COLUMN + " = ?"
      + " and " + NAME_TYPE_COLUMN + " = '" + NOT_PRIMARY_NAME_TYPE + "'";

  // Query to update the version of an Archival Unit metadata.
  private static final String UPDATE_AU_MD_VERSION_QUERY = "update "
      + AU_MD_TABLE
      + " set " + MD_VERSION_COLUMN + " = ?"
      + " where " + AU_MD_SEQ_COLUMN + " = ?";

  // Query to find the name of the type of a metadata item.
  private static final String GET_MD_ITEM_TYPE_NAME_QUERY = "select "
      + "t." + TYPE_NAME_COLUMN
      + " from " + MD_ITEM_TYPE_TABLE + " t"
      + "," + MD_ITEM_TABLE + " m"
      + " where m." + MD_ITEM_SEQ_COLUMN + " = ?"
      + " and m." + MD_ITEM_TYPE_SEQ_COLUMN + " = t." + MD_ITEM_TYPE_SEQ_COLUMN;

  // Query to find the metadata entry of an Archival Unit.
  private static final String FIND_AU_MD_QUERY = "select "
      + AU_MD_SEQ_COLUMN
      + " from " + AU_MD_TABLE
      + " where " + AU_SEQ_COLUMN + " = ?";

  // Query to find a metadata item by its type, Archival Unit and access URL.
  private static final String FIND_MD_ITEM_QUERY = "select "
      + "m." + MD_ITEM_SEQ_COLUMN
      + " from " + MD_ITEM_TABLE + " m"
      + "," + URL_TABLE + " u"
      + " where m." + MD_ITEM_TYPE_SEQ_COLUMN + " = ?"
      + " and m." + AU_MD_SEQ_COLUMN + " = ?"
      + " and m." + MD_ITEM_SEQ_COLUMN + " = u." + MD_ITEM_SEQ_COLUMN
      + " and u." + FEATURE_COLUMN + " = '" + MetadataManager.ACCESS_URL_FEATURE
      + "'"
      + " and u." + URL_COLUMN + " = ?";

  // Query to get the name of a publisher.
  private static final String GET_PUBLISHER_NAME_QUERY = "select "
      + PUBLISHER_NAME_COLUMN
      + " from " + PUBLISHER_TABLE
      + " where " + PUBLISHER_SEQ_COLUMN + " = ?";

  // Query to find AU problems.
  private static final String FIND_AU_PROBLEMS_QUERY = "select "
      + PROBLEM_COLUMN
      + " from " + AU_PROBLEM_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to delete an AU problem entry.
  private static final String DELETE_AU_PROBLEM_QUERY = "delete from "
      + AU_PROBLEM_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?"
      + " and " + PROBLEM_COLUMN + " = ?";

  // Query to add an AU problem entry.
  private static final String INSERT_AU_PROBLEM_QUERY = "insert into "
      + AU_PROBLEM_TABLE
      + "(" + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + PROBLEM_COLUMN
      + ") values (?,?,?)";

  // Query to find the publications of a publisher.
  private static final String FIND_PUBLISHER_PUBLICATIONS_QUERY = "select "
      + PUBLICATION_SEQ_COLUMN
      + " from " + PUBLICATION_TABLE
      + " where " + PUBLISHER_SEQ_COLUMN + " = ?";

  // Query to find the metadata items of a publication.
  private static final String FIND_PUBLICATION_CHILD_MD_ITEMS_QUERY = "select "
      + "distinct "
      + "m." + MD_ITEM_SEQ_COLUMN
      + " from " + PUBLICATION_TABLE + " p"
      + "," + MD_ITEM_TABLE + " m"
      + " where p." + PUBLICATION_SEQ_COLUMN + " = ?"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN;

  // Query to update the parent sequence of a metadata item.
  private static final String UPDATE_MD_ITEM_PARENT_SEQ_QUERY = "update "
      + MD_ITEM_TABLE
      + " set " + PARENT_SEQ_COLUMN + " = ?"
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";

  // Query to delete the metadata item of a publication created for an unknown
  // publisher.
  private static final String
      DELETE_UNKNOWN_PUBLISHER_PUBLICATION_MD_ITEM_QUERY = "delete from "
      + MD_ITEM_TABLE
      + " where "
      + MD_ITEM_SEQ_COLUMN + " in ("
      + "select " + MD_ITEM_SEQ_COLUMN
      + " from " + PUBLICATION_TABLE
      + " where "
      + PUBLICATION_SEQ_COLUMN + " = ?"
      + " and (select count(*) from "
      + PUBLISHER_TABLE + " pr"
      + "," + PUBLICATION_TABLE + " p"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = p." + PUBLISHER_SEQ_COLUMN
      + " and p." + PUBLICATION_SEQ_COLUMN + " = ?"
      + " and pr." + PUBLISHER_NAME_COLUMN
      + " like '" + UNKNOWN_PUBLISHER_AU_PROBLEM + "%') = 1)";

  // Query to delete a publication created for an unknown publisher.
  private static final String DELETE_UNKNOWN_PUBLISHER_PUBLICATION_QUERY =
      "delete from " + PUBLICATION_TABLE
      + " where "
      + PUBLICATION_SEQ_COLUMN + " = ?"
      + " and (select count(*) from "
      + PUBLISHER_TABLE + " pr"
      + "," + PUBLICATION_TABLE + " p"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = p." + PUBLISHER_SEQ_COLUMN
      + " and p." + PUBLICATION_SEQ_COLUMN + " = ?"
      + " and pr." + PUBLISHER_NAME_COLUMN
      + " like '" + UNKNOWN_PUBLISHER_AU_PROBLEM + "%') = 1";

  // Query to delete an unknown publisher.
  private static final String DELETE_UNKNOWN_PUBLISHER_QUERY =
      "delete from " + PUBLISHER_TABLE
      + " where "
      + PUBLISHER_NAME_COLUMN + " = ?"
      + " and " + PUBLISHER_NAME_COLUMN
      + " like '" + UNKNOWN_PUBLISHER_AU_PROBLEM + "%'";

  // The calling task.
  private final ReindexingTask task;

  // The metadata extractor manager.
  private final MetadataExtractorManager mdxManager;

  // The metadata extractor manager SQL executor.
  private final MetadataExtractorManagerSql mdxManagerSql;

  // The database manager.
  private final MetadataDbManager dbManager;

  // The metadata manager.
  private final MetadataManager mdManager;

  // The metadata manager SQL executor.
  //private final MetadataManagerSql mdManagerSql;

  // The archival unit.
  private final ArchivalUnit au;

  // AU-related properties independent of the database.
  private final Plugin plugin;
  private final String platform;
  private final int pluginVersion;
  private final String auId;
  private final String auKey;
  private final String pluginId;
  private final boolean isBulkContent;

  // Database identifiers related to the AU. 
  private Long publisherSeq = null;
  private Long publicationSeq = null;
  private Long pluginSeq = null;
  private Long auSeq = null;
  private Long auMdSeq = null;
  private Long parentSeq = null;

  // Properties used to take shortcuts in processing.
  private String seriesTitle = null;
  private String proprietarySeriesId = null;
  private String publicationTitle = null;
  private String publicationType = null;
  private String pIsbn = null;
  private String eIsbn = null;
  private String pIssn = null;
  private String eIssn = null;
  private String proprietaryId = null;
  private String volume = null;
  private String parentMdItemType = null;
  private boolean newAu = false;
  private String publisherName;

  /**
   * Constructor.
   * 
   * @param task
   *          A ReindexingTaskwith the calling task.
   * @param mdxManager
   *          A MetadataManager with the metadata extractor manager.
   * @param au
   *          An ArchivalUnit with the archival unit.
   */
  public AuMetadataRecorder(ReindexingTask task,
      MetadataExtractorManager mdxManager, ArchivalUnit au) {
    this.task = task;
    this.mdxManager = mdxManager;
    mdxManagerSql = mdxManager.getMetadataExtractorManagerSql();
    dbManager = mdxManager.getDbManager();
    mdManager = mdxManager.getMetadataManager();
    this.au = au;

    plugin = au.getPlugin();
    isBulkContent = plugin.isBulkContent();
    platform = plugin.getPublishingPlatform();
    pluginVersion = mdxManager.getPluginMetadataVersionNumber(plugin);
    auId = au.getAuId();
    auKey = PluginManager.auKeyFromAuId(auId);
    pluginId = PluginManager.pluginIdFromAuId(auId);
  }

  /**
   * Constructor.
   * 
   * @param mdxManager
   *          A MetadataExtractorManager with the metadata manager.
   * @param plugin
   *          A Plugin with the Archival Unit plugin.
   * @param auId
   *          A String with the Archival Unit identifier.
   */
  public AuMetadataRecorder(MetadataExtractorManager mdxManager, Plugin plugin,
      String auId) {
    this.task = null;
    this.mdxManager = mdxManager;
    mdxManagerSql = mdxManager.getMetadataExtractorManagerSql();
    dbManager = mdxManager.getDbManager();
    mdManager = mdxManager.getMetadataManager();
    this.au = null;

    this.plugin = plugin;
    isBulkContent = plugin.isBulkContent();
    platform = plugin.getPublishingPlatform();
    pluginVersion = mdxManager.getPluginMetadataVersionNumber(plugin);
    this.auId = auId;
    auKey = PluginManager.auKeyFromAuId(auId);
    pluginId = PluginManager.pluginIdFromAuId(auId);
  }

  /**
   * Writes to the database metadata related to an archival unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mditr
   *          An Iterator<ArticleMetadataInfo> with the metadata.
   * @throws MetadataIndexingException
   *           if any problem is detected with the passed metadata or the task
   *           is cancelled.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void recordMetadata(Connection conn, Iterator<ArticleMetadataInfo> mditr)
      throws MetadataIndexingException, DbException {
    final String DEBUG_HEADER = "recordMetadata(): ";

    // Get the mandatory metadata fields.
    List<String> mandatoryFields = mdxManager.getMandatoryMetadataFields();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "mandatoryFields = " + mandatoryFields);

    // Loop through the metadata for each item.
    while (mditr.hasNext()) {
      if (task.isCancelled()) {
	throw new MetadataIndexingException("Reindexing task cancelled");
      }

      task.pokeWDog();

      recordMetadataItem(conn, mandatoryFields, mditr);

      log.debug3(DEBUG_HEADER + "updatedArticleCount = "
	  + task.getUpdatedArticleCount());
    }

    // Check whether the metadata should NOT be posted to a REST web service.
    if (CurrentConfig.getParam(PARAM_MD_REST_SERVICE_LOCATION) == null) {
      // Yes.
      if (auMdSeq != null) {
	// Update the AU last extraction timestamp.
	mdxManager.updateAuLastExtractionTime(au, conn, auMdSeq);
      } else {
	log.warning("auMdSeq is null for auid = '" + au.getAuId() + "'.");
      }

      // Find the list of previous problems indexing this Archival Unit.
      List<String> problems = findAuProblems(conn, auId);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "problems.size() = " + problems.size());

      // Check whether the publisher name used is a synthetic name.
      if (publisherName.startsWith(UNKNOWN_PUBLISHER_AU_PROBLEM)) {
	// Yes: Check whether an entry in the AU problem table does not exist.
	if (!problems.contains(publisherName)) {
	  // Yes: Add an unknown publisher entry to the AU problem table.
	  addAuProblem(conn, auId, publisherName);
	}
      } else {
	// No: Check whether there is data obtained when the publisher was
	// unknown that needs to be merged.
	if (problems.size() > 0) {
	  // Yes: Merge it.
	  fixUnknownPublishersAuData(conn, problems);
	}
      }
    }
  }

  /**
   * Writes to the database metadata related to an archival unit item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mandatoryFields
   *          A List<String> with the fields that are required for the item
   *          metadata to pass validation.
   * @param mditr
   *          An Iterator<ArticleMetadataInfo> with the metadata.
   * @return a Long with the database identifier of the metadata item.
   * @throws MetadataIndexingException
   *           if any problem is detected with the passed metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long recordMetadataItem(Connection conn, List<String> mandatoryFields,
      Iterator<ArticleMetadataInfo> mditr) throws MetadataIndexingException,
      DbException {
    final String DEBUG_HEADER = "recordMetadataItem(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "mandatoryFields = " + mandatoryFields);

    Long mdItemSeq = null;

    // Get the next metadata item.
    ArticleMetadataInfo mdInfo = mditr.next();

    // Validate all the metadata fields.
    validateMetadata(mdInfo, mandatoryFields);

    // Normalize all the metadata fields.
    ArticleMetadataInfo normalizedMdInfo = normalizeMetadata(mdInfo);

    // Check whether the metadata should be posted to a REST web service.
    if (CurrentConfig.getParam(PARAM_MD_REST_SERVICE_LOCATION) != null) {
      // Yes: Create the object to be posted.
      ItemMetadata item = normalizedMdInfo.populateItemMetadataDetail();

      // Add to it the Archival Unit identifier.
      item.getScalarMap().put("au_id", auId);

      // Post it.
      mdItemSeq = new StoreAuItemClient().storeAuItem(item);
    } else {
      // No: Store the metadata fields in the database.
      mdItemSeq = storeMetadata(conn, normalizedMdInfo);
    }

    if (log.isDebug2()) log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }

  /**
   * Validates the metadata fields.
   *
   * @param mdinfo
   *          An ArticleMetadataInfo with the metadata fields.
   * @param mandatoryFields
   *          A List<String> with the metadata fields that are mandatory.
   * @throws MetadatException
   *           if the validation fails.
   */
  void validateMetadata(ArticleMetadataInfo mdinfo,
      List<String> mandatoryFields) throws MetadataIndexingException {
    if (mandatoryFields == null || mandatoryFields.size() == 0) {
      return;
    }

    for (String mandatoryField : mandatoryFields) {
      switch (mandatoryField) {
      case "publisher":
	if (StringUtil.isNullString(mdinfo.publisher)) {
	  throw new MetadataIndexingException("Missing mandatory metadata field '"
	      + mandatoryField + "' in " + mdinfo.toString(), mdinfo);
	}
	break;

      case "provider":
	if (StringUtil.isNullString(mdinfo.provider)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "publicationTitle":
	if (StringUtil.isNullString(mdinfo.publicationTitle)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "publicationType":
	if (StringUtil.isNullString(mdinfo.publicationType)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "articleTitle":
	if (StringUtil.isNullString(mdinfo.articleTitle)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "articleType":
	if (StringUtil.isNullString(mdinfo.articleType)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "accessUrl":
	if (StringUtil.isNullString(mdinfo.accessUrl)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "isbn":
	if (StringUtil.isNullString(mdinfo.isbn)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "eisbn":
	if (StringUtil.isNullString(mdinfo.eisbn)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "issn":
	if (StringUtil.isNullString(mdinfo.issn)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "eissn":
	if (StringUtil.isNullString(mdinfo.eissn)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "doi":
	if (StringUtil.isNullString(mdinfo.doi)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "pubDate":
	if (StringUtil.isNullString(mdinfo.pubDate)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "pubYear":
	if (StringUtil.isNullString(mdinfo.pubYear)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "volume":
	if (StringUtil.isNullString(mdinfo.volume)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "issue":
	if (StringUtil.isNullString(mdinfo.issue)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "startPage":
	if (StringUtil.isNullString(mdinfo.startPage)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "endPage":
	if (StringUtil.isNullString(mdinfo.endPage)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "seriesTitle":
	if (StringUtil.isNullString(mdinfo.seriesTitle)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "authors":
	if (mdinfo.authors == null || mdinfo.authors.size() == 0) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "keywords":
	if (mdinfo.keywords == null || mdinfo.keywords.size() == 0) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "featuredUrlMap":
	if (mdinfo.featuredUrlMap == null
	|| mdinfo.featuredUrlMap.size() == 0) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "coverage":
	if (StringUtil.isNullString(mdinfo.coverage)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "itemNumber":
	if (StringUtil.isNullString(mdinfo.itemNumber)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "proprietaryIdentifier":
	if (StringUtil.isNullString(mdinfo.proprietaryIdentifier)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "proprietarySeriesIdentifier":
	if (StringUtil.isNullString(mdinfo.proprietarySeriesIdentifier)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      case "fetchTime":
	if (StringUtil.isNullString(mdinfo.fetchTime)) {
	  throw new MetadataIndexingException(
	      "Missing mandatory metadata field '" + mandatoryField + "' in "
		  + mdinfo.toString(), mdinfo);
	}
	break;

      default:
	log.warning("Ignoring unknown mandatory field '" + mandatoryField
	    + "'");
      }
    }
  }

  /**
   * Normalizes metadata info fields.
   * 
   * @param mdinfo
   *          the ArticleMetadataInfo
   * @return an ArticleMetadataInfo with the normalized properties.
   */
  ArticleMetadataInfo normalizeMetadata(ArticleMetadataInfo mdinfo) {
    if (mdinfo.accessUrl != null) {
      String accessUrl = mdinfo.accessUrl.trim();
      if (accessUrl.length() > MAX_URL_COLUMN) {
	log.warning("accessUrl too long '" + mdinfo.accessUrl
	    + "' for title: '" + mdinfo.publicationTitle + "' publisher: "
	    + mdinfo.publisher + "'");
	mdinfo.accessUrl =
	    MetadataDbManager.truncateVarchar(accessUrl, MAX_URL_COLUMN);
      } else {
	mdinfo.accessUrl = accessUrl;
      }
    }

    if (mdinfo.isbn != null) {
      // Strip punctuation and ensure values are proper ISBN lengths.
      mdinfo.isbn = MetadataUtil.toUnpunctuatedIsbn(mdinfo.isbn);

      if (mdinfo.isbn != null) {
	mdinfo.isbn = mdinfo.isbn.toUpperCase();
      }
    }

    if (mdinfo.eisbn != null) {
      // Strip punctuation and ensure values are proper ISBN lengths.
      mdinfo.eisbn = MetadataUtil.toUnpunctuatedIsbn(mdinfo.eisbn);

      if (mdinfo.eisbn != null) {
	mdinfo.eisbn = mdinfo.eisbn.toUpperCase();
      }
    }

    if (mdinfo.issn != null) {
      // Strip punctuation and ensure values are proper ISSN lengths.
      mdinfo.issn = MetadataUtil.toUnpunctuatedIssn(mdinfo.issn);

      if (mdinfo.issn != null) {
	mdinfo.issn = mdinfo.issn.toUpperCase();
      }
    }

    if (mdinfo.eissn != null) {
      // Strip punctuation and ensure values are proper ISSN lengths.
      mdinfo.eissn = MetadataUtil.toUnpunctuatedIssn(mdinfo.eissn);

      if (mdinfo.eissn != null) {
	mdinfo.eissn = mdinfo.eissn.toUpperCase();
      }
    }

    if (mdinfo.doi != null) {
      String doi = mdinfo.doi.trim();
      if (StringUtil.startsWithIgnoreCase(doi, "doi:")) {
	doi = doi.substring("doi:".length());
	log.debug3("doi = '" + doi + "'.");
      }

      if (doi.length() > MAX_DOI_COLUMN) {
	log.warning("doi too long '" + mdinfo.doi + "' for title: '"
	    + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	    + "'");
	mdinfo.doi = MetadataDbManager.truncateVarchar(doi.toLowerCase(),
	    MAX_DOI_COLUMN);
      } else {
	mdinfo.doi = doi.toLowerCase();
      }
    }

    if (mdinfo.pubDate != null) {
      String pubDate = mdinfo.pubDate.trim();
      if (pubDate.length() > MAX_DATE_COLUMN) {
	log.warning("pubDate too long '" + mdinfo.pubDate + "' for title: '"
	    + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	    + "'");
	mdinfo.pubDate =
	    MetadataDbManager.truncateVarchar(pubDate, MAX_DATE_COLUMN);
      } else {
	mdinfo.pubDate = pubDate;
      }
    }

    if (mdinfo.volume != null) {
      String volume = mdinfo.volume.trim();
      if (volume.length() > MAX_VOLUME_COLUMN) {
	log.warning("volume too long '" + mdinfo.volume + "' for title: '"
	    + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	    + "'");
	mdinfo.volume =
	    MetadataDbManager.truncateVarchar(volume, MAX_VOLUME_COLUMN);
      } else {
	mdinfo.volume = volume;
      }
    }

    if (mdinfo.issue != null) {
      String issue = mdinfo.issue.trim();
      if (issue.length() > MAX_ISSUE_COLUMN) {
	log.warning("issue too long '" + mdinfo.issue + "' for title: '"
	    + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	    + "'");
	mdinfo.issue =
	    MetadataDbManager.truncateVarchar(issue, MAX_ISSUE_COLUMN);
      } else {
	mdinfo.issue = issue;
      }
    }

    if (mdinfo.startPage != null) {
      String startPage = mdinfo.startPage.trim();
      if (startPage.length() > MAX_START_PAGE_COLUMN) {
	log.warning("startPage too long '" + mdinfo.startPage
	    + "' for title: '" + mdinfo.publicationTitle + "' publisher: "
	    + mdinfo.publisher + "'");
	mdinfo.startPage =
	    MetadataDbManager.truncateVarchar(startPage, MAX_START_PAGE_COLUMN);
      } else {
	mdinfo.startPage = startPage;
      }
    }

    if (mdinfo.articleTitle != null) {
      String name = mdinfo.articleTitle.trim();
      if (name.length() > MAX_NAME_COLUMN) {
	log.warning("article title too long '" + mdinfo.articleTitle
	    + "' for title: '" + mdinfo.publicationTitle + "' publisher: "
	    + mdinfo.publisher + "'");
	mdinfo.articleTitle =
	    MetadataDbManager.truncateVarchar(name, MAX_NAME_COLUMN);
      } else {
	mdinfo.articleTitle = name;
      }
    }

    if (mdinfo.publisher != null) {
      String name = mdinfo.publisher.trim();
      if (name.length() > MAX_NAME_COLUMN) {
	log.warning("publisher too long '" + mdinfo.publisher
	    + "' for title: '" + mdinfo.publicationTitle + "'");
	mdinfo.publisher =
	    MetadataDbManager.truncateVarchar(name, MAX_NAME_COLUMN);
      } else {
	mdinfo.publisher = name;
      }
    }

    if (mdinfo.provider != null) {
      String name = mdinfo.provider.trim();
      if (name.length() > MAX_NAME_COLUMN) {
	log.warning("provider too long '" + mdinfo.provider
	    + "' for title: '" + mdinfo.publicationTitle + "'");
	mdinfo.provider =
	    MetadataDbManager.truncateVarchar(name, MAX_NAME_COLUMN);
      } else {
	mdinfo.provider = name;
      }
    }

    if (mdinfo.seriesTitle != null) {
      String name = mdinfo.seriesTitle.trim();
      if (name.length() > MAX_NAME_COLUMN) {
        log.warning("series title too long '" + mdinfo.seriesTitle
            + "' for publisher: " + mdinfo.publisher + "'");
        mdinfo.seriesTitle =
            MetadataDbManager.truncateVarchar(name, MAX_NAME_COLUMN);
      } else {
        mdinfo.seriesTitle = name;
      }
    }

    if (mdinfo.publicationTitle != null) {
      String name = mdinfo.publicationTitle.trim();
      if (name.length() > MAX_NAME_COLUMN) {
	log.warning("journal title too long '" + mdinfo.publicationTitle
	    + "' for publisher: " + mdinfo.publisher + "'");
	mdinfo.publicationTitle =
	    MetadataDbManager.truncateVarchar(name, MAX_NAME_COLUMN);
      } else {
	mdinfo.publicationTitle = name;
      }
    }

    if (mdinfo.authors != null) {
      List<String> authors = new ArrayList<String>();
      for (String author : mdinfo.authors) {
	String name = author.trim();
	if (name.length() > MAX_AUTHOR_COLUMN) {
	  log.warning("author too long '" + author + "' for title: '"
	      + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	      + "'");
	  authors.add(MetadataDbManager.truncateVarchar(name,
	      MAX_AUTHOR_COLUMN));
	} else {
	  authors.add(name);
	}
      }
      mdinfo.authors = authors;
    }

    if (mdinfo.keywords != null) {
      List<String> keywords = new ArrayList<String>();
      for (String keyword : mdinfo.keywords) {
	String name = keyword.trim();
	if (name.length() > MAX_KEYWORD_COLUMN) {
	  log.warning("keyword too long '" + keyword + "' for title: '"
	      + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	      + "'");
	  keywords.add(MetadataDbManager.truncateVarchar(name,
	      MAX_KEYWORD_COLUMN));
	} else {
	  keywords.add(name);
	}
      }
      mdinfo.keywords = keywords;
    }

    if (mdinfo.featuredUrlMap != null) {
      Map<String, String> featuredUrls = new HashMap<String, String>();
      for (String feature : mdinfo.featuredUrlMap.keySet()) {
	String validFeature = feature;
	if (feature.length() > MAX_FEATURE_COLUMN) {
	  log.warning("feature too long '" + feature + "' for title: '"
	      + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	      + "'");
	  validFeature = MetadataDbManager.truncateVarchar(feature,
	      MAX_FEATURE_COLUMN);
	}

	String url = mdinfo.featuredUrlMap.get(feature).trim();
	if (url.length() > MAX_URL_COLUMN) {
	  log.warning("URL too long '" + mdinfo.featuredUrlMap.get(feature)
	      + "' for title: '" + mdinfo.publicationTitle + "' publisher: "
	      + mdinfo.publisher + "'");
	  featuredUrls.put(validFeature,
	      MetadataDbManager.truncateVarchar(url, MAX_URL_COLUMN));
	} else {
	  featuredUrls.put(validFeature, url);
	}
      }
      mdinfo.featuredUrlMap = featuredUrls;
    }

    if (mdinfo.endPage != null) {
      String endPage = mdinfo.endPage.trim();
      if (endPage.length() > MAX_END_PAGE_COLUMN) {
	log.warning("endPage too long '" + mdinfo.endPage + "' for title: '"
	    + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	    + "'");
	mdinfo.endPage =
	    MetadataDbManager.truncateVarchar(endPage, MAX_END_PAGE_COLUMN);
      } else {
	mdinfo.endPage = endPage;
      }
    }

    if (mdinfo.coverage != null) {
      String coverage = mdinfo.coverage.trim();
      if (coverage.length() > MAX_COVERAGE_COLUMN) {
	log.warning("coverage too long '" + mdinfo.coverage + "' for title: '"
	    + mdinfo.publicationTitle + "' publisher: " + mdinfo.publisher
	    + "'");
	mdinfo.coverage =
	    MetadataDbManager.truncateVarchar(coverage, MAX_COVERAGE_COLUMN);
      } else {
	mdinfo.coverage = coverage;
      }
    } else {
      mdinfo.coverage = "fulltext";
    }

    if (mdinfo.itemNumber != null) {
      String itemNumber = mdinfo.itemNumber.trim();
      if (itemNumber.length() > MAX_ITEM_NO_COLUMN) {
	log.warning("itemNumber too long '" + mdinfo.itemNumber
	    + "' for title: '" + mdinfo.publicationTitle + "' publisher: "
	    + mdinfo.publisher + "'");
	mdinfo.itemNumber = MetadataDbManager.truncateVarchar(mdinfo.itemNumber,
	    MAX_ITEM_NO_COLUMN);
      } else {
	mdinfo.itemNumber = itemNumber;
      }
    }

    if (mdinfo.proprietaryIdentifier != null) {
      String name = mdinfo.proprietaryIdentifier.trim();
      if (name.length() > MAX_PROPRIETARY_ID_COLUMN) {
	log.warning("proprietaryIdentifier too long '"
	    + mdinfo.proprietaryIdentifier + "' for title: '"
	    + mdinfo.publicationTitle + "' publisher: "
	    + mdinfo.publisher + "'");
	mdinfo.proprietaryIdentifier =
	    MetadataDbManager.truncateVarchar(name, MAX_PROPRIETARY_ID_COLUMN);
      } else {
	mdinfo.proprietaryIdentifier = name;
      }
    }

    if (mdinfo.proprietarySeriesIdentifier != null) {
      String name = mdinfo.proprietarySeriesIdentifier.trim();
      if (name.length() > MAX_PROPRIETARY_ID_COLUMN) {
        log.warning("proprietarySeriesIdentifier too long '"
            + mdinfo.proprietarySeriesIdentifier + "' for series title: '"
            + mdinfo.seriesTitle + "' publisher: " + mdinfo.publisher + "'");
        mdinfo.proprietarySeriesIdentifier =
            MetadataDbManager.truncateVarchar(name, MAX_PROPRIETARY_ID_COLUMN);
      } else {
        mdinfo.proprietarySeriesIdentifier = name;
      }
    }

    return mdinfo;
  }

  /**
   * Replace gensym metadata title with new title.
   *  
   * @param conn
   *          A Connection with the connection to the database
   * @param mdSequence
   *          The md_info record index.
   * @param unknownRoot
   *          The unknown root prefix
   * @param title
   *          The replacement title
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void replaceUnknownMdTitle(Connection conn, 
      Long mdSequence, String unknownRoot, String title) throws DbException {
    final String DEBUG_HEADER = "replaceGenSym(): ";

    // Find the publication names.
    Map<String, String> names = mdManager.getMdItemNames(conn, mdSequence);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "names.size() = " + names.size());

    // Loop through each publication name.
    for (Map.Entry<String, String> entry : names.entrySet()) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "entry = " + entry);

      // Check whether this is the primary name.
      if (entry.getValue().equals(PRIMARY_NAME_TYPE)) {
        // Yes: Check whether the name has been synthesized.
        if (title.startsWith(unknownRoot)) {
          // Yes: Check whether this is not a synthesized name.
          if (!entry.getKey().startsWith(unknownRoot)) {
            // Yes: Remove any synthesized names.
            removeNotPrimarySynthesizedMdItemNames(conn, mdSequence);

            // Use the primary name instead of the synthesized name.
            publicationTitle = entry.getKey();
          }
        } else {
          // No: Check whether this is a synthesized name.
          if (entry.getKey().startsWith(unknownRoot)) {
            // Yes: Update the synthesized primary name with the current one.
            updatePrimarySynthesizedMdItemName(conn, mdSequence, title);

            // Remove the previously entered non-primary name for this
            // publication.
            removeNotPrimaryMdItemName(conn, mdSequence, title);
          }
        }

        break;
      }
    }
  }

  /**
   * Updates the primary name of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the identifier of the metadata item.
   * @param primaryName
   *          A String with the primary name of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void updatePrimarySynthesizedMdItemName(Connection conn,
      Long mdItemSeq, String primaryName) throws DbException {
    final String DEBUG_HEADER = "updatePrimarySynthesizedMdItemName(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "primaryName = " + primaryName);
    }

    PreparedStatement updatePrimaryName =
	dbManager.prepareStatement(conn, UPDATE_MD_ITEM_PRIMARY_NAME_QUERY);

    try {
      updatePrimaryName.setString(1, primaryName);
      updatePrimaryName.setLong(2, mdItemSeq);
      int count = dbManager.executeUpdate(updatePrimaryName);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      log.error("Cannot update the primary name", sqle);
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      log.error("primaryName = '" + primaryName + "'.");
      log.error("SQL = '" + UPDATE_MD_ITEM_PRIMARY_NAME_QUERY + "'.");
      throw new DbException("Cannot update the primary name", sqle);
    } finally {
      MetadataDbManager.safeCloseStatement(updatePrimaryName);
    }
  }

  /**
   * Removes non-primary metadata item synthesized names from the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void removeNotPrimarySynthesizedMdItemNames(Connection conn,
      Long mdItemSeq) throws DbException {
    final String DEBUG_HEADER = "removeNotPrimarySynthesizedMdItemNames(): ";
    PreparedStatement deleteName =
	dbManager.prepareStatement(conn, DELETE_NOT_PRIMARY_MDITEM_NAMES_QUERY);

    try {
      deleteName.setLong(1, mdItemSeq);
      int count = dbManager.executeUpdate(deleteName);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      String message =
	  "Cannot remove metadata item non-primary synthesized names";
      log.error(message, sqle);
      log.error("mdItemSeq = " + mdItemSeq + ".");
      log.error("SQL = '" + DELETE_NOT_PRIMARY_MDITEM_NAMES_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      MetadataDbManager.safeCloseStatement(deleteName);
    }
  }

  /**
   * Removes a non-primary metadata item name from the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param name
   *          A String with the non-primary metadata item name to be removed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void removeNotPrimaryMdItemName(Connection conn, Long mdItemSeq,
      String name) throws DbException {
    final String DEBUG_HEADER = "removeNotPrimaryMdItemName(): ";
    PreparedStatement deleteName =
	dbManager.prepareStatement(conn, DELETE_NOT_PRIMARY_MDITEM_NAME_QUERY);

    try {
      deleteName.setLong(1, mdItemSeq);
      deleteName.setString(2, name);
      int count = dbManager.executeUpdate(deleteName);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      String message = "Cannot remove metadata item non-primary name";
      log.error(message, sqle);
      log.error("mdItemSeq = " + mdItemSeq + ".");
      log.error("name = '" + name + "'.");
      log.error("SQL = '" + DELETE_NOT_PRIMARY_MDITEM_NAME_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      MetadataDbManager.safeCloseStatement(deleteName);
    }
  }

  /**
   * Stores in the database metadata for the Archival Unit.
   * 
   * @param conn
   *          A Connection with the connection to the database
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata.
   * @return a Long with the database identifier of the metadata item.
   * @throws MetadataIndexingException
   *           if any problem is detected with the passed metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long storeMetadata(Connection conn, ArticleMetadataInfo mdinfo)
      throws MetadataIndexingException, DbException {
    final String DEBUG_HEADER = "storeMetadata(): ";
    if (log.isDebug3()) {
      log.debug3(DEBUG_HEADER + "Starting: auId = " + auId);
      log.debug3(DEBUG_HEADER + "auKey = " + auKey);
      log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
      log.debug3(DEBUG_HEADER + "mdinfo.articleTitle = " + mdinfo.articleTitle);
    }

    // Check whether this is a new publisher.
    if (publisherSeq == null || !isSamePublisher(mdinfo)) {
      // Yes.
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "is new publisher.");

      // Find or create the publisher.
      findOrCreatePublisher(conn, mdinfo);

      // It cannot be the same publication as the previous one if it has a
      // different publisher.
      publicationSeq = null;
    }
    
    // Check whether this is a new publication.
    if (publicationSeq == null || !isSamePublication(mdinfo)) {
      // Yes.
      log.debug3(DEBUG_HEADER + "is new publication.");

      // Get the publication type in the metadata
      publicationType = mdinfo.publicationType;
      log.debug3(DEBUG_HEADER + "publicationType = " + publicationType);

      // Validate the types of the metadata item and its parent.
      if (!validateMdItemTypeHierarchy(publicationType, mdinfo.articleType)) {
	throw new MetadataIndexingException("Mismatch between articleType '"
	    + mdinfo.articleType + "' and publicationType '" + publicationType
	    + "'", mdinfo);
      }

      // Get the journal title received in the metadata.
      publicationTitle = mdinfo.publicationTitle;
      log.debug3(DEBUG_HEADER + "publicationTitle = " + publicationTitle);

      // Check whether no name was received in the metadata.
      if (StringUtil.isNullString(publicationTitle)) {
	// Yes: Synthesize a name.
        String defaultId = Long.toString(TimeBase.nowMs());
	publicationTitle = synthesizePublicationTitle(mdinfo, defaultId);
      }
      
      // Check whether it is a book series.
      if (MetadataField.PUBLICATION_TYPE_BOOKSERIES.equals(publicationType)) {
        // Yes: Get the series title received in the metadata
        seriesTitle = mdinfo.seriesTitle;
        log.debug3(DEBUG_HEADER + "seriesTitle = " + seriesTitle);

        if (StringUtil.isNullString(seriesTitle)) {
          // Yes: Synthesize a name.
          seriesTitle = synthesizeSeriesTitle(mdinfo, publicationTitle);
        }

        // Get the proprietary series identifier received in the metadata.
        proprietarySeriesId = mdinfo.proprietarySeriesIdentifier;
        log.debug3(DEBUG_HEADER
            + "proprietarySeriesId = " + proprietarySeriesId);
      } else {
	// No.
	seriesTitle = null;
	proprietarySeriesId = null;
      }
      
      // Get any ISBN values received in the metadata.
      pIsbn = mdinfo.isbn;
      log.debug3(DEBUG_HEADER + "pIsbn = " + pIsbn);

      eIsbn = mdinfo.eisbn;
      log.debug3(DEBUG_HEADER + "eIsbn = " + eIsbn);

      // Get any ISSN values received in the metadata.
      pIssn = mdinfo.issn;
      log.debug3(DEBUG_HEADER + "pIssn = " + pIssn);

      eIssn = mdinfo.eissn;
      log.debug3(DEBUG_HEADER + "eIssn = " + eIssn);

      // Get the proprietary identifier received in the metadata.
      proprietaryId = mdinfo.proprietaryIdentifier;
      log.debug3(DEBUG_HEADER + "proprietaryId = " + proprietaryId);

      // Get the volume received in the metadata.
      volume = mdinfo.volume;
      log.debug3(DEBUG_HEADER + "volume = " + volume);
      
      // Get the publication to which this metadata belongs.
      publicationSeq = mdManager.findOrCreatePublication(conn, publisherSeq, 
          pIssn, eIssn, pIsbn, eIsbn, publicationType, 
	  seriesTitle, proprietarySeriesId, publicationTitle, proprietaryId);
      log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

      // Get the identifier of the parent, which is the publication metadata
      // item.
      parentSeq = mdManager.findPublicationMetadataItem(conn, publicationSeq);
      log.debug3(DEBUG_HEADER + "parentSeq = " + parentSeq);

      // replace any unknown titles with this publication title
      replaceUnknownMdTitle(
          conn, parentSeq,UNKNOWN_TITLE_NAME_ROOT, publicationTitle); 

      // Get the type of the parent.
      parentMdItemType = getMdItemTypeName(conn, parentSeq);
      log.debug3(DEBUG_HEADER + "parentMdItemType = " + parentMdItemType);

      // Skip it if the publication could not be found or created.
      if (publicationSeq == null || parentSeq == null ||
  	parentMdItemType == null) {
        if (log.isDebug3()) log.debug3(DEBUG_HEADER
  	  + "Done: publicationSeq or parentSeq or parentMdItemType is null.");
        return null;
      }

      // Replace any unknown series titles with this series title
      if (MetadataField.PUBLICATION_TYPE_BOOKSERIES.equals(publicationType)
          && !StringUtil.isNullString(seriesTitle)) {
        Long seriesPublicationSeq = mdManager.findBookSeries(conn, publisherSeq,
            pIssn, eIssn, seriesTitle);
        log.debug3(DEBUG_HEADER 
            + "seriesPublicationSeq = " + seriesPublicationSeq);
        if (seriesPublicationSeq != null) {
          Long seriesSeq = mdManager.findPublicationMetadataItem(conn,
              seriesPublicationSeq);
          log.debug3(DEBUG_HEADER + "seriesMdSeq = " + seriesSeq);
          if (seriesSeq != null) {
            replaceUnknownMdTitle(
                conn, seriesSeq, UNKNOWN_SERIES_NAME_ROOT, seriesTitle);
          }
        }
      }
    }

    // Populate in the database any necessary Archival Unit dependencies.
    populateAuDbDependencies(conn);

    // Check whether the Archival Unit metadata has not been located in the
    // database.
    if (auMdSeq == null) {
      // Yes: Find the Archival Unit metadata in the database.
      auMdSeq = findAuMd(conn, auSeq);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "old auMdSeq = " + auMdSeq);
    }

    // Check whether it is a new Archival Unit metadata.
    if (auMdSeq == null) {
      // Get the name of the provider received in the metadata.
      String providerName = mdinfo.provider;
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "providerName = " + providerName);

      // Check whether no provider name was received in the metadata.
      if (StringUtil.isNullString(providerName)) {
	// Yes: Use the publisher name.
	providerName = publisherName;
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "providerName = " + providerName);
      }

      // TODO: Replace the second argument with mdinfo.providerLid when
      // available.
      addAuMd(conn, null, providerName);

      newAu = true;
    } else {
      // No: Update the Archival Unit metadata ancillary data.
      updateAuMd(conn, auMdSeq, pluginVersion);
      log.debug3(DEBUG_HEADER + "updated AU.");

      // Update any unknown provider.
      updateAuUnknownProvider(conn, auMdSeq, mdinfo);
    }

    // Replace or create the metadata item.
    Long mdItemSeq = replaceOrCreateMdItem(conn, mdinfo);

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }

  /**
   * Finds the publisher in the database or creates a new one.
   * 
   * @param conn
   *          A Connection with the connection to the database
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata.
   * @throws MetadataIndexingException
   *           if any problem is detected with the passed metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void findOrCreatePublisher(Connection conn,
      ArticleMetadataInfo mdinfo) throws MetadataIndexingException, DbException {
    final String DEBUG_HEADER = "findOrCreatePublisher(): ";

    // Get the publisher received in the metadata.
    publisherName = mdinfo.publisher;
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);

    // Check whether the publisher is in the metadata.
    if (publisherName != null) {
      // Yes: Find the publisher or create it.
      publisherSeq = dbManager.findOrCreatePublisher(conn, publisherName);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
    } else {
      // No: Find the AU in the database.
      auSeq = mdxManagerSql.findAuByAuId(conn, auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

      // Check whether the AU was found.
      if (auSeq != null) {
	// Yes: Get the publisher of the AU.
	publisherSeq = mdxManager.findAuPublisher(conn, auSeq);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publisherSeq = " + publisherSeq);

	// Check whether the AU publisher was found.
	if (publisherSeq != null) {
	  // Yes: Get its name.
	  publisherName = getPublisherName(conn, publisherSeq);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);
	} else {
	  // No: Report the problem.
	  log.error("Null publisherSeq for auSeq = " + auSeq);
	  log.error("auId = " + auId);
	  log.error("auKey = " + auKey);
	  log.error("auMdSeq = " + auMdSeq);
	  log.error("auSeq = " + auSeq);
	  throw new MetadataIndexingException("Null publisherSeq for auSeq = " + auSeq,
	      mdinfo);
	}
      } else {
	// No: Loop through all outstanding previous problems for this AU.
	for (String problem : findAuProblems(conn, auId)) {
	  // Check whether there is an unknown publisher already for this AU.
	  if (problem.startsWith(UNKNOWN_PUBLISHER_AU_PROBLEM)) {
	    // Yes: Get the corresponding publisher identifier.
	    publisherSeq = dbManager.findPublisher(conn, problem);
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "publisherSeq = " + publisherSeq);

	    // Check whether the publisher exists.
	    if (publisherSeq != null) {
	      // Yes: Use it.
	      publisherName = problem;
	      break;
	    } else {
	      // No: Remove the obsolete problem.
	      removeAuProblem(conn, auId, problem);
	    }
	  }
	}

	// Check whether no previous unknown publisher for this AU exists.
	if (publisherName == null) {
	  // Yes: Create a synthetic publisher name to be able to process the
	  // Archival Unit.
	  publisherName = UNKNOWN_PUBLISHER_AU_PROBLEM + TimeBase.nowMs();
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);

	  // Create the publisher.
	  publisherSeq = dbManager.addPublisher(conn, publisherName);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides an indication of whether there is a match between the types of a
   * parent metadata item and a child metadata item.
   * 
   * @param parentType
   *          A String with the type of the parent metadata item.
   * @param childType
   *          A String with the type of the child metadata item.
   * @return <code>true</code> if there is a match between the types,
   *         <code>false</code> otherwise.
   */
  boolean validateMdItemTypeHierarchy(String parentType, String childType) {
    if (MetadataField.ARTICLE_TYPE_JOURNALARTICLE.equals(childType)) {
      return MetadataField.PUBLICATION_TYPE_JOURNAL.equals(parentType);
    } else if (MetadataField.ARTICLE_TYPE_BOOKCHAPTER.equals(childType)) {
      return MetadataField.PUBLICATION_TYPE_BOOK.equals(parentType)
	  || MetadataField.PUBLICATION_TYPE_BOOKSERIES.equals(parentType);
    } else if (MetadataField.ARTICLE_TYPE_BOOKVOLUME.equals(childType)) {
      return MetadataField.PUBLICATION_TYPE_BOOK.equals(parentType)
	  || MetadataField.PUBLICATION_TYPE_BOOKSERIES.equals(parentType);
    } else if (MetadataField.ARTICLE_TYPE_PROCEEDINGSARTICLE.
	equals(childType)) {
      return MetadataField.PUBLICATION_TYPE_PROCEEDINGS.equals(parentType);
    }
    
    return false;
  }

  /**
   * Creates a synthetic publication title using the available metadata.
   * 
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata.
   * @param defaultId
   *          A default id for the publication title
   * @return a String with the synthetic publication title.
   */
  private String synthesizePublicationTitle(
      ArticleMetadataInfo mdinfo, String defaultId) {
    final String DEBUG_HEADER = "synthesizePublicationTitle(): ";
    String result = null;

    // Check whether the metadata included the ISBN.
    if (!StringUtil.isNullString(mdinfo.isbn)) {
      // Yes: Use it.
      result = UNKNOWN_TITLE_NAME_ROOT + "/isbn=" + mdinfo.isbn;
      // No: Check whether the metadata included the eISBN.
    } else if (!StringUtil.isNullString(mdinfo.eisbn)) {
      // Yes: Use it.
      result = UNKNOWN_TITLE_NAME_ROOT + "/eisbn=" + mdinfo.eisbn;
      // No: Check whether the metadata included the ISSN.
    } else if (!StringUtil.isNullString(mdinfo.issn)) {
      // Yes: Use it.
      result = UNKNOWN_TITLE_NAME_ROOT + "/issn=" + mdinfo.issn;
      // No: Check whether the metadata included the eISSN.
    } else if (!StringUtil.isNullString(mdinfo.eissn)) {
      // Yes: Use it.
      result = UNKNOWN_TITLE_NAME_ROOT + "/eissn=" + mdinfo.eissn;
      // No: Check whether the metadata included the proprietary identifier.
    } else if (!StringUtil.isNullString(mdinfo.proprietaryIdentifier)) {
      // Yes: Use it.
      result = UNKNOWN_TITLE_NAME_ROOT + "/journalId="
	  + mdinfo.proprietaryIdentifier;
    } else {
      // No: Generate a random name.
      result = UNKNOWN_TITLE_NAME_ROOT + "/id=" +  defaultId;
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Creates a synthetic book seriesn title using the available metadata.
   * 
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata.
   * @param defaultId
   *          A default id for generating the series title
   * @return a String with the synthetic series title.
   */
  private String synthesizeSeriesTitle(
      ArticleMetadataInfo mdinfo, String defaultId) {
    final String DEBUG_HEADER = "synthesizeSeriesTitle(): ";
    String result = null;

    if (!StringUtil.isNullString(mdinfo.issn)) {
      // Yes: Use it.
      result = UNKNOWN_SERIES_NAME_ROOT + "/issn=" + mdinfo.issn;
      // No: Check whether the metadata included the eISSN.
    } else if (!StringUtil.isNullString(mdinfo.eissn)) {
      // Yes: Use it.
      result = UNKNOWN_SERIES_NAME_ROOT + "/eissn=" + mdinfo.eissn;
      // No: Check whether the metadata included the proprietary identifier.
    } else if (!StringUtil.isNullString(mdinfo.proprietarySeriesIdentifier)) {
      // Yes: Use it.
      result = UNKNOWN_SERIES_NAME_ROOT + "/seriesId="
          + mdinfo.proprietarySeriesIdentifier;
    } else {
      // No: Generate a random name.
      result = UNKNOWN_SERIES_NAME_ROOT + "/id=" + defaultId;
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Updates the metadata version an Archival Unit in the database.
   * 
   * @param conn
   *          A Connection with the connection to the database
   * @param auMdSeq
   *          A Long with the identifier of the archival unit metadata.
   * @param version
   *          A String with the archival unit metadata version.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void updateAuMd(Connection conn, Long auMdSeq, int version)
      throws DbException {
    final String DEBUG_HEADER = "updateAuMd(): ";
    try {
      PreparedStatement updateAu =
	  dbManager.prepareStatement(conn, UPDATE_AU_MD_VERSION_QUERY);

      try {
	updateAu.setShort(1, (short) version);
	updateAu.setLong(2, auMdSeq);
	int count = dbManager.executeUpdate(updateAu);

	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "count = " + count);
	  log.debug3(DEBUG_HEADER + "Updated auMdSeq = " + auMdSeq);
	}
      } finally {
	updateAu.close();
      }
    } catch (SQLException sqle) {
      throw new DbException("Cannot update AU metadata version", sqle);
    }
  }

  /**
   * Replaces a metadata item if it exists in the database, otherwise it creates
   * it.
   * 
   * @param conn
   *          A Connection with the connection to the database
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata.
   * @return a Long with the database identifier of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long replaceOrCreateMdItem(Connection conn,
      ArticleMetadataInfo mdinfo) throws DbException {
    final String DEBUG_HEADER = "replaceOrCreateMdItem(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    boolean addedNewItem = false;

    // Get the publication date received in the metadata.
    String date = mdinfo.pubDate;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "date = " + date);

    // Get the issue received in the metadata.
    String issue = mdinfo.issue;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issue = " + issue);

    // Get the start page received in the metadata.
    String startPage = mdinfo.startPage;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "startPage = " + startPage);

    // Get the end page received in the metadata.
    String endPage = mdinfo.endPage;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "endPage = " + endPage);

    // Get the item number received in the metadata.
    String itemNo = mdinfo.itemNumber;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "itemNo = " + itemNo);

    // Get the item title received in the metadata.
    String itemTitle = mdinfo.articleTitle;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "itemTitle = " + itemTitle);

    // Get the coverage received in the metadata.
    String coverage = mdinfo.coverage;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "coverage = " + coverage);

    // Get the DOI received in the metadata.
    String doi = mdinfo.doi;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "doi = " + doi);

    // Get the featured URLs received in the metadata.
    Map<String, String> featuredUrlMap = mdinfo.featuredUrlMap;

    if (log.isDebug3()) {
      for (String feature : featuredUrlMap.keySet()) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "feature = " + feature
	    + ", URL = " + featuredUrlMap.get(feature));
      }
    }

    // Get the earliest fetch time of the metadata items URLs.
    long fetchTime = -1;

    try {
      fetchTime = Long.valueOf(mdinfo.fetchTime).longValue();
    } catch (NumberFormatException nfe) {
      if (log.isDebug())
	log.debug("Unparsable fetch time '" + mdinfo.fetchTime + "'");
    }

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "fetchTime = " + fetchTime);

    // Get the access URL received in the metadata.
    String accessUrl = mdinfo.accessUrl;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "accessUrl = " + accessUrl);

    // Determine what type of a metadata item it is.
    String mdItemType = mdinfo.articleType;
    if (StringUtil.isNullString(mdItemType)) {
      // Skip it if the parent type is not a book or journal.
      log.error(DEBUG_HEADER + "Unknown mdItemType = " + mdItemType);
      return null;
    }
    
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "mdItemType = " + mdItemType);

    // Find the metadata item type record sequence.
    Long mdItemTypeSeq = mdManager.findMetadataItemType(conn, mdItemType);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "mdItemTypeSeq = " + mdItemTypeSeq);

    // sanity check -- type should be known in database
    if (mdItemTypeSeq == null) {
      log.error(DEBUG_HEADER + "Unknown articleType = " + mdItemType);
      return null;
    }
    
    Long mdItemSeq = null;

    // Check whether it is a metadata item for an existing Archival Unit.
    if (!newAu) {
      // Yes: Find the metadata item in the database.
      mdItemSeq = findMdItem(conn, mdItemTypeSeq, auMdSeq, accessUrl);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Check whether it is an existing metadata item.
      if (mdItemSeq != null) {
	// Yes: Delete it.
	int deletedCount =
	    mdxManagerSql.removeAuChildMetadataItem(conn, auMdSeq, mdItemSeq);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "deletedCount = " + deletedCount);
      } else {
	// No: Remember that the item was added to the database.
	addedNewItem = true;
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "New item: addedNewItem = " + addedNewItem);
      }
    } else {
      // No: Remember that the item was added to the database.
      addedNewItem = true;
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "New AU: addedNewItem = " + addedNewItem);
    }

    // Create the new metadata item in the database.
    mdItemSeq = mdManager.addMdItem(conn, parentSeq, mdItemTypeSeq, auMdSeq,
	  date, coverage, fetchTime);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "new mdItemSeq = " + mdItemSeq);

    mdManager.addMdItemName(conn, mdItemSeq, itemTitle, PRIMARY_NAME_TYPE);

    // Get the volume received in the metadata.
    String volume = mdinfo.volume;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "volume = " + volume);

    // Add the bibliographic data.
    int addedCount = dbManager.addBibItem(conn, mdItemSeq, volume, issue,
	startPage, endPage, itemNo);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "addedCount = " + addedCount);

    // Add the item URLs.
    mdxManager.addMdItemUrls(conn, mdItemSeq, accessUrl, featuredUrlMap);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "added AUItem URL.");

    // Get the authors received in the metadata.
    Collection<String> authors = mdinfo.authors;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "authors = " + authors);

    // Add the item authors.
    mdxManagerSql.addMdItemAuthors(conn, mdItemSeq, authors);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "added AUItem authors.");

    // Get the keywords received in the metadata.
    Collection<String> keywords = mdinfo.keywords;
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "keywords = " + keywords);

    // Add the item keywords.
    mdxManagerSql.addMdItemKeywords(conn, mdItemSeq, keywords);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "added AUItem keywords.");

    // Add the item DOI.
    mdManager.addMdItemDoi(conn, mdItemSeq, doi);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "added AUItem DOI.");

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "addedNewItem = " + addedNewItem);
    // Check whether the item was added.
    if (addedNewItem) {
      // Yes: Count the added article.
      if (task != null) {
	task.incrementUpdatedArticleCount();
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }

  /**
   * Provides the name of the type of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the identifier of the metadata item.
   * @return a String with the name of the type of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String getMdItemTypeName(Connection conn, Long mdItemSeq)
      throws DbException {
    final String DEBUG_HEADER = "getMdItemTypeName(): ";
    String typeName = null;

    try {
      PreparedStatement getMdItemTypeName = dbManager.prepareStatement(conn,
	  GET_MD_ITEM_TYPE_NAME_QUERY);
      ResultSet resultSet = null;

      try {
	getMdItemTypeName.setLong(1, mdItemSeq);
	resultSet = dbManager.executeQuery(getMdItemTypeName);

	if (resultSet.next()) {
	  typeName = resultSet.getString(TYPE_NAME_COLUMN);
	  log.debug3(DEBUG_HEADER + "typeName = " + typeName);
	}
      } finally {
	MetadataDbManager.safeCloseResultSet(resultSet);
	getMdItemTypeName.close();
      }
    } catch (SQLException sqle) {
      throw new DbException("Cannot get a metadata item type name", sqle);
    }

    return typeName;
  }

  /**
   * Provides the identifier of an Archival Unit metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the identifier of the Archival Unit.
   * @return a Long with the identifier of the Archival Unit metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findAuMd(Connection conn, Long auSeq) throws DbException {
    final String DEBUG_HEADER = "findAuMd(): ";
    Long auMdSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findAuMd = dbManager.prepareStatement(conn,
	FIND_AU_MD_QUERY);

    try {
      findAuMd.setLong(1, auSeq);

      resultSet = dbManager.executeQuery(findAuMd);
      if (resultSet.next()) {
	auMdSeq = resultSet.getLong(AU_MD_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      throw new DbException("Cannot find AU metadata", sqle);
    } finally {
      MetadataDbManager.safeCloseResultSet(resultSet);
      MetadataDbManager.safeCloseStatement(findAuMd);
    }

    log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
    return auMdSeq;
  }

  /**
   * Provides the identifier of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemTypeSeq
   *          A Long with the identifier of the metadata item type.
   * @param auMdSeq
   *          A Long with the identifier of the archival unit metadata.
   * @param accessUrl
   *          A String with the access URL of the metadata item.
   * @return a Long with the identifier of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findMdItem(Connection conn, Long mdItemTypeSeq, Long auMdSeq,
      String accessUrl) throws DbException {
    final String DEBUG_HEADER = "findMdItem(): ";
    Long mdItemSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findMdItem =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_QUERY);

    try {
      findMdItem.setLong(1, mdItemTypeSeq);
      findMdItem.setLong(2, auMdSeq);
      findMdItem.setString(3, accessUrl);

      resultSet = dbManager.executeQuery(findMdItem);
      if (resultSet.next()) {
	mdItemSeq = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      throw new DbException("Cannot find metadata item", sqle);
    } finally {
      MetadataDbManager.safeCloseResultSet(resultSet);
      MetadataDbManager.safeCloseStatement(findMdItem);
    }

    log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }

  /**
   * Provides an indication of whether the previous publisher is the same as the
   * current publisher.
   * 
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata of the current
   *          publication.
   * @return <code>true</code> if the previous publisher is the same as the
   *         current publisher, <code>false</code> otherwise.
   */
  private boolean isSamePublisher(ArticleMetadataInfo mdinfo) {
    return isSameProperty(publisherName, mdinfo.publisher);
  }
  
  /**
   * Provides an indication of whether the previous publication is the same as
   * the current publication.
   * 
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata of the current
   *          publication.
   * @return <code>true</code> if the previous publication is the same as the
   *         current publication, <code>false</code> otherwise.
   */
  private boolean isSamePublication(ArticleMetadataInfo mdinfo) {
    return isSameProperty(publicationTitle, mdinfo.publicationTitle) &&
	isSameProperty(publicationType, mdinfo.publicationType) &&
	isSameProperty(pIsbn, mdinfo.isbn) &&
	isSameProperty(eIsbn, mdinfo.eisbn) &&
	isSameProperty(pIssn, mdinfo.issn) &&
	isSameProperty(eIssn, mdinfo.eissn) &&
	isSameProperty(proprietaryId, mdinfo.proprietaryIdentifier) &&
	isSameProperty(volume, mdinfo.volume) &&
	isSameProperty(seriesTitle, mdinfo.seriesTitle) &&
	isSameProperty(proprietarySeriesId, mdinfo.proprietarySeriesIdentifier);
  }

  /**
   * Provides an indication of whether the previous property is the same as the
   * current property.
   * 
   * @param previous
   *          A String with the previous property.
   * @param current
   *          A String with the current property.
   * @return <code>true</code> if the previous property is the same as the
   *         current property, <code>false</code> otherwise.
   */
  private boolean isSameProperty(String previous, String current) {
    if (!StringUtil.isNullString(previous)) {
      return !StringUtil.isNullString(current) && previous.equals(current);
    }

    return StringUtil.isNullString(current);
  }

  /**
   * Provides the name of a publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the identifier of the publisher.
   * @return a String with the name of the publisher.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String getPublisherName(Connection conn, Long publisherSeq)
      throws DbException {
    final String DEBUG_HEADER = "getPublisherName(): ";
    String publisherName = null;
    ResultSet resultSet = null;

    PreparedStatement getPublisherNameStatement =
	dbManager.prepareStatement(conn, GET_PUBLISHER_NAME_QUERY);

    try {
      getPublisherNameStatement.setLong(1, publisherSeq);

      resultSet = dbManager.executeQuery(getPublisherNameStatement);
      if (resultSet.next()) {
	publisherName = resultSet.getString(PUBLISHER_NAME_COLUMN);
      }
    } catch (SQLException sqle) {
      log.error("Cannot get the name of a publisher", sqle);
      log.error("publisherSeq = '" + publisherSeq + "'.");
      log.error("SQL = '" + GET_PUBLISHER_NAME_QUERY + "'.");
      throw new DbException("Cannot get the name of a publisher", sqle);
    } finally {
      MetadataDbManager.safeCloseResultSet(resultSet);
      MetadataDbManager.safeCloseStatement(getPublisherNameStatement);
    }

    log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);
    return publisherName;
  }

  /**
   * Provides the problems found indexing an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a List<String> with the problems found indexing the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private List<String> findAuProblems(Connection conn, String auId)
      throws DbException {
    final String DEBUG_HEADER = "findAuProblems(): ";
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

    List<String> problems = new ArrayList<String>();
    ResultSet results = null;
    String problem;

    PreparedStatement findProblems =
	dbManager.prepareStatement(conn, FIND_AU_PROBLEMS_QUERY);

    try {
      String pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
      String auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      findProblems.setString(1, pluginId);
      findProblems.setString(2, auKey);
      results = dbManager.executeQuery(findProblems);

      while (results.next()) {
	problem = results.getString(PROBLEM_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "problem = " + problem);

	problems.add(problem);
      }
    } catch (SQLException sqle) {
      log.error("Cannot find AU problems", sqle);
      log.error("auId = '" + auId + "'.");
      log.error("SQL = '" + FIND_AU_PROBLEMS_QUERY + "'.");
      throw new DbException("Cannot find AU problems", sqle);
    } finally {
      MetadataDbManager.safeCloseResultSet(results);
      MetadataDbManager.safeCloseStatement(findProblems);
    }

    return problems;
  }

  /**
   * Removes an entry from the table of AU problems.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archiva lUnit identifier.
   * @param problem
   *          A String with the problem.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void removeAuProblem(Connection conn, String auId, String problem)
      throws DbException {
    final String DEBUG_HEADER = "removeAuProblem(): ";
    PreparedStatement deleteAuProblem =
	dbManager.prepareStatement(conn, DELETE_AU_PROBLEM_QUERY);

    try {
      String pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
      String auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      deleteAuProblem.setString(1, pluginId);
      deleteAuProblem.setString(2, auKey);
      deleteAuProblem.setString(3, problem);
      int count = dbManager.executeUpdate(deleteAuProblem);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      log.error("Cannot remove problem AU entry", sqle);
      log.error("auId = '" + auId + "'.");
      log.error("problem = '" + problem + "'.");
      log.error("SQL = '" + DELETE_AU_PROBLEM_QUERY + "'.");
      throw new DbException("Cannot remove problem AU entry", sqle);
    } finally {
      MetadataDbManager.safeCloseStatement(deleteAuProblem);
    }
  }

  /**
   * Adds an entry to the table of AU problems.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @param problem
   *          A String with the problem.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void addAuProblem(Connection conn, String auId, String problem)
      throws DbException {
    final String DEBUG_HEADER = "addAuProblem(): ";
    PreparedStatement addAuProblemStatement =
	dbManager.prepareStatement(conn, INSERT_AU_PROBLEM_QUERY);

    try {
      String pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
      String auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      addAuProblemStatement.setString(1, pluginId);
      addAuProblemStatement.setString(2, auKey);
      addAuProblemStatement.setString(3, problem);
      int count = dbManager.executeUpdate(addAuProblemStatement);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      log.error("Cannot add problem AU entry", sqle);
      log.error("auId = '" + auId + "'.");
      log.error("problem = '" + problem + "'.");
      log.error("SQL = '" + INSERT_AU_PROBLEM_QUERY + "'.");
      throw new DbException("Cannot add problem AU entry", sqle);
    } finally {
      MetadataDbManager.safeCloseStatement(addAuProblemStatement);
    }
  }

  /**
   * Fixes the Archival Unit data of unknown publishers.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param problems
   *          A List<String> with the recorded problems for the Archival Unit.
   * @throws MetadataIndexingException
   *           if the reindexing task is cancelled.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void fixUnknownPublishersAuData(Connection conn,
      List<String> problems) throws MetadataIndexingException, DbException {
    final String DEBUG_HEADER = "fixUnknownPublishersAuData(): ";
    log.debug3(DEBUG_HEADER + "Starting...");

    // Loop through all the problems.
    for (String problem : problems) {
      if (task.isCancelled()) {
	throw new MetadataIndexingException("Reindexing task cancelled");
      }

      // Consider only problems created by an unknown publisher.
      if (problem.startsWith(UNKNOWN_PUBLISHER_AU_PROBLEM)) {
	log.debug3(DEBUG_HEADER + "Need to migrate data under publisher '"
	    + problem + "' to publisher '" + publisherName + "'.");
	fixUnknownPublisherAuData(conn, problem);
      }
    }

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Fixes the Archival Unit data of an unknown publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param unknownPublisherName
   *          A String with the name of the unknown publisher.
   * @throws MetadataIndexingException
   *           if the reindexing task is cancelled.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void fixUnknownPublisherAuData(Connection conn,
      String unknownPublisherName)
	  throws MetadataIndexingException, DbException {
    final String DEBUG_HEADER = "fixUnknownPublisherAuData(): ";
    log.debug3(DEBUG_HEADER + "unknownPublisherName = " + unknownPublisherName);

    // Get the identifier of the unknown publisher.
    Long unknownPublisherSeq =
	dbManager.findPublisher(conn, unknownPublisherName);
    log.debug3(DEBUG_HEADER + "unknownPublisherSeq = " + unknownPublisherSeq);

    // Check whether the unknown publisher is not the current one.
    if (unknownPublisherSeq != null && unknownPublisherSeq != publisherSeq) {
      // Yes: Get the identifiers of any publications of the unknown publisher.
      Set<Long> unknownPublicationSeqs =
	  findPublisherPublications(conn, unknownPublisherSeq);

      // Get the identifiers of the metadata items of the current publication.
      Set<Long> mdItemSeqs =
	  findPublicationChildMetadataItems(conn, publicationSeq);

      Map<String, Long> mdItemMapByName = new HashMap<String, Long>();

      // Loop through all the identifiers of the metadata items of the current
      // publication.
      for (Long mdItemSeq : mdItemSeqs) {
	if (task.isCancelled()) {
	  throw new MetadataIndexingException("Reindexing task cancelled");
	}

	// Get allthe names of this metadata item.
	Map<String, String> mdItemSeqNames =
	    mdManager.getMdItemNames(conn, mdItemSeq);

	// Map the identifier by each of its names.
	for (String mdItemSeqName : mdItemSeqNames.keySet()) {
	  mdItemMapByName.put(mdItemSeqName, mdItemSeq);
	}
      }

      // Loop though all the identifiers of any publications of the unknown
      // publisher.
      for (Long unknownPublicationSeq : unknownPublicationSeqs) {
	if (task.isCancelled()) {
	  throw new MetadataIndexingException("Reindexing task cancelled");
	}

	log.debug3(DEBUG_HEADER + "unknownPublicationSeq = "
	    + unknownPublicationSeq);

	// Ignore the publication if it is the current one.
	if (unknownPublicationSeq != publicationSeq) {
	  // Fix the metadata of the publication of the unknown publisher.
	  fixUnknownPublisherPublicationMetadata(conn, unknownPublicationSeq,
	      mdItemMapByName);

	  // Fix COUNTER reports references.
	  fixUnknownPublisherPublicationCounterReportsData(conn,
	      unknownPublicationSeq);

	  // Remove the metadata item of the publication created for an unknown
	  // publisher.
	  removeUnknownPublisherPublicationMdItem(conn, unknownPublicationSeq);

	  // Remove the publication created for an unknown publisher.
	  removeUnknownPublisherPublication(conn, unknownPublicationSeq);
	}
      }
    }

    // Remove the record of the fixed unknown publisher problem.
    removeUnknownPublisher(conn, unknownPublisherName);

    // Remove the record of the fixed unknown publisher problem.
    removeAuProblem(conn, auId, unknownPublisherName);

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the identifiers of the publications of a publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the identifier of the publisher.
   * @return a Set<Long> with the identifiers of the publications.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Set<Long> findPublisherPublications(Connection conn,
      Long publisherSeq) throws DbException {
    final String DEBUG_HEADER = "findPublisherPublications(): ";
    Set<Long> publicationSeqs = new HashSet<Long>();
    Long publicationSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findPublications =
	dbManager.prepareStatement(conn, FIND_PUBLISHER_PUBLICATIONS_QUERY);

    try {
      findPublications.setLong(1, publisherSeq);

      resultSet = dbManager.executeQuery(findPublications);
      while (resultSet.next()) {
	publicationSeq = resultSet.getLong(PUBLICATION_SEQ_COLUMN);
	log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

	publicationSeqs.add(publicationSeq);
      }
    } catch (SQLException sqle) {
      log.error("Cannot find the publications of a publisher", sqle);
      log.error("publisherSeq = '" + publisherSeq + "'.");
      log.error("SQL = '" + FIND_PUBLISHER_PUBLICATIONS_QUERY + "'.");
      throw new DbException("Cannot find the publications of a publisher",
	  sqle);
    } finally {
      MetadataDbManager.safeCloseResultSet(resultSet);
      MetadataDbManager.safeCloseStatement(findPublications);
    }

    return publicationSeqs;
  }

  /**
   * Provides the identifiers of the child metadata items (chapters, articles)
   * of a publication.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publicationSeq
   *          A Long with the identifier of the publication.
   * @return a Set<Long> with the identifiers of the metadata items.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Set<Long> findPublicationChildMetadataItems(Connection conn,
      Long publicationSeq) throws DbException {
    final String DEBUG_HEADER = "findPublicationChildMetadataItems(): ";
    Set<Long> mdItemSeqs = new HashSet<Long>();
    Long mdItemSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findMdItems =
	dbManager.prepareStatement(conn, FIND_PUBLICATION_CHILD_MD_ITEMS_QUERY);

    try {
      findMdItems.setLong(1, publicationSeq);

      resultSet = dbManager.executeQuery(findMdItems);
      while (resultSet.next()) {
	mdItemSeq = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
	log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

	mdItemSeqs.add(mdItemSeq);
      }
    } catch (SQLException sqle) {
      log.error("Cannot find the child metadata items of a publication", sqle);
      log.error("publicationSeq = '" + publicationSeq + "'.");
      log.error("SQL = '" + FIND_PUBLICATION_CHILD_MD_ITEMS_QUERY + "'.");
      throw new DbException(
	  "Cannot find the child metadata items of a publication", sqle);
    } finally {
      MetadataDbManager.safeCloseResultSet(resultSet);
      MetadataDbManager.safeCloseStatement(findMdItems);
    }

    return mdItemSeqs;
  }

  /**
   * Fixes the metadata of a publication of an unknown publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param unknownPublicationSeq
   *          A Long with the identifier of the publication.
   * @param mdItemMapByName
   *          A Map<String, Long> with a map of the current publication metadata
   *          items by their names.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void fixUnknownPublisherPublicationMetadata(Connection conn,
      Long unknownPublicationSeq, Map<String, Long> mdItemMapByName)
      throws DbException {
    final String DEBUG_HEADER = "fixUnknownPublisherPublicationMetadata(): ";
    log.debug3(DEBUG_HEADER + "unknownPublicationSeq = "
	+ unknownPublicationSeq);

    // Get the identifiers of the metadata items of the unknown publication.
    Set<Long> unknownMdItemSeqs =
	findPublicationChildMetadataItems(conn, unknownPublicationSeq);

    // Loop through all the identifiers of the metadata items of the unknown
    // publication.
    for (Long unknownMdItemSeq : unknownMdItemSeqs) {
      boolean merged = false;

      // Map the identifier by each of its names.
      Map<String, String> unknownMdItemSeqNames =
	  mdManager.getMdItemNames(conn, unknownMdItemSeq);

      // Loop through all of the names of the unknown publication metadata item.
      for (String unknownMdItemSeqName : unknownMdItemSeqNames.keySet()) {
	// Check whether the current publication has a child metadata item with
	// the same name.
	if (mdItemMapByName.containsKey(unknownMdItemSeqName)) {
	  // Yes: Merge the properties of the unknown publication child metadata
	  // item into the corresponding current publication child metadata
	  // item.
	  mdxManager.mergeChildMdItemProperties(conn, unknownMdItemSeq,
	      mdItemMapByName.get(unknownMdItemSeqName));

	  merged = true;
	  break;
	}
      }

      // Check whether the properties were not merged.
      if (!merged) {
	// Yes: Assign the unknown publication metadata item to the current
	// publication.
	updateMdItemParentSeq(conn, unknownMdItemSeq, parentSeq);
      }
    }

    // Get the identifier of the unknown publication metadata item.
    Long unknownParentSeq =
	mdManager.findPublicationMetadataItem(conn, unknownPublicationSeq);
    log.debug3(DEBUG_HEADER + "unknownParentSeq = " + unknownParentSeq);

    // Merge the properties of the unknown publication metadata item into the
    // current publication metadata item.
    mdxManager.mergeParentMdItemProperties(conn, unknownParentSeq, parentSeq);

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Updates the identifier of the parenet of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the identifier of the metadata item.
   * @param parentSeq
   *          A Long with the identifier of the parent metadata item.
   * @return a Set<Long> with the identifiers of the Archival Units.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void updateMdItemParentSeq(Connection conn, Long mdItemSeq,
      Long parentSeq) throws DbException {
    final String DEBUG_HEADER = "updateMdItemParentSeq(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "parentSeq = " + parentSeq);
    }

    PreparedStatement updateParentSeq =
	dbManager.prepareStatement(conn, UPDATE_MD_ITEM_PARENT_SEQ_QUERY);

    try {
      updateParentSeq.setLong(1, parentSeq);
      updateParentSeq.setLong(2, mdItemSeq);
      int count = dbManager.executeUpdate(updateParentSeq);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      log.error("Cannot update the parent sequence", sqle);
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      log.error("parentSeq = '" + parentSeq + "'.");
      log.error("SQL = '" + UPDATE_MD_ITEM_PARENT_SEQ_QUERY + "'.");
      throw new DbException("Cannot update the parent sequence", sqle);
    } finally {
      MetadataDbManager.safeCloseStatement(updateParentSeq);
    }
  }

  /**
   * Fixes the COUNTER Reports data of a publication of an unknown publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param unknownPublicationSeq
   *          A Long with the identifier of the publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void fixUnknownPublisherPublicationCounterReportsData(Connection conn,
      Long unknownPublicationSeq)throws DbException {
    final String DEBUG_HEADER =
	"fixUnknownPublisherPublicationCounterReportsData(): ";
    log.debug3(DEBUG_HEADER + "unknownPublicationSeq = "
	+ unknownPublicationSeq);

    CounterReportsManager crManager =
	LockssDaemon.getLockssDaemon().getCounterReportsManager();

    // Merge the book type aggregate counts.
    crManager.mergeBookTypeAggregates(conn, unknownPublicationSeq,
	publicationSeq);

    // Delete the book type aggregate counts for the unknown publisher
    // publication.
    crManager.deleteBookTypeAggregates(conn, unknownPublicationSeq);

    // Merge the journal type aggregate counts.
    crManager.mergeJournalTypeAggregates(conn, unknownPublicationSeq,
	publicationSeq);

    // Delete the journal type aggregate counts for the unknown publisher
    // publication.
    crManager.deleteJournalTypeAggregates(conn, unknownPublicationSeq);

    // Merge the journal publication year aggregate counts.
    crManager.mergeJournalPubYearAggregates(conn, unknownPublicationSeq,
	publicationSeq);

    // Delete the journal publication year aggregate counts for the unknown
    // publisher
    // publication.
    crManager.deleteJournalPubYearAggregates(conn, unknownPublicationSeq);

    log.debug3(DEBUG_HEADER + "Done.");
  }

  /**
   * Removes the metadata item of a publication created for an unknown
   * publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publicationSeq
   *          A Long with the publication identifier.
   * @return an int with the number of rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int removeUnknownPublisherPublicationMdItem(Connection conn,
      Long publicationSeq) throws DbException {
    final String DEBUG_HEADER = "removeUnknownPublisherPublicationMdItem(): ";
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    int count = 0;

    if (publicationSeq != null) {
      log.debug3(DEBUG_HEADER + "SQL = '"
	  + DELETE_UNKNOWN_PUBLISHER_PUBLICATION_MD_ITEM_QUERY + "'.");
      PreparedStatement deleteMdItem = null;

      try {
	deleteMdItem = dbManager.prepareStatement(conn,
	    DELETE_UNKNOWN_PUBLISHER_PUBLICATION_MD_ITEM_QUERY);
	deleteMdItem.setLong(1, publicationSeq);
	deleteMdItem.setLong(2, publicationSeq);
	count = dbManager.executeUpdate(deleteMdItem);
      } catch (SQLException sqle) {
	log.error("Cannot delete an unknown publisher publication", sqle);
	log.error("publicationSeq = " + publicationSeq);
	log.error("SQL = '"
	    + DELETE_UNKNOWN_PUBLISHER_PUBLICATION_MD_ITEM_QUERY + "'.");
	throw new DbException("Cannot delete an unknown publisher publication",
	    sqle);
      } finally {
	MetadataDbManager.safeCloseStatement(deleteMdItem);
      }
    }

    log.debug3(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Removes a publication created for an unknown publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publicationSeq
   *          A Long with the publication identifier.
   * @return an int with the number of rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int removeUnknownPublisherPublication(Connection conn,
      Long publicationSeq) throws DbException {
    final String DEBUG_HEADER = "removeUnknownPublisherPublication(): ";
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    int count = 0;

    if (publicationSeq != null) {
      log.debug3(DEBUG_HEADER + "SQL = '"
	  + DELETE_UNKNOWN_PUBLISHER_PUBLICATION_QUERY + "'.");
      PreparedStatement deletePublication = null;

      try {
	deletePublication = dbManager.prepareStatement(conn,
	    DELETE_UNKNOWN_PUBLISHER_PUBLICATION_QUERY);
	deletePublication.setLong(1, publicationSeq);
	deletePublication.setLong(2, publicationSeq);
	count = dbManager.executeUpdate(deletePublication);
      } catch (SQLException sqle) {
	log.error("Cannot delete an unknown publisher publication", sqle);
	log.error("publicationSeq = " + publicationSeq);
	log.error("SQL = '" + DELETE_UNKNOWN_PUBLISHER_PUBLICATION_QUERY
	    + "'.");
	throw new DbException("Cannot delete an unknown publisher publication",
	    sqle);
      } finally {
	MetadataDbManager.safeCloseStatement(deletePublication);
      }
    }

    log.debug3(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Removes an unknown publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherName
   *          A String with the publisher name.
   * @return an int with the number of rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int removeUnknownPublisher(Connection conn,
      String publisherName) throws DbException {
    final String DEBUG_HEADER = "removeUnknownPublisherPublication(): ";
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    int count = 0;

    if (publisherName != null
	&& publisherName.startsWith(UNKNOWN_PUBLISHER_AU_PROBLEM)) {
      log.debug3(DEBUG_HEADER + "SQL = '"
	  + DELETE_UNKNOWN_PUBLISHER_QUERY + "'.");
      PreparedStatement deletePublisher = null;

      try {
	deletePublisher =
	    dbManager.prepareStatement(conn, DELETE_UNKNOWN_PUBLISHER_QUERY);
	deletePublisher.setString(1, publisherName);
	count = dbManager.executeUpdate(deletePublisher);
      } catch (SQLException sqle) {
	log.error("Cannot delete an unknown publisher", sqle);
	log.error("publisherName = " + publisherName);
	log.error("SQL = '" + DELETE_UNKNOWN_PUBLISHER_QUERY
	    + "'.");
	throw new DbException("Cannot delete an unknown publisher", sqle);
      } finally {
	MetadataDbManager.safeCloseStatement(deletePublisher);
      }
    }

    log.debug3(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Populates in the database any needed Archival Unit dependencies.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws MetadataIndexingException
   *           if the plugin could neither be found in the database nor added to
   *           it.
   */
  private void populateAuDbDependencies(Connection conn)
      throws DbException, MetadataIndexingException {
    final String DEBUG_HEADER = "populateAuDbDependencies(): ";
    // Check whether the plugin has not been located in the database.
    if (pluginSeq == null) {
      // Yes: Find the publishing platform or create it.
      Long platformSeq = mdManager.findOrCreatePlatform(conn, platform);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "platformSeq = " + platformSeq);

      if (platformSeq == null) {
        String message = "Cannot find or create platform '" + platform + "'";
        log.error(message);
        throw new MetadataIndexingException(message);
      }

      // Find the plugin or create it.
      pluginSeq = mdManager.findOrCreatePlugin(conn, pluginId, platformSeq,
	  isBulkContent);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginSeq = " + pluginSeq);

      if (pluginSeq == null) {
  	String message = "Cannot find or create plugin '" + pluginId
  	    + "' for platform '" + platform + "'";
  	log.error(message);
  	log.error("platformSeq = " + platformSeq);
  	throw new MetadataIndexingException(message);
      }
    }

    // Check whether the Archival Unit has not been located in the database.
    if (auSeq == null) {
      // Yes: Find it or create it.
      auSeq = mdManager.findOrCreateAu(conn, pluginSeq, auKey);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

      if (auSeq == null) {
  	String message = "Cannot find or create AU '" + auKey + "' for plugin '"
  	    + pluginId + "'";
  	log.error(message);
  	log.error("pluginSeq = " + pluginSeq);
  	throw new MetadataIndexingException(message);
      }
    }
  }

  /**
   * Populates the identifier of the current Archival Unit metadata if existing
   * or after creating it otherwise.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @param providerLid
   *          A String with the provider LOCKSS identifier.
   * @param providerName
   *          A String with the provider name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws MetadataIndexingException
   *           if the current Archival Unit metadata could neither be found in
   *           the database nor added to it.
   */
  private void addAuMd(Connection conn, String providerLid, String providerName)
      throws DbException, MetadataIndexingException {
    final String DEBUG_HEADER = "addAuMd(): ";
    long creationTime = 0;

    // Check whether the content is obtained via web services, not from the
    // local repository.
    if (!ConfigManager.getCurrentConfig()
	.getBoolean(PluginManager.PARAM_AU_CONTENT_FROM_WS,
	    PluginManager.DEFAULT_AU_CONTENT_FROM_WS)) {
      // Yes: Check whether it is possible to obtain the Archival Unit creation
      // time.
      if (au != null &&AuUtil.getAuState(au) != null) {
	// Yes: Get it.
	creationTime = AuUtil.getAuCreationTime(au);
      }
    } else {
      // No.
      if (log.isDebug()) log.debug(DEBUG_HEADER
	  + "Not storing the (unavailable) Archival Unit creation time.");
    }

    // Get the unknown provider, as it can only be obtained from the metadata.
    Long providerSeq =
	dbManager.findOrCreateProvider(conn, providerLid, providerName);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "providerSeq = " + providerSeq);

    // Add to the database the new Archival Unit metadata extraction data.
    auMdSeq = mdManager.addAuMd(conn, auSeq, pluginVersion,
	NEVER_EXTRACTED_EXTRACTION_TIME, creationTime, providerSeq);
    log.debug3(DEBUG_HEADER + "new auSeq = " + auMdSeq);

    if (auMdSeq == null) {
      String message = "Cannot create AuMd for '" + auKey + "' and plugin '"
	  + pluginId + "'";
      log.error(message);
      throw new MetadataIndexingException(message);
    }
  }

  /**
   * Writes to the database a record of the archival unit metadata extraction
   * when no metadata has been found.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   * @throws MetadataIndexingException
   *           if some element could neither be found in the database nor added
   *           to it.
   */
  void recordMetadataExtraction(Connection conn)
      throws DbException, MetadataIndexingException {
    final String DEBUG_HEADER = "recordMetadataExtraction(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    // Find the AU in the database.
    auSeq = mdxManagerSql.findAuByAuId(conn, auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

    // Check whether the AU was not found.
    if (auSeq == null) {
      // Yes: Populate in the database any necessary Archival Unit dependencies.
      populateAuDbDependencies(conn);
    }

    // Find the Archival Unit metadata in the database.
    auMdSeq = findAuMd(conn, auSeq);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);

    // Check whether it is a new Archival Unit metadata.
    if (auMdSeq == null) {
      // Yes: Create it.
      addAuMd(conn, null, UNKNOWN_PROVIDER_NAME);
    } else {
      // No: Update the Archival Unit metadata ancillary data.
      updateAuMd(conn, auMdSeq, pluginVersion);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Updated AU plugin version.");
    }

    if (au != null) {
    // Update the last extraction time.
      mdxManager.updateAuLastExtractionTime(au, conn, auMdSeq);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Updated AU last extraction time.");
    } else {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "AU is null: AU last extraction time NOT updated.");
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Updates in the database the unknown provider of an archival unit.
   * 
   * @param conn
   *          A Connection with the connection to the database
   * @param auMdSeq
   *          a Long with the database identifier of the archival unit metadata.
   * @param mdinfo
   *          An ArticleMetadataInfo providing the metadata, including the
   *          provider.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void updateAuUnknownProvider(Connection conn, Long auMdSeq,
      ArticleMetadataInfo mdinfo) throws DbException {
    final String DEBUG_HEADER = "findOrCreateProvider(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Invoked.");

    // Get the name of the provider received in the metadata.
    String providerName = mdinfo.provider;
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "providerName = " + providerName);

    // Check whether no provider name was received in the metadata.
    if (StringUtil.isNullString(providerName)) {
      // Yes: Use the publisher name.
      providerName = publisherName;
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "providerName = " + providerName);
    }

    // Find or create the provider.
    // TODO: Replace the second argument with mdinfo.providerLid when available.
    Long providerSeq = dbManager.findOrCreateProvider(conn, null, providerName);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "providerSeq = " + providerSeq);

    boolean updated =
	mdxManagerSql.updateAuUnknownProvider(conn, auMdSeq, providerSeq);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "updated = " + updated);

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }
}
