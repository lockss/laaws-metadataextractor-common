/*

Copyright (c) 2015-2018 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.metadata.extractor.MetadataExtractorManager.*;
import static org.lockss.metadata.SqlConstants.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.db.JdbcContext;
import org.lockss.metadata.ItemMetadata;
import org.lockss.metadata.ItemMetadataContinuationToken;
import org.lockss.metadata.ItemMetadataPage;
import org.lockss.metadata.extractor.MetadataExtractorManager.PrioritizedAuId;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.PluginManager;
import org.lockss.util.Logger;

public class MetadataExtractorManagerSql {
  private static final Logger log =
      Logger.getLogger(MetadataExtractorManagerSql.class);

  private static final int UNKNOWN_VERSION = -1;

  // Query to count enabled pending AUs.
  private static final String COUNT_ENABLED_PENDING_AUS_QUERY = "select "
      + "count(*) from " + PENDING_AU_TABLE
      + " where " + PRIORITY_COLUMN + " >= 0";

  // Query to count bibliographic items.
  private static final String COUNT_BIB_ITEM_QUERY = "select count(*) from "
      + BIB_ITEM_TABLE;

  // Query to count PUBLISHER items that have associated AU_ITEMs
  private static final String COUNT_PUBLISHER_QUERY = 
        "select count(distinct "
      + PUBLISHER_TABLE + "." + PUBLISHER_SEQ_COLUMN + ") from "
      + PUBLISHER_TABLE + "," + PUBLICATION_TABLE + "," + MD_ITEM_TABLE 
      + " where " + PUBLISHER_TABLE + "." + PUBLISHER_SEQ_COLUMN
      + "=" + PUBLICATION_TABLE + "." + PUBLISHER_SEQ_COLUMN
      + " and " + PUBLICATION_TABLE + "." + MD_ITEM_SEQ_COLUMN
      + "=" + MD_ITEM_TABLE + "." + MD_ITEM_SEQ_COLUMN;

  // Query to count PROVIDER items that have associated AU_ITEMs
  private static final String COUNT_PROVIDER_QUERY =
      "select count(distinct "
    + PROVIDER_TABLE + "." + PROVIDER_SEQ_COLUMN + ") from "
    + PROVIDER_TABLE + "," + AU_MD_TABLE + "," + MD_ITEM_TABLE 
    + " where " + PROVIDER_TABLE + "." + PROVIDER_SEQ_COLUMN
    + "=" + AU_MD_TABLE + "." + PROVIDER_SEQ_COLUMN
    + " and " + AU_MD_TABLE + "." + AU_MD_SEQ_COLUMN
    + "=" + MD_ITEM_TABLE + "." + AU_MD_SEQ_COLUMN;
  
  // Query to find enabled pending AUs sorted by priority. Subsitute "true"
  // to prioritize indexing new AUs ahead of reindexing existing ones, "false"
  // to index in the order they were added to the queue. AUs with a priority of
  // zero (requested from the Debug Panel) are always sorted first.
  private static final String FIND_PRIORITIZED_ENABLED_PENDING_AUS_QUERY =
        "select "
      +       PENDING_AU_TABLE + "." + PLUGIN_ID_COLUMN
      + "," + PENDING_AU_TABLE + "." + AU_KEY_COLUMN
      + "," + PENDING_AU_TABLE + "." + PRIORITY_COLUMN
      + ",(" + AU_MD_TABLE + "." + AU_SEQ_COLUMN + " is null) as "
      + ISNEW_COLUMN
      + "," + PENDING_AU_TABLE + "." + FULLY_REINDEX_COLUMN
      + " from " + PENDING_AU_TABLE
      + "   left join " + PLUGIN_TABLE
      + "     on " + PLUGIN_TABLE + "." + PLUGIN_ID_COLUMN
      + "        = " + PENDING_AU_TABLE + "." + PLUGIN_ID_COLUMN
      + "   left join " + AU_TABLE
      + "     on " + AU_TABLE + "." + AU_KEY_COLUMN
      + "        = " + PENDING_AU_TABLE + "." + AU_KEY_COLUMN
      + "    and " + AU_TABLE + "." + PLUGIN_SEQ_COLUMN
      + "        = " + PLUGIN_TABLE + "." + PLUGIN_SEQ_COLUMN
      + "   left join " + AU_MD_TABLE
      + "     on " + AU_MD_TABLE + "." + AU_SEQ_COLUMN
      + "        = " + AU_TABLE + "." + AU_SEQ_COLUMN
      + " where " + PRIORITY_COLUMN + " >= 0"
      + " order by (" + PENDING_AU_TABLE + "." + PRIORITY_COLUMN + " > 0),"
      + "(true = ? and " + AU_MD_TABLE + "." + AU_SEQ_COLUMN + " is not null)," 
      + PENDING_AU_TABLE + "." + PRIORITY_COLUMN;

  // Query to delete a pending AU by its key and plugin identifier.
  private static final String DELETE_PENDING_AU_QUERY = "delete from "
      + PENDING_AU_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to delete the metadata items of an Archival Unit.
  private static final String DELETE_AU_MD_ITEM_QUERY = "delete from "
      + MD_ITEM_TABLE
      + " where "
      + AU_MD_SEQ_COLUMN + " = ?";

  // Query to get the identifier of the metadata of an AU in the database.
  private static final String FIND_AU_MD_BY_AU_ID_QUERY = "select m."
      + AU_MD_SEQ_COLUMN
      + " from " + AU_MD_TABLE + " m,"
      + AU_TABLE + " a,"
      + PLUGIN_TABLE + " p"
      + " where m." + AU_SEQ_COLUMN + " = " + "a." + AU_SEQ_COLUMN
      + " and a." + PLUGIN_SEQ_COLUMN + " = " + "p." + PLUGIN_SEQ_COLUMN
      + " and p." + PLUGIN_ID_COLUMN + " = ?"
      + " and a." + AU_KEY_COLUMN + " = ?";

  // Query to delete an AU by Archival Unit key and plugin identifier.
  private static final String DELETE_AU_QUERY = "delete from " + AU_TABLE
      + " where "
      + AU_SEQ_COLUMN + " = ?";

  // Query to get the identifier of an AU in the database.
  private static final String FIND_AU_BY_AU_ID_QUERY = "select a."
      + AU_SEQ_COLUMN
      + " from " + AU_TABLE + " a,"
      + PLUGIN_TABLE + " p"
      + " where a." + PLUGIN_SEQ_COLUMN + " = " + "p." + PLUGIN_SEQ_COLUMN
      + " and p." + PLUGIN_ID_COLUMN + " = ?"
      + " and a." + AU_KEY_COLUMN + " = ?";

  // Query to update the extraction time of the metadata of an Archival Unit.
  private static final String UPDATE_AU_MD_EXTRACT_TIME_QUERY = "update "
      + AU_MD_TABLE
      + " set " + EXTRACT_TIME_COLUMN + " = ?"
      + " where " + AU_MD_SEQ_COLUMN + " = ?";

  // Query to delete a disabled pending AU by its key and plugin identifier.
  private static final String DELETE_DISABLED_PENDING_AU_QUERY = "delete from "
      + PENDING_AU_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?"
      + " and " + PRIORITY_COLUMN + " < 0";

  // Query to add an enabled pending AU at the bottom of the current priority
  // list.
  private static final String INSERT_ENABLED_PENDING_AU_QUERY = "insert into "
      + PENDING_AU_TABLE
      + "(" + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + PRIORITY_COLUMN
      + "," + FULLY_REINDEX_COLUMN
      + ") values (?,?,"
      + "(select coalesce(max(" + PRIORITY_COLUMN + "), 0) + 1"
      + " from " + PENDING_AU_TABLE
      + " where " + PRIORITY_COLUMN + " >= 0),?)";

  // Query to add an enabled pending AU at the bottom of the current priority
  // list using MySQL.
  private static final String INSERT_ENABLED_PENDING_AU_MYSQL_QUERY = "insert "
      + "into " + PENDING_AU_TABLE
      + "(" + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + PRIORITY_COLUMN
      + "," + FULLY_REINDEX_COLUMN
      + ") values (?,?,"
      + "(select next_priority from "
      + "(select coalesce(max(" + PRIORITY_COLUMN + "), 0) + 1 as next_priority"
      + " from " + PENDING_AU_TABLE
      + " where " + PRIORITY_COLUMN + " >= 0) as temp_pau_table),?)";

  // Query to add an enabled pending AU at the top of the current priority list.
  private static final String INSERT_HIGHEST_PRIORITY_PENDING_AU_QUERY =
      "insert into "
      + PENDING_AU_TABLE
      + "(" + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + PRIORITY_COLUMN
      + "," + FULLY_REINDEX_COLUMN
      + ") values (?,?,0,?)";

  // Query to find a pending AU by its key and plugin identifier.
  private static final String FIND_PENDING_AU_QUERY = "select "
      + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + " from " + PENDING_AU_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to get the version of the metadata of an AU as is recorded in the
  // database.
  private static final String FIND_AU_METADATA_VERSION_QUERY = "select m."
      + MD_VERSION_COLUMN
      + " from " + AU_MD_TABLE + " m,"
      + AU_TABLE + " a,"
      + PLUGIN_TABLE + " p"
      + " where m." + AU_SEQ_COLUMN + " = " + " a." + AU_SEQ_COLUMN
      + " and a." + PLUGIN_SEQ_COLUMN + " = " + " p." + PLUGIN_SEQ_COLUMN
      + " and p." + PLUGIN_ID_COLUMN + " = ?"
      + " and a." + AU_KEY_COLUMN + " = ?";

  // Query to find the full reindexing flag of an Archival Unit.
  private static final String FIND_AU_FULL_REINDEXING_BY_AU_QUERY = "select "
      + FULLY_REINDEX_COLUMN
      + " from " + PENDING_AU_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";
  
  // Query to update the full reindexing of an Archival Unit.
  private static final String UPDATE_AU_FULL_REINDEXING_QUERY = "update "
      + PENDING_AU_TABLE
      + " set " + FULLY_REINDEX_COLUMN + " = ?"
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to find the extraction time of an Archival Unit.
  private static final String FIND_AU_MD_EXTRACT_TIME_BY_AUSEQ_QUERY = "select "
      + EXTRACT_TIME_COLUMN
      + " from " + AU_MD_TABLE
      + " where " + AU_SEQ_COLUMN + " = ?";

  // Query to find the extraction time of an Archival Unit.
  private static final String FIND_AU_MD_EXTRACT_TIME_BY_AU_QUERY = "select m."
      + EXTRACT_TIME_COLUMN
      + " from " + AU_MD_TABLE + " m,"
      + AU_TABLE + " a,"
      + PLUGIN_TABLE + " p"
      + " where m." + AU_SEQ_COLUMN + " = " + " a." + AU_SEQ_COLUMN
      + " and a." + PLUGIN_SEQ_COLUMN + " = " + " p." + PLUGIN_SEQ_COLUMN
      + " and p." + PLUGIN_ID_COLUMN + " = ?"
      + " and a." + AU_KEY_COLUMN + " = ?";

  // Query to add a platform.
  private static final String INSERT_PLATFORM_QUERY = "insert into "
      + PLATFORM_TABLE
      + "(" + PLATFORM_SEQ_COLUMN
      + "," + PLATFORM_NAME_COLUMN
      + ") values (default,?)";

  // Query to add a disabled pending AU.
  private static final String INSERT_DISABLED_PENDING_AU_QUERY = "insert into "
      + PENDING_AU_TABLE
      + "(" + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + PRIORITY_COLUMN
      + ") values (?,?," + MIN_INDEX_PRIORITY + ")";

  // Query to add a pending AU with failed indexing.
  private static final String INSERT_FAILED_INDEXING_PENDING_AU_QUERY = "insert"
      + " into "
      + PENDING_AU_TABLE
      + "(" + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + "," + PRIORITY_COLUMN
      + ") values (?,?," + FAILED_INDEX_PRIORITY + ")";
  
  // Query to find pending AUs with a given priority.
  private static final String FIND_PENDING_AUS_WITH_PRIORITY_QUERY =
      "select "
      + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + " from " + PENDING_AU_TABLE
      + " where " + PRIORITY_COLUMN + " = ?";

  // Query to find the publisher of an Archival Unit.
  private static final String FIND_AU_PUBLISHER_QUERY = "select distinct "
      + "pr." + PUBLISHER_SEQ_COLUMN
      + " from " + PUBLISHER_TABLE + " pr"
      + "," + PUBLICATION_TABLE + " p"
      + "," + MD_ITEM_TABLE + " m"
      + "," + AU_MD_TABLE + " am"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = p." + PUBLISHER_SEQ_COLUMN
      + " and p." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
      + " and am." + AU_SEQ_COLUMN + " = ?";

  // Query to find the authors of a metadata item.
  private static final String FIND_MD_ITEM_AUTHOR_QUERY = "select "
      + AUTHOR_NAME_COLUMN
      + " from " + AUTHOR_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";

  // Query to find the keywords of a metadata item.
  private static final String FIND_MD_ITEM_KEYWORD_QUERY = "select "
      + KEYWORD_COLUMN
      + " from " + KEYWORD_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";

  // Query to add a metadata item author.
  private static final String INSERT_AUTHOR_QUERY = "insert into "
      + AUTHOR_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + AUTHOR_NAME_COLUMN
      + "," + AUTHOR_IDX_COLUMN
      + ") values (?,?,"
      + "(select coalesce(max(" + AUTHOR_IDX_COLUMN + "), 0) + 1"
      + " from " + AUTHOR_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?))";

  // Query to add a metadata item author using MySQL.
  private static final String INSERT_AUTHOR_MYSQL_QUERY = "insert into "
      + AUTHOR_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + AUTHOR_NAME_COLUMN
      + "," + AUTHOR_IDX_COLUMN
      + ") values (?,?,"
      + "(select next_idx from "
      + "(select coalesce(max(" + AUTHOR_IDX_COLUMN + "), 0) + 1 as next_idx"
      + " from " + AUTHOR_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?) as temp_author_table))";
  
  // Query to add a metadata item keyword.
  private static final String INSERT_KEYWORD_QUERY = "insert into "
      + KEYWORD_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + KEYWORD_COLUMN
      + ") values (?,?)";

  // Query to remove an archival unit from the UNCONFIGURED_AU table.
  private static final String DELETE_UNCONFIGURED_AU_QUERY = "delete from "
      + UNCONFIGURED_AU_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to delete an Archival Unit child metadata item.
  private static final String DELETE_AU_CHILD_MD_ITEM_QUERY = "delete from "
      + MD_ITEM_TABLE
      + " where "
      + AU_MD_SEQ_COLUMN + " = ?"
      + " and " + MD_ITEM_SEQ_COLUMN + " = ?"
      + " and " + PARENT_SEQ_COLUMN + " is not null";

  // Query to update the unknown provider of an Archival Unit.
  private static final String UPDATE_AU_MD_UNKNOWN_PROVIDER_QUERY = "update "
      + AU_MD_TABLE
      + " set " + PROVIDER_SEQ_COLUMN + " = ?"
      + " where " + AU_MD_SEQ_COLUMN + " IN ("
      + "select am." + AU_MD_SEQ_COLUMN
      + " from " + AU_MD_TABLE + " am"
      + "," + PROVIDER_TABLE + " p"
      + " where am." + PROVIDER_SEQ_COLUMN + " = p." + PROVIDER_SEQ_COLUMN
      + " and p." + PROVIDER_NAME_COLUMN + " = '" + UNKNOWN_PROVIDER_NAME + "'"
      + " and am." + AU_MD_SEQ_COLUMN + " = ?"
      + ")";

  // Query to find a page of metadata items of an AU in the database.
  private static final String FIND_NEXT_PAGE_AU_MD_ITEM_QUERY = "select "
      + "distinct pr." + PUBLISHER_NAME_COLUMN
      + ", min1." + NAME_COLUMN + " as publication_name"
      + ", mi2." + MD_ITEM_SEQ_COLUMN
      + ", min2." + NAME_COLUMN + " as item_title"
      + ", mi2." + DATE_COLUMN
      + ", mi2." + COVERAGE_COLUMN
      + ", mi2." + FETCH_TIME_COLUMN
      + ", b." + VOLUME_COLUMN
      + ", b." + ISSUE_COLUMN
      + ", b." + START_PAGE_COLUMN
      + ", b." + END_PAGE_COLUMN
      + ", b." + ITEM_NO_COLUMN
      + ", d." + DOI_COLUMN
      + ", pv." + PROVIDER_NAME_COLUMN
      + " from " + PUBLISHER_TABLE + " pr"
      + "," + PLUGIN_TABLE + " pl"
      + "," + PUBLICATION_TABLE + " pn"
      + "," + MD_ITEM_NAME_TABLE + " min1"
      + "," + AU_MD_TABLE + " am"
      + "," + AU_TABLE
      + "," + PROVIDER_TABLE + " pv"
      + "," + MD_ITEM_TABLE + " mi2"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min2"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
      + " and min2." + NAME_TYPE_COLUMN + " = 'primary'"
      + " left outer join " + DOI_TABLE + " d"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " left outer join " + BIB_ITEM_TABLE + " b"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = b." + MD_ITEM_SEQ_COLUMN
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
      + " and min1." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = mi2." + PARENT_SEQ_COLUMN
      + " and mi2." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
      + " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
      + " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
      + " and am." + PROVIDER_SEQ_COLUMN + " = pv." + PROVIDER_SEQ_COLUMN
      + " and pl." + PLUGIN_ID_COLUMN + " = ?"
      + " and au." + AU_KEY_COLUMN + " = ?"
      + " and mi2." + MD_ITEM_SEQ_COLUMN + " > ?"
      + " order by mi2." + MD_ITEM_SEQ_COLUMN;

  // Query to find the possibly multiple metadata children of metadata items in
  // the database.
  private static final String GET_MULTIPLE_MD_DETAIL_QUERY = "select distinct "
      + "mi2." + MD_ITEM_SEQ_COLUMN
      + ", u." + FEATURE_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + AUTHOR_NAME_COLUMN
      + ", a." + AUTHOR_IDX_COLUMN
      + ", k." + KEYWORD_COLUMN
      + ", issn." + ISSN_COLUMN
      + ", issn." + ISSN_TYPE_COLUMN
      + ", isbn." + ISBN_COLUMN
      + ", isbn." + ISBN_TYPE_COLUMN
      + ", pi." + PROPRIETARY_ID_COLUMN
      + " from " + MD_ITEM_TABLE + " mi2"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min2"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
      + " and min2." + NAME_TYPE_COLUMN + " = 'primary'"
      + " left outer join " + ISSN_TABLE
      + " on mi2." + PARENT_SEQ_COLUMN + " = issn." + MD_ITEM_SEQ_COLUMN
      + " left outer join " + ISBN_TABLE
      + " on mi2." + PARENT_SEQ_COLUMN + " = isbn." + MD_ITEM_SEQ_COLUMN
      + " left outer join " + PROPRIETARY_ID_TABLE + " pi"
      + " on mi2." + PARENT_SEQ_COLUMN + " = pi." + MD_ITEM_SEQ_COLUMN
      + " left outer join " + URL_TABLE + " u"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = u." + MD_ITEM_SEQ_COLUMN
      + " left outer join " + AUTHOR_TABLE + " a"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = a." + MD_ITEM_SEQ_COLUMN
      + " left outer join " + KEYWORD_TABLE + " k"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = k." + MD_ITEM_SEQ_COLUMN
      + " where mi2." + MD_ITEM_SEQ_COLUMN + " in ()"
      + " order by mi2." + MD_ITEM_SEQ_COLUMN + ", a." + AUTHOR_IDX_COLUMN;

  private DbManager dbManager;
  private MetadataExtractorManager mdxManager;

  /**
   * Constructor.
   * 
   * @param dbManager
   *          A DbManager with the database manager.
   * @param mdxManager
   *          A MetadataExtractorManager with the metadata extractor manager.
   */
  MetadataExtractorManagerSql(DbManager dbManager,
      MetadataExtractorManager mdxManager) throws DbException {
    this.dbManager = dbManager;
    this.mdxManager = mdxManager;
  }

  /**
   * Provides the number of enabled pending AUs.
   * 
   * @return a long with the number of enabled pending AUs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getEnabledPendingAusCount() throws DbException {
    final String DEBUG_HEADER = "getEnabledPendingAusCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    // Get a connection to the database.
    Connection conn = dbManager.getConnection();

    try {
      rowCount = getEnabledPendingAusCount(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of enabled pending AUs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a long with the number of enabled pending AUs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getEnabledPendingAusCount(Connection conn) throws DbException {
    final String DEBUG_HEADER = "getEnabledPendingAusCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    PreparedStatement stmt =
	dbManager.prepareStatement(conn, COUNT_ENABLED_PENDING_AUS_QUERY);
    ResultSet resultSet = null;

    try {
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of enabled pending AUs";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_ENABLED_PENDING_AUS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of articles in the metadata database.
   * 
   * @return a long with the number of articles in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getArticleCount() throws DbException {
    final String DEBUG_HEADER = "getArticleCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    // Get a connection to the database.
    Connection conn = dbManager.getConnection();

    try {
      rowCount = getArticleCount(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of articles in the metadata database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a long with the number of articles in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getArticleCount(Connection conn) throws DbException {
    final String DEBUG_HEADER = "getArticleCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    PreparedStatement stmt =
        dbManager.prepareStatement(conn, COUNT_BIB_ITEM_QUERY);
    ResultSet resultSet = null;

    try {
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of articles";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_BIB_ITEM_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of publishers in the metadata database.
   * 
   * @return a long with the number of publishers in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getPublisherCount() throws DbException {
    final String DEBUG_HEADER = "getPublisherCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    // Get a connection to the database.
    Connection conn = dbManager.getConnection();

    try {
      rowCount = getPublisherCount(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of publishers in the metadata database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a long with the number of publishers in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getPublisherCount(Connection conn) throws DbException {
    final String DEBUG_HEADER = "getPublisherCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    PreparedStatement stmt =
        dbManager.prepareStatement(conn, COUNT_PUBLISHER_QUERY);
    ResultSet resultSet = null;

    try {
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of publishers";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_PUBLISHER_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of providers in the metadata database.
   * 
   * @return a long with the number of providers in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getProviderCount() throws DbException {
    final String DEBUG_HEADER = "getProviderCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    // Get a connection to the database.
    Connection conn = dbManager.getConnection();

    try {
      rowCount = getProviderCount(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of providers in the metadata database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a long with the number of providers in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getProviderCount(Connection conn) throws DbException {
    final String DEBUG_HEADER = "getProviderCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    PreparedStatement stmt =
        dbManager.prepareStatement(conn, COUNT_PROVIDER_QUERY);
    ResultSet resultSet = null;

    try {
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of providers";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_PROVIDER_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides a list of AuIds that require reindexing sorted by priority.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param maxAuIds
   *          An int with the maximum number of AuIds to return.
   * @param prioritizeIndexingNewAus
   *          A boolean with the indication of whether to prioritize new
   *          Archival Units for indexing purposes.
   * @return a List<String> with the list of AuIds that require reindexing
   *         sorted by priority.
   */
  List<PrioritizedAuId> getPrioritizedAuIdsToReindex(Connection conn,
      int maxAuIds, boolean prioritizeIndexingNewAus) {
    final String DEBUG_HEADER = "getPrioritizedAuIdsToReindex(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "maxAuIds = " + maxAuIds);
      log.debug2(DEBUG_HEADER + "prioritizeIndexingNewAus = "
	  + prioritizeIndexingNewAus);
    }

    ArrayList<PrioritizedAuId> auIds = new ArrayList<PrioritizedAuId>();

    PreparedStatement selectPendingAus = null;
    ResultSet results = null;
    String sql = FIND_PRIORITIZED_ENABLED_PENDING_AUS_QUERY;
      
    try {
      selectPendingAus = dbManager.prepareStatement(conn, sql);
      selectPendingAus.setBoolean(1, prioritizeIndexingNewAus);
      results = dbManager.executeQuery(selectPendingAus);

      while ((auIds.size() < maxAuIds) && results.next()) {
	String pluginId = results.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
	String auKey = results.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);
	String auId = PluginManager.generateAuId(pluginId, auKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

	if (mdxManager.isEligibleForReindexing(auId)) {
	  if (!mdxManager.activeReindexingTasks.containsKey(auId)) {
	    PrioritizedAuId auToReindex = new PrioritizedAuId();
	    auToReindex.auId = auId;

	    long priority = results.getLong(PRIORITY_COLUMN);
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "priority = " + priority);
	    auToReindex.priority = priority;

	    boolean isNew = results.getBoolean(ISNEW_COLUMN);
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isNew = " + isNew);
	    auToReindex.isNew = isNew;

	    boolean needFullReindex = results.getBoolean(FULLY_REINDEX_COLUMN);
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "needFullReindex = " + needFullReindex);
	    auToReindex.needFullReindex = needFullReindex;

	    auIds.add(auToReindex);
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Added auId = " + auId
		+ " to reindex list");
	  }
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot identify the enabled pending AUs";
      log.error(message, sqle);
      log.error("maxAuIds = " + maxAuIds);
      log.error("SQL = '" + sql + "'.");
      log.error("prioritizeIndexingNewAus = " + prioritizeIndexingNewAus);
    } catch (DbException dbe) {
      String message = "Cannot identify the enabled pending AUs";
      log.error(message, dbe);
      log.error("SQL = '" + sql + "'.");
      log.error("prioritizeIndexingNewAus = " + prioritizeIndexingNewAus);
    } finally {
      DbManager.safeCloseResultSet(results);
      DbManager.safeCloseStatement(selectPendingAus);
    }

    auIds.trimToSize();
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "auIds.size() = " + auIds.size());
    return auIds;
  }

  /**
   * Removes an AU from the pending Aus table.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long removeFromPendingAus(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "removeFromPendingAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String pluginId = null;
    String auKey = null;
    PreparedStatement deletePendingAu =
	dbManager.prepareStatement(conn, DELETE_PENDING_AU_QUERY);

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      auKey = PluginManager.auKeyFromAuId(auId);
  
      deletePendingAu.setString(1, pluginId);
      deletePendingAu.setString(2, auKey);
      int deletedCount = dbManager.executeUpdate(deletePendingAu);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "deletedCount = " + deletedCount);
    } catch (SQLException sqle) {
      String message = "Cannot remove AU from pending table";
      log.error(message, sqle);
      log.error("auId = '" + auId + "'.");
      log.error("SQL = '" + DELETE_PENDING_AU_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(deletePendingAu);
    }

    long enabledPendingAusCount = getEnabledPendingAusCount(conn);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "enabledPendingAusCount = "
	+ enabledPendingAusCount);
    return enabledPendingAusCount;
  }

  /**
   * Removes all metadata items for an AU.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @return an int with the number of metadata items deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int removeAuMetadataItems(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "removeAuMetadataItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);
    int count = 0;

    Long auMdSeq = findAuMdByAuId(conn, auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);

    if (auMdSeq != null) {
      PreparedStatement deleteMetadataItems =
	  dbManager.prepareStatement(conn, DELETE_AU_MD_ITEM_QUERY);

      try {
	deleteMetadataItems.setLong(1, auMdSeq);
	count = dbManager.executeUpdate(deleteMetadataItems);
      } catch (SQLException sqle) {
	String message = "Cannot delete AU metadata items";
	log.error(message, sqle);
	log.error("auId = " + auId);
	log.error("SQL = '" + DELETE_AU_MD_ITEM_QUERY + "'.");
	log.error("auMdSeq = " + auMdSeq);
	throw new DbException(message, sqle);
      } finally {
	DbManager.safeCloseStatement(deleteMetadataItems);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Provides the identifier of an Archival Unit metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @return a Long with the identifier of the Archival Unit metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findAuMdByAuId(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "findAuMdByAuId(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String pluginId = null;
    String auKey = null;
    Long auMdSeq = null;
    PreparedStatement findAuMd =
	dbManager.prepareStatement(conn, FIND_AU_MD_BY_AU_ID_QUERY);
    ResultSet resultSet = null;

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      findAuMd.setString(1, pluginId);
      findAuMd.setString(2, auKey);
      resultSet = dbManager.executeQuery(findAuMd);

      if (resultSet.next()) {
	auMdSeq = resultSet.getLong(AU_MD_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU metadata identifier";
      log.error(message, sqle);
      log.error("auId = " + auId);
      log.error("SQL = '" + FIND_AU_MD_BY_AU_ID_QUERY + "'.");
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAuMd);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
    return auMdSeq;
  }

  /**
   * Removes an AU.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @return an int with the number of rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int removeAu(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "removeAu(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);
    int count = 0;

    Long auSeq = findAuByAuId(conn, auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

    if (auSeq != null) {
      PreparedStatement deleteAu =
	  dbManager.prepareStatement(conn, DELETE_AU_QUERY);

      try {
	deleteAu.setLong(1, auSeq);
	count = dbManager.executeUpdate(deleteAu);
      } catch (SQLException sqle) {
	String message = "Cannot delete AU";
	log.error(message, sqle);
	log.error("auId = " + auId);
	log.error("SQL = '" + DELETE_AU_QUERY + "'.");
	log.error("auSeq = " + auSeq);
	throw new DbException(message, sqle);
      } finally {
	DbManager.safeCloseStatement(deleteAu);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Provides the identifier of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @return a Long with the identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findAuByAuId(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "findAuByAuId(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String pluginId = PluginManager.pluginIdFromAuId(auId);
    String auKey = PluginManager.auKeyFromAuId(auId);
    Long auSeq = null;
    PreparedStatement findAu =
	dbManager.prepareStatement(conn, FIND_AU_BY_AU_ID_QUERY);
    ResultSet resultSet = null;

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      findAu.setString(1, pluginId);
      findAu.setString(2, auKey);
      resultSet = dbManager.executeQuery(findAu);

      if (resultSet.next()) {
	auSeq = resultSet.getLong(AU_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU identifier";
      log.error(message, sqle);
      log.error("auId = " + auId);
      log.error("SQL = '" + FIND_AU_BY_AU_ID_QUERY + "'.");
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);
    return auSeq;
  }

  /**
   * Updates the timestamp of the last extraction of an Archival Unit metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auMdSeq
   *          A Long with the identifier of the Archival Unit metadata.
   * @param now
   *          A long with the timestamp of the metadata extraction to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void updateAuLastExtractionTime(Connection conn, Long auMdSeq, long now)
      throws DbException {
    final String DEBUG_HEADER = "updateAuLastExtractionTime(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
      log.debug2(DEBUG_HEADER + "now = " + now);
    }

    PreparedStatement updateAuLastExtractionTime =
	dbManager.prepareStatement(conn, UPDATE_AU_MD_EXTRACT_TIME_QUERY);

    try {
      updateAuLastExtractionTime.setLong(1, now);
      updateAuLastExtractionTime.setLong(2, auMdSeq);
      dbManager.executeUpdate(updateAuLastExtractionTime);
    } catch (SQLException sqle) {
      String message = "Cannot update the AU extraction time";
      log.error(message, sqle);
      log.error("SQL = '" + UPDATE_AU_MD_EXTRACT_TIME_QUERY + "'.");
      log.error("auMdSeq = '" + auMdSeq + "'.");
      log.error("now = " + now + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(updateAuLastExtractionTime);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
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
  void removeDisabledFromPendingAus(Connection conn, String auId)
      throws DbException {
    final String DEBUG_HEADER = "removeDisabledFromPendingAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String pluginId = null;
    String auKey = null;
    PreparedStatement deletePendingAu =
	dbManager.prepareStatement(conn, DELETE_DISABLED_PENDING_AU_QUERY);

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      auKey = PluginManager.auKeyFromAuId(auId);
  
      deletePendingAu.setString(1, pluginId);
      deletePendingAu.setString(2, auKey);
      dbManager.executeUpdate(deletePendingAu);
    } catch (SQLException sqle) {
      String message = "Cannot remove disabled AU from pending table";
      log.error(message, sqle);
      log.error("auId = '" + auId + "'.");
      log.error("SQL = '" + DELETE_DISABLED_PENDING_AU_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(deletePendingAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
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
    final String DEBUG_HEADER = "getInsertPendingAuBatchStatement(): ";
    if (dbManager.isTypeMysql()) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "SQL = "
	  + INSERT_ENABLED_PENDING_AU_MYSQL_QUERY);
      return dbManager.prepareStatement(conn,
	  INSERT_ENABLED_PENDING_AU_MYSQL_QUERY);
    }

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "SQL = " + INSERT_ENABLED_PENDING_AU_QUERY);
    return dbManager.prepareStatement(conn, INSERT_ENABLED_PENDING_AU_QUERY);
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
    final String DEBUG_HEADER =
	"getPrioritizedInsertPendingAuBatchStatement(): ";
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "SQL = "
	+ INSERT_HIGHEST_PRIORITY_PENDING_AU_QUERY);
    return dbManager.prepareStatement(conn,
	INSERT_HIGHEST_PRIORITY_PENDING_AU_QUERY);
  }

  /**
   * Provides an indication of whether an Archival Unit is pending reindexing.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key.
   * @return a boolean with <code>true</code> if the Archival Unit is pending
   *         reindexing, <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean isAuPending(Connection conn, String pluginId, String auKey)
      throws DbException {
    final String DEBUG_HEADER = "isAuPending(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "pluginId = " + pluginId);
      log.debug2(DEBUG_HEADER + "auKey = " + auKey);
    }

    boolean result = false;
    PreparedStatement selectPendingAu = null;
    ResultSet results = null;

    try {
      selectPendingAu = dbManager.prepareStatement(conn, FIND_PENDING_AU_QUERY);

      // Find the AU in the table.
      selectPendingAu.setString(1, pluginId);
      selectPendingAu.setString(2, auKey);
      results = dbManager.executeQuery(selectPendingAu);
      result = results.next();
    } catch (SQLException sqle) {
      String message = "Cannot find pending AU";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PENDING_AU_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(results);
      DbManager.safeCloseStatement(selectPendingAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Adds an Archival Unit to the batch of Archival Units to be added to the
   * pending Archival Units table in the database.
   * 
   * @param pluginId
   *          A String with the plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key.
   * @param fullReindex
   *          A boolean indicating whether a full reindex of the Archival Unit
   *          is required.
   * @param insertPendingAuBatchStatement
   *          A PreparedStatement with the SQL staement used to add Archival
   *          Units to the pending Archival Units table in the database.
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  void addAuToPendingAusBatch(String pluginId, String auKey,
      boolean fullReindex, PreparedStatement insertPendingAuBatchStatement)
	  throws SQLException {
    insertPendingAuBatchStatement.setString(1, pluginId);
    insertPendingAuBatchStatement.setString(2, auKey);
    insertPendingAuBatchStatement.setBoolean(3, fullReindex);
    insertPendingAuBatchStatement.addBatch();
  }

  /**
   * Adds a batch of Archival Units to the pending Archival Units table in the
   * database.
   * 
   * @param insertPendingAuBatchStatement
   *          A PreparedStatement with the SQL staement used to add Archival
   *          Units to the pending Archival Units table in the database.
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  void addAuBatchToPendingAus(PreparedStatement insertPendingAuBatchStatement)
      throws SQLException {
    final String DEBUG_HEADER = "addAuBatchToPendingAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    insertPendingAuBatchStatement.executeBatch();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the version of the metadata of an AU stored in the database.
   * 
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @return an int with the version of the metadata of the AU stored in the
   *         database.
   */
  int getAuMetadataVersion(ArchivalUnit au) {
    final String DEBUG_HEADER = "getAuMetadataVersion(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "au = " + au);

    int version = UNKNOWN_VERSION;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the version.
      version = getAuMetadataVersion(conn, au);
    } catch (DbException dbe) {
      log.error("Cannot get AU metadata version - Using " + version + ": "
	  + dbe);
      log.error("au = '" + au + "'.");
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "version = " + version);
    return version;
  }


  /**
   * Provides the version of the metadata of an AU stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @return an int with the version of the metadata of the AU stored in the
   *         database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int getAuMetadataVersion(Connection conn, ArchivalUnit au)
      throws DbException {
    final String DEBUG_HEADER = "getAuMetadataVersion(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "au = " + au);

    String pluginId = null;
    String auKey = null;
    int version = UNKNOWN_VERSION;
    PreparedStatement selectMetadataVersion = null;
    ResultSet resultSet = null;

    try {
      String auId = au.getAuId();
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      selectMetadataVersion =
	  dbManager.prepareStatement(conn, FIND_AU_METADATA_VERSION_QUERY);
      selectMetadataVersion.setString(1, pluginId);
      selectMetadataVersion.setString(2, auKey);
      resultSet = dbManager.executeQuery(selectMetadataVersion);

      if (resultSet.next()) {
	version = resultSet.getShort(MD_VERSION_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "version = " + version);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get AU metadata version";
      log.error(message, sqle);
      log.error("au = '" + au + "'.");
      log.error("SQL = '" + FIND_AU_METADATA_VERSION_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(selectMetadataVersion);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "version = " + version);
    return version;
  }

  /**
   * Provides an indication of whether an Archival Unit requires full
   * reindexing.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @return an boolean indicating whether the Archival Unit requires full
   *         reindexing.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean needAuFullReindexing(Connection conn, ArchivalUnit au)
      throws DbException {
    final String DEBUG_HEADER = "needAuFullReindexing(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "au = " + au);

    String auId = au.getAuId();
    String pluginId = PluginManager.pluginIdFromAuId(auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

    String auKey = PluginManager.auKeyFromAuId(auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

    boolean fullReindexing = false;
    PreparedStatement selectFullReindexing = null;
    ResultSet resultSet = null;
  
    try {
      selectFullReindexing =
          dbManager.prepareStatement(conn, FIND_AU_FULL_REINDEXING_BY_AU_QUERY);
      selectFullReindexing.setString(1, pluginId);
      selectFullReindexing.setString(2, auKey);
      resultSet = dbManager.executeQuery(selectFullReindexing);
  
      if (resultSet.next()) {
        fullReindexing = resultSet.getBoolean(FULLY_REINDEX_COLUMN);
        if (log.isDebug3())
          log.debug3(DEBUG_HEADER + "full reindexing = " + fullReindexing);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get AU fully reindexing flag";
      log.error(message, sqle);
      log.error("au = '" + au + "'.");
      log.error("SQL = '" + FIND_AU_FULL_REINDEXING_BY_AU_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(selectFullReindexing);
    }
  
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "fullReindexing = " + fullReindexing);
    return fullReindexing;
  }

  /**
   * Sets whether AU stored in the database requires full reindexing.
   * @param conn
   *          A Connection with the database connection to be used.
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @param fullReindexing the new value of full_reindexing for the AU
   *         in the database
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void updateAuFullReindexing(Connection conn, ArchivalUnit au,
      boolean fullReindexing) throws DbException {
    final String DEBUG_HEADER = "updateAuFullReindexing(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "au = " + au);
      log.debug2(DEBUG_HEADER + "fullReindexing = " + fullReindexing);
    }

    PreparedStatement updateFullReindexing = null;
  
    String auId = au.getAuId();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

    String pluginId = PluginManager.pluginIdFromAuId(auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

    String auKey = PluginManager.auKeyFromAuId(auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

    try {
      updateFullReindexing =
        dbManager.prepareStatement(conn, UPDATE_AU_FULL_REINDEXING_QUERY);
      updateFullReindexing.setBoolean(1, fullReindexing);
      updateFullReindexing.setString(2, pluginId);
      updateFullReindexing.setString(3, auKey);
      dbManager.executeUpdate(updateFullReindexing);
    } catch (SQLException sqle) {
      String message = "Cannot set AU fully reindex flag";
      log.error(message, sqle);
      log.error("au = '" + au + "'.");
      log.error("SQL = '" + UPDATE_AU_FULL_REINDEXING_QUERY + "'.");
      log.error("fullReindexing = '" + fullReindexing + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(updateFullReindexing);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the extraction time of an Archival Unit metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the identifier of the Archival Unit.
   * @return a long with the extraction time of the Archival Unit metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getAuExtractionTime(Connection conn, Long auSeq) throws DbException {
    final String DEBUG_HEADER = "getAuExtractionTime(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);

    long timestamp = NEVER_EXTRACTED_EXTRACTION_TIME;
    PreparedStatement selectLastExtractionTime = null;
    ResultSet resultSet = null;

    try {
      selectLastExtractionTime = dbManager.prepareStatement(conn,
	  FIND_AU_MD_EXTRACT_TIME_BY_AUSEQ_QUERY);
      selectLastExtractionTime.setLong(1, auSeq);
      resultSet = dbManager.executeQuery(selectLastExtractionTime);

      if (resultSet.next()) {
	timestamp = resultSet.getLong(EXTRACT_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "timestamp = " + timestamp);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get AU extraction time";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_AU_MD_EXTRACT_TIME_BY_AUSEQ_QUERY + "'.");
      log.error("auSeq = '" + auSeq + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(selectLastExtractionTime);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "timestamp = " + timestamp);
    return timestamp;
  }

  /**
   * Provides the extraction time of an Archival Unit metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param au
   *          An ArchivalUnit with the AU involved.
   * @return a long with the extraction time of the Archival Unit metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getAuExtractionTime(Connection conn, ArchivalUnit au)
      throws DbException {
    final String DEBUG_HEADER = "getAuExtractionTime(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "au = " + au);

    String pluginId = null;
    String auKey = null;
    long timestamp = NEVER_EXTRACTED_EXTRACTION_TIME;
    PreparedStatement selectLastExtractionTime = null;
    ResultSet resultSet = null;

    try {
      String auId = au.getAuId();
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      selectLastExtractionTime =
	  dbManager.prepareStatement(conn, FIND_AU_MD_EXTRACT_TIME_BY_AU_QUERY);
      selectLastExtractionTime.setString(1, pluginId);
      selectLastExtractionTime.setString(2, auKey);
      resultSet = dbManager.executeQuery(selectLastExtractionTime);

      if (resultSet.next()) {
	timestamp = resultSet.getLong(EXTRACT_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "timestamp = " + timestamp);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get AU extraction time";
      log.error(message, sqle);
      log.error("au = '" + au + "'.");
      log.error("SQL = '" + FIND_AU_MD_EXTRACT_TIME_BY_AU_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(selectLastExtractionTime);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "timestamp = " + timestamp);
    return timestamp;
  }

  /**
   * Adds a disabled AU to the list of pending AUs to reindex.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addDisabledAuToPendingAus(Connection conn, String auId)
      throws DbException {
    final String DEBUG_HEADER = "addDisabledAuToPendingAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String pluginId = null;
    String auKey = null;
    PreparedStatement addPendingAuStatement =
	dbManager.prepareStatement(conn, INSERT_DISABLED_PENDING_AU_QUERY);

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      addPendingAuStatement.setString(1, pluginId);
      addPendingAuStatement.setString(2, auKey);
      int count = dbManager.executeUpdate(addPendingAuStatement);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      String message = "Cannot add disabled pending AU";
      log.error(message, sqle);
      log.error("auId = '" + auId + "'.");
      log.error("SQL = '" + INSERT_PLATFORM_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(addPendingAuStatement);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Adds an AU with failed indexing to the list of pending AUs to reindex.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addFailedIndexingAuToPendingAus(Connection conn, String auId)
      throws DbException {
    final String DEBUG_HEADER = "addFailedIndexingAuToPendingAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String pluginId = null;
    String auKey = null;
    PreparedStatement addPendingAuStatement =
	dbManager.prepareStatement(conn,
	    INSERT_FAILED_INDEXING_PENDING_AU_QUERY);

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      addPendingAuStatement.setString(1, pluginId);
      addPendingAuStatement.setString(2, auKey);
      int count = dbManager.executeUpdate(addPendingAuStatement);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      String message = "Cannot add failed pending AU";
      log.error(message, sqle);
      log.error("auId = '" + auId + "'.");
      log.error("SQL = '" + INSERT_PLATFORM_QUERY + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(addPendingAuStatement);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the identifiers of pending Archival Units with a given priority.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param priority
   *          An int with the priority of the requested Archival Units.
   * @return a Collection<String> with the identifiers of pending Archival Units
   *         with the given priority.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> findPendingAusWithPriority(Connection conn, int priority)
      throws DbException {
    final String DEBUG_HEADER = "findPendingAusWithPriority(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "priority = " + priority);

    Collection<String> aus = new ArrayList<String>();
    String pluginId;
    String auKey;
    String auId;
    ResultSet results = null;

    PreparedStatement selectAus =
	dbManager.prepareStatement(conn, FIND_PENDING_AUS_WITH_PRIORITY_QUERY);

    try {
      selectAus.setInt(1, priority);
      results = dbManager.executeQuery(selectAus);

      while (results.next()) {
	pluginId = results.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
	auKey = results.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);
	auId = PluginManager.generateAuId(pluginId, auKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

	aus.add(auId);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find pending AUs";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_PLATFORM_QUERY + "'.");
      log.error("priority = '" + priority + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(results);
      DbManager.safeCloseStatement(selectAus);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "aus.size() = " + aus.size());
    return aus;
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
  Long findAuPublisher(Connection conn, Long auSeq) throws DbException {
    final String DEBUG_HEADER = "findAuPublisher(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);

    Long publisherSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findPublisher =
	dbManager.prepareStatement(conn, FIND_AU_PUBLISHER_QUERY);

    try {
      findPublisher.setLong(1, auSeq);

      resultSet = dbManager.executeQuery(findPublisher);
      if (resultSet.next()) {
	publisherSeq = resultSet.getLong(PUBLISHER_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find the publisher of an AU";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_AU_PUBLISHER_QUERY + "'.");
      log.error("auSeq = '" + auSeq + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findPublisher);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
    return publisherSeq;
  }

  /**
   * Provides the authors of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @return a Collection<String> with the authors of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getMdItemAuthors(Connection conn, Long mdItemSeq)
      throws DbException {
    final String DEBUG_HEADER = "getMdItemAuthors(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    List<String> authors = new ArrayList<String>();

    PreparedStatement findMdItemAuthor =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_AUTHOR_QUERY);

    ResultSet resultSet = null;

    try {
      // Get the existing authors.
      findMdItemAuthor.setLong(1, mdItemSeq);
      resultSet = dbManager.executeQuery(findMdItemAuthor);

      while (resultSet.next()) {
	authors.add(resultSet.getString(AUTHOR_NAME_COLUMN));
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the authors of a metadata item";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_ITEM_AUTHOR_QUERY + "'.");
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findMdItemAuthor);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "authors = " + authors);
    return authors;
  }

  /**
   * Provides the keywords of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @return A Collection<String> with the keywords of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getMdItemKeywords(Connection conn, Long mdItemSeq)
      throws DbException {
    final String DEBUG_HEADER = "getMdItemKeywords(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    List<String> keywords = new ArrayList<String>();

    PreparedStatement findMdItemKeyword =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_KEYWORD_QUERY);

    ResultSet resultSet = null;

    try {
      // Get the existing keywords.
      findMdItemKeyword.setLong(1, mdItemSeq);
      resultSet = dbManager.executeQuery(findMdItemKeyword);

      while (resultSet.next()) {
	keywords.add(resultSet.getString(KEYWORD_COLUMN));
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the keywords of a metadata item";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_ITEM_KEYWORD_QUERY + "'.");
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findMdItemKeyword);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "keywords = " + keywords);
    return keywords;
  }

  /**
   * Adds to the database the authors of a metadata item.
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
  void addMdItemAuthors(Connection conn, Long mdItemSeq,
      Collection<String> authors) throws DbException {
    final String DEBUG_HEADER = "addMdItemAuthors(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "authors = " + authors);
    }

    if (authors == null || authors.size() == 0) {
      return;
    }

    String sql = getInsertMdItemAuthorSql();
    PreparedStatement insertMdItemAuthor =
	dbManager.prepareStatement(conn, sql);

    try {
      for (String author : authors) {
	insertMdItemAuthor.setLong(1, mdItemSeq);
	insertMdItemAuthor.setString(2, author);
	insertMdItemAuthor.setLong(3, mdItemSeq);
	int count = dbManager.executeUpdate(insertMdItemAuthor);

	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "count = " + count);
	  log.debug3(DEBUG_HEADER + "Added author = " + author);
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot add metadata item authors";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      log.error("authors = " + authors + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemAuthor);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the SQL query used to insert a metadata item author.
   * 
   * @return a String with the SQL query used to insert a metadata item author.
   */
  private String getInsertMdItemAuthorSql() {
    if (dbManager.isTypeMysql()) {
      return INSERT_AUTHOR_MYSQL_QUERY;
    }

    return INSERT_AUTHOR_QUERY;
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
  void addMdItemKeywords(Connection conn, Long mdItemSeq,
      Collection<String> keywords) throws DbException {
    final String DEBUG_HEADER = "addMdItemKeywords(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "keywords = " + keywords);
    }

    if (keywords == null || keywords.size() == 0) {
      return;
    }

    PreparedStatement insertMdItemKeyword =
	dbManager.prepareStatement(conn, INSERT_KEYWORD_QUERY);

    try {
      for (String keyword : keywords) {
	insertMdItemKeyword.setLong(1, mdItemSeq);
	insertMdItemKeyword.setString(2, keyword);
	int count = dbManager.executeUpdate(insertMdItemKeyword);

	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "count = " + count);
	  log.debug3(DEBUG_HEADER + "Added keyword = " + keyword);
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot add metadata item keywords";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_KEYWORD_QUERY + "'.");
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      log.error("keywords = " + keywords + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemKeyword);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Removes an Archival Unit from the table of unconfigured Archival Units.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void removeFromUnconfiguredAus(Connection conn, String auId) {
    final String DEBUG_HEADER = "removeFromUnconfiguredAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    PreparedStatement deleteUnconfiguredAu = null;
    String pluginId = null;
    String auKey = null;

    try {
      if (mdxManager.getMetadataManager().isAuInUnconfiguredAuTable(conn, auId))
      {
	deleteUnconfiguredAu =
	    dbManager.prepareStatement(conn, DELETE_UNCONFIGURED_AU_QUERY);

	pluginId = PluginManager.pluginIdFromAuId(auId);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);
	auKey = PluginManager.auKeyFromAuId(auId);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	deleteUnconfiguredAu.setString(1, pluginId);
	deleteUnconfiguredAu.setString(2, auKey);
	int count = dbManager.executeUpdate(deleteUnconfiguredAu);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
	DbManager.commitOrRollback(conn, log);
      }
    } catch (SQLException sqle) {
      String message = "Cannot delete archival unit from unconfigured table";
      log.error(message, sqle);
      log.error("auId = " + auId);
      log.error("SQL = '" + DELETE_UNCONFIGURED_AU_QUERY + "'.");
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
    } catch (DbException dbe) {
      String message = "Cannot delete archival unit from unconfigured table";
      log.error(message, dbe);
      log.error("auId = " + auId);
      log.error("SQL = '" + DELETE_UNCONFIGURED_AU_QUERY + "'.");
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
    } finally {
      DbManager.safeCloseStatement(deleteUnconfiguredAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Removes an Archival Unit child metadata item from the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auMdSeq
   *          A Long with the identifier of the Archival Unit metadata.
   * @param mdItemSeq
   *          A Long with the metadata identifier.
   * @return an int with the number of metadata items deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int removeAuChildMetadataItem(Connection conn, Long auMdSeq, Long mdItemSeq)
      throws DbException {
    final String DEBUG_HEADER = "removeAuChildMetadataItem(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    }

    int count = 0;

    // Do nothing if any of the parameters are null.
    if (auMdSeq != null && mdItemSeq != null) {
      PreparedStatement deleteMetadataItem =
	  dbManager.prepareStatement(conn, DELETE_AU_CHILD_MD_ITEM_QUERY);

      try {
	deleteMetadataItem.setLong(1, auMdSeq);
	deleteMetadataItem.setLong(2, mdItemSeq);
	count = dbManager.executeUpdate(deleteMetadataItem);
      } catch (SQLException sqle) {
	String message = "Cannot delete child metadata item";
	log.error(message, sqle);
	log.error("mdItemSeq = " + mdItemSeq);
	log.error("SQL = '" + DELETE_AU_CHILD_MD_ITEM_QUERY + "'.");
	throw new DbException(message, sqle);
      } finally {
	DbManager.safeCloseStatement(deleteMetadataItem);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Updates the unknown provider of an archival unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auMdSeq
   *          A Long with the archival unit metadata identifier.
   * @param providerSeq
   *          A Long with the provider identifier.
   * @return a boolean with <code>true</code> if the unknown provider was
   *         updated, <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean updateAuUnknownProvider(Connection conn, Long auMdSeq,
      Long providerSeq) throws DbException {
    final String DEBUG_HEADER = "updateAuUnknownProvider(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
      log.debug2(DEBUG_HEADER + "providerSeq = " + providerSeq);
    }

    int updatedCount = -1;
    PreparedStatement updateUnknownProvider =
	dbManager.prepareStatement(conn, UPDATE_AU_MD_UNKNOWN_PROVIDER_QUERY);

    try {
      updateUnknownProvider.setLong(1, providerSeq);
      updateUnknownProvider.setLong(2, auMdSeq);
      updatedCount = dbManager.executeUpdate(updateUnknownProvider);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "updatedCount = " + updatedCount);
    } catch (SQLException sqle) {
      String message = "Cannot update unknown provider";
      log.error(message, sqle);
      log.error("auMdSeq = '" + auMdSeq + "'.");
      log.error("providerSeq = '" + providerSeq + "'.");
      log.error("SQL = '" + UPDATE_AU_MD_UNKNOWN_PROVIDER_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(updateUnknownProvider);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "result = " + (updatedCount > 0));
    return updatedCount > 0;
  }

  /**
   * Removes all metadata items for an AU.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @return an int with the number of metadata items deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  int removeAuMetadataItems(JdbcContext jdbcCtxt, String auId)
      throws DbException {
    final String DEBUG_HEADER = "removeAuMetadataItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);
    int count = 0;

    Connection conn = jdbcCtxt.getConnection();

    Long auMdSeq = findAuMdByAuId(jdbcCtxt, auId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);

    if (auMdSeq != null) {
      PreparedStatement deleteMetadataItems =
	  dbManager.prepareStatement(conn, DELETE_AU_MD_ITEM_QUERY);
      jdbcCtxt.setStatement(deleteMetadataItems);

      try {
	deleteMetadataItems.setLong(1, auMdSeq);
	count = dbManager.executeUpdate(deleteMetadataItems);
      } catch (SQLException sqle) {
	String message = "Cannot delete AU metadata items";
	log.error(message, sqle);
	log.error("auId = " + auId);
	log.error("SQL = '" + DELETE_AU_MD_ITEM_QUERY + "'.");
	log.error("auMdSeq = " + auMdSeq);
	throw new DbException(message, sqle);
      } finally {
	DbManager.safeCloseStatement(deleteMetadataItems);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Provides the identifier of an Archival Unit metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the AU identifier.
   * @return a Long with the identifier of the Archival Unit metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findAuMdByAuId(JdbcContext jdbcCtxt, String auId)
      throws DbException {
    final String DEBUG_HEADER = "findAuMdByAuId(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    Connection conn = jdbcCtxt.getConnection();

    String pluginId = null;
    String auKey = null;
    Long auMdSeq = null;
    PreparedStatement findAuMd =
	dbManager.prepareStatement(conn, FIND_AU_MD_BY_AU_ID_QUERY);
    ResultSet resultSet = null;
    jdbcCtxt.setStatement(findAuMd);

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      findAuMd.setString(1, pluginId);
      findAuMd.setString(2, auKey);
      resultSet = dbManager.executeQuery(findAuMd);

      if (resultSet.next()) {
	auMdSeq = resultSet.getLong(AU_MD_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU metadata identifier";
      log.error(message, sqle);
      log.error("auId = " + auId);
      log.error("SQL = '" + FIND_AU_MD_BY_AU_ID_QUERY + "'.");
      log.error("pluginId = " + pluginId);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAuMd);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
    return auMdSeq;
  }

  /**
   * Provides the full metadata stored for an AU given the AU identifier or a
   * pageful of the metadata defined by the page index and size.
   * 
   * @param auId
   *          A String with the Archival Unit text identifier.
   * @param limit
   *          An Integer with the maximum number of AU metadata items to be
   *          returned.
   * @param continuationToken
   *          An ItemMetadataContinuationToken with the pagination token, if
   *          any.
   * @return an ItemMetadataPage with the requested metadata of the Archival
   *         Unit.
   * @throws IllegalArgumentException
   *           if the Archival Unit cannot be found in the database.
   * @throws ConcurrentModificationException
   *           if there is a conflict between the pagination request and the
   *           current content in the database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  ItemMetadataPage getAuMetadataDetail(String auId, Integer limit,
      ItemMetadataContinuationToken continuationToken) throws DbException {
    final String DEBUG_HEADER = "getAuMetadataDetail(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auId = " + auId);
      log.debug2(DEBUG_HEADER + "limit = " + limit);
      log.debug2(DEBUG_HEADER + "continuationToken = " + continuationToken);
    }

    ItemMetadataPage result = new ItemMetadataPage();
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      Long auSeq = findAuByAuId(conn, auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

      if (auSeq == null) {
	throw new IllegalArgumentException("AuId not found in DB: " + auId);
      }

      // Get the last extraction time of the Archival Unit.
      long extractionTime = getAuExtractionTime(conn, auSeq);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "extractionTime = " + extractionTime);

      // Get the continuation token members, if any.
      Long auExtractionTimestamp = null;
      Long lastItemMdItemSeq = null;

      if (continuationToken != null) {
	auExtractionTimestamp = continuationToken.getAuExtractionTimestamp();
	lastItemMdItemSeq = continuationToken.getLastItemMdItemSeq();
      }

      // Evaluate the pagination consistency of the request.
      if (auExtractionTimestamp != null
	  && auExtractionTimestamp.longValue() != extractionTime) {
	String message = "Incompatible pagination request: request timestamp: "
	    + continuationToken.getAuExtractionTimestamp()
	    + ", current timestamp:" + " " + extractionTime;
	log.warning(message);
	throw new ConcurrentModificationException("Incompatible pagination for "
	    + "auid '" + auId + "': Metadata has changed since previous "
	    + "request");
      }

      // Get the requested item metadata of the Archival Unit.
      result = getAuMetadataDetail(conn, auId, extractionTime, limit,
	  lastItemMdItemSeq);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    return result;
  }

  /**
   * Provides the full metadata stored for an AU given the AU identifier or a
   * pageful of the metadata defined by the page index and size.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit text identifier.
   * @param auExtractionTime
   *          A long with the Archival Unit last extraction time.
   * @param limit
   *          An Integer with the maximum number of AU metadata items to be
   *          returned.
   * @param lastItemMdItemSeq
   *          A Long with the primary key of the last item previously returned,
   *          if any.
   * @return an ItemMetadataPage with the requested metadata of the Archival
   *         Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private ItemMetadataPage getAuMetadataDetail(Connection conn, String auId,
      long auExtractionTime, Integer limit, Long lastItemMdItemSeq)
	  throws DbException {
    final String DEBUG_HEADER = "getAuMetadataDetail(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auId = " + auId);
      log.debug2(DEBUG_HEADER + "auExtractionTime = " + auExtractionTime);
      log.debug2(DEBUG_HEADER + "limit = " + limit);
      log.debug2(DEBUG_HEADER + "lastItemMdItemSeq = " + lastItemMdItemSeq);
    }

    ItemMetadataPage result = new ItemMetadataPage();
    List<ItemMetadata> items = new ArrayList<ItemMetadata>();
    result.setItems(items);

    Map<Long, ItemMetadata> itemMap = new HashMap<Long, ItemMetadata>();

    String pluginId = null;
    String auKey = null;
    Long previousMdItemSeq = null;
    ItemMetadata itemMetadata = null;
    String sql = FIND_NEXT_PAGE_AU_MD_ITEM_QUERY;

    PreparedStatement getScalarMetadata = dbManager.prepareStatement(conn, sql);

    ResultSet resultSet = null;

    // Indication of whether there are more results that may be requested in a
    // subsequent pagination request.
    boolean hasMore = false;

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId() = " + pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      getScalarMetadata.setString(1, pluginId);
      getScalarMetadata.setString(2, auKey);

      // Handle the first page of requested results.
      if (lastItemMdItemSeq == null) {
	getScalarMetadata.setLong(3, -1);
      } else {
	getScalarMetadata.setLong(3, lastItemMdItemSeq);
      }

      boolean isPaginating = false;

      // Determine whether this is a paginating request.
      if (limit != null && limit.intValue() > 0) {
	// Yes.
	isPaginating = true;

	// Request one more row than needed, to determine whether there will be
	// additional results besides this requested page.
	getScalarMetadata.setMaxRows(limit + 1);
      }

      resultSet = dbManager.executeQuery(getScalarMetadata);

      while (resultSet.next()) {
	// Check whether the full requested page has already been obtained.
	if (isPaginating && items.size() == limit) {
	  // Yes: There are more results after this page.
	  hasMore = true;

	  // Do not process the additional row.
	  break;
	}

	Long mdItemSeq = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

	if (!mdItemSeq.equals(previousMdItemSeq)) {
	  itemMetadata = new ItemMetadata(mdItemSeq);
	  itemMetadata.setScalarMap(new HashMap<String, String>());
	  itemMetadata.setSetMap(new HashMap<String, Set<String>>());
	  itemMetadata.setListMap(new HashMap<String, List<String>>());
	  itemMetadata.setMapMap(new HashMap<String, Map<String, String>>());

	  Map<String, String> scalarMap = itemMetadata.getScalarMap();

	  scalarMap.put("au_id", auId);

	  String providerName = resultSet.getString(PROVIDER_NAME_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "providerName = " + providerName);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(PROVIDER_NAME_COLUMN, providerName);
	  }

	  String publisherName = resultSet.getString(PUBLISHER_NAME_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(PUBLISHER_NAME_COLUMN, publisherName);
	  }

	  String publicationName = resultSet.getString("publication_name");
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	  if (!resultSet.wasNull()) {
	    scalarMap.put("publication_name", publicationName);
	  }

	  String date = resultSet.getString(DATE_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "date = " + date);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(DATE_COLUMN, date);
	  }

	  String coverage = resultSet.getString(COVERAGE_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "coverage = " + coverage);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(COVERAGE_COLUMN, coverage);
	  }

	  Long fetchTime = resultSet.getLong(FETCH_TIME_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "fetchTime = " + fetchTime);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(FETCH_TIME_COLUMN, fetchTime.toString());
	  }

	  String itemTitle = resultSet.getString("item_title");
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "itemTitle = " + itemTitle);

	  if (!resultSet.wasNull()) {
	    scalarMap.put("item_title", itemTitle);
	  }

	  String volume = resultSet.getString(VOLUME_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "volume = " + volume);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(VOLUME_COLUMN, volume);
	  }

	  String issue = resultSet.getString(ISSUE_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issue = " + issue);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(ISSUE_COLUMN, issue);
	  }

	  String startPage = resultSet.getString(START_PAGE_COLUMN);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "startPage = " + startPage);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(START_PAGE_COLUMN, startPage);
	  }

	  String endPage = resultSet.getString(END_PAGE_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "endPage = " + endPage);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(END_PAGE_COLUMN, endPage);
	  }

	  String itemNo = resultSet.getString(ITEM_NO_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "itemNo = " + itemNo);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(ITEM_NO_COLUMN, itemNo);
	  }

	  String doi = resultSet.getString(DOI_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "doi = " + doi);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(DOI_COLUMN, doi);
	  }

	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "itemMetadata = " + itemMetadata);

	  items.add(itemMetadata);
	  itemMap.put(mdItemSeq, itemMetadata);

	  previousMdItemSeq = mdItemSeq;
	} else {
	  log.error("Ignoring unexpected multiple scalar results for "
	      + "mdItemSeq = " + mdItemSeq + ": Existing result: "
	      + itemMetadata);
	}
      }

      resultSet.close();

      // Check whether no metadata was found for the given Archival Unit.
      if (items.size() == 0) {
	// Yes: Done.
	if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
	return result;
      }

      // No: Get the repeatable metadata.
      StringBuilder inIds = new StringBuilder("(");
      boolean isFirst = true;

      for (Long id : itemMap.keySet()) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "id = " + id);

	if (!isFirst) {
	  inIds.append(", ");
	} else {
	  isFirst = false;
	}

	inIds.append(id);
      }

      inIds.append(")");

      sql = GET_MULTIPLE_MD_DETAIL_QUERY.replaceFirst("\\(\\)",
	  inIds.toString());
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "sql = " + sql);

      PreparedStatement getNonScalarMetadata =
	  dbManager.prepareStatement(conn, sql);

      resultSet = dbManager.executeQuery(getNonScalarMetadata);

      while (resultSet.next()) {
	Long mdItemSeq = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

	itemMetadata = itemMap.get(mdItemSeq);

	if (itemMetadata == null) {
	  log.error("Ignoring non-scalar results for mdItemSeq = " + mdItemSeq
	      + " with no previously retrieved scalar results");
	  continue;
	}

	String issn = resultSet.getString(ISSN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issn = " + issn);

	if (!resultSet.wasNull()) {
	  String type = resultSet.getString(ISSN_TYPE_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "type = " + type);

	  if (!resultSet.wasNull()) {
	    Map<String, String> issns =
		itemMetadata.getMapMap().get(ISSN_COLUMN);

	    if (issns == null) {
	      itemMetadata.getMapMap().put(ISSN_COLUMN,
		  new HashMap<String, String>());

	      itemMetadata.getMapMap().get(ISSN_COLUMN).put(type, issn);
	    } else {
	      issns.put(type, issn);
	    }
	  }
	}

	String isbn = resultSet.getString(ISBN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbn = " + isbn);

	if (!resultSet.wasNull()) {
	  String type = resultSet.getString(ISBN_TYPE_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "type = " + type);

	  if (!resultSet.wasNull()) {
	    Map<String, String> isbns =
		itemMetadata.getMapMap().get(ISBN_COLUMN);

	    if (isbns == null) {
	      itemMetadata.getMapMap().put(ISBN_COLUMN,
		  new HashMap<String, String>());

	      itemMetadata.getMapMap().get(ISBN_COLUMN).put(type, isbn);
	    } else {
	      isbns.put(type, isbn);
	    }
	  }
	}

	String proprietaryId = resultSet.getString(PROPRIETARY_ID_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "proprietaryId = " + proprietaryId);

	if (!resultSet.wasNull()) {
	  Set<String> pis = itemMetadata.getSetMap().get(PROPRIETARY_ID_COLUMN);

	  if (pis == null) {
	    itemMetadata.getSetMap().put(PROPRIETARY_ID_COLUMN,
		new HashSet<String>());

	    itemMetadata.getSetMap().get(PROPRIETARY_ID_COLUMN)
	    .add(proprietaryId);
	  } else {
	    if (!pis.contains(proprietaryId)) {
	      pis.add(proprietaryId);
	    }
	  }
	}

	String url = resultSet.getString(URL_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "url = " + url);

	if (!resultSet.wasNull()) {
	  String feature = resultSet.getString(FEATURE_COLUMN);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "feature = " + feature);

	  if (!resultSet.wasNull()) {
	    Map<String, String> urls =
		itemMetadata.getMapMap().get(URL_COLUMN);

	    if (urls == null) {
	      itemMetadata.getMapMap().put(URL_COLUMN,
		  new HashMap<String, String>());

	      itemMetadata.getMapMap().get(URL_COLUMN).put(feature, url);
	    } else {
	      urls.put(feature, url);
	    }
	  }
	}

	String author = resultSet.getString(AUTHOR_NAME_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "author = " + author);

	if (!resultSet.wasNull()) {
	  List<String> authors =
	      itemMetadata.getListMap().get(AUTHOR_NAME_COLUMN);

	  if (authors == null) {
	    itemMetadata.getListMap().put(AUTHOR_NAME_COLUMN,
		new ArrayList<String>());

	    itemMetadata.getListMap().get(AUTHOR_NAME_COLUMN).add(author);
	  } else {
	    if (!authors.contains(author)) {
	      authors.add(author);
	    }
	  }
	}

	String keyword = resultSet.getString(KEYWORD_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "keyword = " + keyword);

	if (!resultSet.wasNull()) {
	  Set<String> keywords = itemMetadata.getSetMap().get(KEYWORD_COLUMN);

	  if (keywords == null) {
	    itemMetadata.getSetMap().put(KEYWORD_COLUMN, new HashSet<String>());
	    itemMetadata.getSetMap().get(KEYWORD_COLUMN).add(keyword);
	  } else {
	    if (!keywords.contains(keyword)) {
	      keywords.add(keyword);
	    }
	  }
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get AU metadata";
      log.error(message, sqle);
      log.error("auId = '" + auId + "'.");
      log.error("auExtractionTime = " + auExtractionTime);
      log.error("limit = " + limit);
      log.error("lastItemMdItemSeq = " + lastItemMdItemSeq);
      log.error("SQL = '" + sql + "'.");
      log.error("pluginId = '" + pluginId + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getScalarMetadata);
    }

    // Check whether there are additional items after this page.
    if (hasMore) {
      // Yes: Build and save the response continuation token.
      ItemMetadataContinuationToken continuationToken =
	  new ItemMetadataContinuationToken(auExtractionTime,
	      items.get(items.size()-1).getId());
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "continuationToken = " + continuationToken);

      result.setContinuationToken(continuationToken);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
    return result;
  }
}
