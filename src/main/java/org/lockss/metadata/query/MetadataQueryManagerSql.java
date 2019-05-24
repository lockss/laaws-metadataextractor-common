/*

Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.metadata.query;

import static org.lockss.metadata.MetadataConstants.*;
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
import org.lockss.log.L4JLogger;
import org.lockss.metadata.ItemMetadata;
import org.lockss.metadata.ItemMetadataContinuationToken;
import org.lockss.metadata.ItemMetadataPage;
import org.lockss.plugin.PluginManager;

/**
 * The MetadataQueryManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class MetadataQueryManagerSql {
  private static final L4JLogger log = L4JLogger.getLogger();

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

  // Query to find the extraction time of an Archival Unit.
  private static final String FIND_AU_MD_EXTRACT_TIME_BY_AUSEQ_QUERY = "select "
      + EXTRACT_TIME_COLUMN
      + " from " + AU_MD_TABLE
      + " where " + AU_SEQ_COLUMN + " = ?";

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

  /**
   * Constructor.
   * 
   * @param dbManager
   *          A DbManager with the database manager.
   */
  MetadataQueryManagerSql(DbManager dbManager) throws DbException {
    this.dbManager = dbManager;
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
    log.debug2("auId = {}", auId);

    String pluginId = null;
    String auKey = null;
    Long auMdSeq = null;
    PreparedStatement findAuMd =
	dbManager.prepareStatement(conn, FIND_AU_MD_BY_AU_ID_QUERY);
    ResultSet resultSet = null;

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      log.trace("pluginId() = {}", pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      log.trace("auKey = {}", auKey);

      findAuMd.setString(1, pluginId);
      findAuMd.setString(2, auKey);
      resultSet = dbManager.executeQuery(findAuMd);

      if (resultSet.next()) {
	auMdSeq = resultSet.getLong(AU_MD_SEQ_COLUMN);
	log.trace("auMdSeq = {}", auMdSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU metadata identifier";
      log.error(message, sqle);
      log.error("auId = {}", auId);
      log.error("SQL = '{}'", FIND_AU_MD_BY_AU_ID_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAuMd);
    }

    log.debug2("auMdSeq = {}", auMdSeq);
    return auMdSeq;
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
    log.debug2("auId = {}", auId);

    String pluginId = PluginManager.pluginIdFromAuId(auId);
    String auKey = PluginManager.auKeyFromAuId(auId);
    Long auSeq = null;
    PreparedStatement findAu =
	dbManager.prepareStatement(conn, FIND_AU_BY_AU_ID_QUERY);
    ResultSet resultSet = null;

    try {
      pluginId = PluginManager.pluginIdFromAuId(auId);
      log.trace("pluginId = {}", pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      log.trace("auKey = {}", auKey);

      findAu.setString(1, pluginId);
      findAu.setString(2, auKey);
      resultSet = dbManager.executeQuery(findAu);

      if (resultSet.next()) {
	auSeq = resultSet.getLong(AU_SEQ_COLUMN);
	log.trace("auSeq = {}", auSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU identifier";
      log.error(message, sqle);
      log.error("auId = {}", auId);
      log.error("SQL = '{}'", FIND_AU_BY_AU_ID_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAu);
    }

    log.debug2("auSeq = {}", auSeq);
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
    log.debug2("auMdSeq = {}", auMdSeq);
    log.debug2("now = {}", now);

    PreparedStatement updateAuLastExtractionTime =
	dbManager.prepareStatement(conn, UPDATE_AU_MD_EXTRACT_TIME_QUERY);

    try {
      updateAuLastExtractionTime.setLong(1, now);
      updateAuLastExtractionTime.setLong(2, auMdSeq);
      dbManager.executeUpdate(updateAuLastExtractionTime);
    } catch (SQLException sqle) {
      String message = "Cannot update the AU extraction time";
      log.error(message, sqle);
      log.error("SQL = '{}'", UPDATE_AU_MD_EXTRACT_TIME_QUERY);
      log.error("auMdSeq = {}", auMdSeq);
      log.error("now = {}", now);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(updateAuLastExtractionTime);
    }

    log.debug2("Done.");
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
    log.debug2("auSeq = {}", auSeq);

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
	log.trace("timestamp = {}", timestamp);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get AU extraction time";
      log.error(message, sqle);
      log.error("SQL = '{}'", FIND_AU_MD_EXTRACT_TIME_BY_AUSEQ_QUERY);
      log.error("auSeq = {}", auSeq);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(selectLastExtractionTime);
    }

    log.debug2("timestamp = {}", timestamp);
    return timestamp;
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
  public void addMdItemAuthors(Connection conn, Long mdItemSeq,
      Collection<String> authors) throws DbException {
    log.debug2("mdItemSeq = {}", mdItemSeq);
    log.debug2("authors = {}", authors);

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
	log.trace("count = {}", count);
	log.trace("Added author = {}", author);
      }
    } catch (SQLException sqle) {
      String message = "Cannot add metadata item authors";
      log.error(message, sqle);
      log.error("SQL = '{}'", sql);
      log.error("mdItemSeq = {}", mdItemSeq);
      log.error("authors = {}", authors);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemAuthor);
    }

    log.debug2("Done.");
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
  public void addMdItemKeywords(Connection conn, Long mdItemSeq,
      Collection<String> keywords) throws DbException {
    log.debug2("mdItemSeq = {}", mdItemSeq);
    log.debug2("keywords = {}", keywords);

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
	log.trace("count = {}", count);
	log.trace("Added keyword = {}", keyword);
      }
    } catch (SQLException sqle) {
      String message = "Cannot add metadata item keywords";
      log.error(message, sqle);
      log.error("SQL = '{}'", INSERT_KEYWORD_QUERY);
      log.error("mdItemSeq = {}", mdItemSeq);
      log.error("keywords = {}", keywords);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemKeyword);
    }

    log.debug2("Done.");
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
  public boolean updateAuUnknownProvider(Connection conn, Long auMdSeq,
      Long providerSeq) throws DbException {
    log.debug2("auMdSeq = {}", auMdSeq);
    log.debug2("providerSeq = {}", providerSeq);

    int updatedCount = -1;
    PreparedStatement updateUnknownProvider =
	dbManager.prepareStatement(conn, UPDATE_AU_MD_UNKNOWN_PROVIDER_QUERY);

    try {
      updateUnknownProvider.setLong(1, providerSeq);
      updateUnknownProvider.setLong(2, auMdSeq);
      updatedCount = dbManager.executeUpdate(updateUnknownProvider);
      log.trace("updatedCount = {}", updatedCount);
    } catch (SQLException sqle) {
      String message = "Cannot update unknown provider";
      log.error(message, sqle);
      log.error("auMdSeq = {}", auMdSeq);
      log.error("providerSeq = {}", providerSeq);
      log.error("SQL = '{}'", UPDATE_AU_MD_UNKNOWN_PROVIDER_QUERY);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(updateUnknownProvider);
    }

    log.debug2("result = {}", (updatedCount > 0));
    return updatedCount > 0;
  }

  /**
   * Provides the full metadata stored for an AU given the AU identifier or a
   * pageful of the metadata defined by the continuation token and size.
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
    log.debug2("auId = {}", auId);
    log.debug2("limit = {}", limit);
    log.debug2("continuationToken = {}", continuationToken);

    ItemMetadataPage result = new ItemMetadataPage();
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      Long auSeq = findAuByAuId(conn, auId);
      log.trace("auSeq = {}", auSeq);

      if (auSeq == null) {
	throw new IllegalArgumentException("AuId not found in DB: " + auId);
      }

      // Get the last extraction time of the Archival Unit.
      long extractionTime = getAuExtractionTime(conn, auSeq);
      log.trace("extractionTime = {}", extractionTime);

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
	log.warn("Incompatible pagination request: request timestamp: {},"
	    + " current timestamp: {}",
	    continuationToken.getAuExtractionTimestamp(), extractionTime);
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

    log.debug2("Done.");
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
    log.debug2("auId = {}", auId);
    log.debug2("auExtractionTime = {}", auExtractionTime);
    log.debug2("limit = {}", limit);
    log.debug2("lastItemMdItemSeq = {}", lastItemMdItemSeq);

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
      log.trace("pluginId() = {}", pluginId);

      auKey = PluginManager.auKeyFromAuId(auId);
      log.trace("auKey = {}", auKey);

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
	log.trace("mdItemSeq = {}", mdItemSeq);

	if (!mdItemSeq.equals(previousMdItemSeq)) {
	  itemMetadata = new ItemMetadata(mdItemSeq);
	  itemMetadata.setScalarMap(new HashMap<String, String>());
	  itemMetadata.setSetMap(new HashMap<String, Set<String>>());
	  itemMetadata.setListMap(new HashMap<String, List<String>>());
	  itemMetadata.setMapMap(new HashMap<String, Map<String, String>>());

	  Map<String, String> scalarMap = itemMetadata.getScalarMap();

	  scalarMap.put("au_id", auId);

	  String providerName = resultSet.getString(PROVIDER_NAME_COLUMN);
	  log.trace("providerName = {}", providerName);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(PROVIDER_NAME_COLUMN, providerName);
	  }

	  String publisherName = resultSet.getString(PUBLISHER_NAME_COLUMN);
	  log.trace("publisherName = {}", publisherName);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(PUBLISHER_NAME_COLUMN, publisherName);
	  }

	  String publicationName = resultSet.getString("publication_name");
	  log.trace("publicationName = {}", publicationName);

	  if (!resultSet.wasNull()) {
	    scalarMap.put("publication_name", publicationName);
	  }

	  String date = resultSet.getString(DATE_COLUMN);
	  log.trace("date = {}", date);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(DATE_COLUMN, date);
	  }

	  String coverage = resultSet.getString(COVERAGE_COLUMN);
	  log.trace("coverage = {}", coverage);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(COVERAGE_COLUMN, coverage);
	  }

	  Long fetchTime = resultSet.getLong(FETCH_TIME_COLUMN);
	  log.trace("fetchTime = {}", fetchTime);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(FETCH_TIME_COLUMN, fetchTime.toString());
	  }

	  String itemTitle = resultSet.getString("item_title");
	  log.trace("itemTitle = {}", itemTitle);

	  if (!resultSet.wasNull()) {
	    scalarMap.put("item_title", itemTitle);
	  }

	  String volume = resultSet.getString(VOLUME_COLUMN);
	  log.trace("volume = {}", volume);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(VOLUME_COLUMN, volume);
	  }

	  String issue = resultSet.getString(ISSUE_COLUMN);
	  log.trace("issue = {}", issue);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(ISSUE_COLUMN, issue);
	  }

	  String startPage = resultSet.getString(START_PAGE_COLUMN);
	  log.trace("startPage = {}", startPage);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(START_PAGE_COLUMN, startPage);
	  }

	  String endPage = resultSet.getString(END_PAGE_COLUMN);
	  log.trace("endPage = {}", endPage);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(END_PAGE_COLUMN, endPage);
	  }

	  String itemNo = resultSet.getString(ITEM_NO_COLUMN);
	  log.trace("itemNo = {}", itemNo);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(ITEM_NO_COLUMN, itemNo);
	  }

	  String doi = resultSet.getString(DOI_COLUMN);
	  log.trace("doi = {}", doi);

	  if (!resultSet.wasNull()) {
	    scalarMap.put(DOI_COLUMN, doi);
	  }

	  log.trace("itemMetadata = {}", itemMetadata);

	  items.add(itemMetadata);
	  itemMap.put(mdItemSeq, itemMetadata);

	  previousMdItemSeq = mdItemSeq;
	} else {
	  log.error("Ignoring unexpected multiple scalar results for "
	      + "mdItemSeq = {}: Existing result: {}", mdItemSeq, itemMetadata);
	}
      }

      resultSet.close();

      // Check whether no metadata was found for the given Archival Unit.
      if (items.size() == 0) {
	// Yes: Done.
	log.debug2("result = {}", result);
	return result;
      }

      // No: Get the repeatable metadata.
      StringBuilder inIds = new StringBuilder("(");
      boolean isFirst = true;

      for (Long id : itemMap.keySet()) {
	log.trace("id = {}", id);

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
      log.trace("sql = '{}'", sql);

      PreparedStatement getNonScalarMetadata =
	  dbManager.prepareStatement(conn, sql);

      resultSet = dbManager.executeQuery(getNonScalarMetadata);

      while (resultSet.next()) {
	Long mdItemSeq = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
	log.trace("mdItemSeq = {}", mdItemSeq);

	itemMetadata = itemMap.get(mdItemSeq);

	if (itemMetadata == null) {
	  log.error("Ignoring non-scalar results for mdItemSeq = {}"
	      + " with no previously retrieved scalar results.", mdItemSeq);
	  continue;
	}

	String issn = resultSet.getString(ISSN_COLUMN);
	log.trace("issn = {}", issn);

	if (!resultSet.wasNull()) {
	  String type = resultSet.getString(ISSN_TYPE_COLUMN);
	  log.trace("type = {}", type);

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
	log.trace("isbn = {}", isbn);

	if (!resultSet.wasNull()) {
	  String type = resultSet.getString(ISBN_TYPE_COLUMN);
	  log.trace("type = {}", type);

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
	log.trace("proprietaryId = {}", proprietaryId);

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
	log.trace("url = {}", url);

	if (!resultSet.wasNull()) {
	  String feature = resultSet.getString(FEATURE_COLUMN);
	  log.trace("feature = {}", feature);

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
	log.trace("author = {}", author);

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
	log.trace("keyword = {}", keyword);

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
      log.error("auId = '{}'", auId);
      log.error("auExtractionTime = {}", auExtractionTime);
      log.error("limit = {}", limit);
      log.error("lastItemMdItemSeq = {}", lastItemMdItemSeq);
      log.error("SQL = '{}'", sql);
      log.error("pluginId = '{}'", pluginId);
      log.error("auKey = '{}'", auKey);
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
      log.trace("continuationToken = {}", continuationToken);

      result.setContinuationToken(continuationToken);
    }

    log.debug2("Done.");
    return result;
  }
}
