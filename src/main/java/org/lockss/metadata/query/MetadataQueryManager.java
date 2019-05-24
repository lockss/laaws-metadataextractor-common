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

import static org.lockss.metadata.SqlConstants.*;
import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lockss.app.BaseLockssManager;
import org.lockss.app.ConfigurableManager;
import org.lockss.config.Configuration;
import org.lockss.config.Configuration.Differences;
import org.lockss.db.DbException;
import org.lockss.extractor.ArticleMetadata;
import org.lockss.extractor.MetadataField;
import org.lockss.log.L4JLogger;
import org.lockss.metadata.ArticleMetadataBuffer;
import org.lockss.metadata.ArticleMetadataBuffer.ArticleMetadataInfo;
import org.lockss.metadata.AuMetadataRecorder;
import org.lockss.metadata.ItemMetadata;
import org.lockss.metadata.ItemMetadataContinuationToken;
import org.lockss.metadata.ItemMetadataPage;
import org.lockss.metadata.MetadataConstants;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.metadata.MetadataManager;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuUtil;
import org.lockss.plugin.Plugin;
import org.lockss.util.StringUtil;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.time.TimeBase;

/**
 * Implementation of a manager for querying Archival Unit metadata.
 */
public class MetadataQueryManager extends BaseLockssManager implements
    ConfigurableManager {
  private static final L4JLogger log = L4JLogger.getLogger();

  /**
   * Prefix for configuration properties.
   */
  public static final String PREFIX = MetadataConstants.PREFIX;

  /**
   * Mandatory metadata fields.
   */
  static final String PARAM_MANDATORY_FIELDS = PREFIX + "mandatoryFields";
  static final List<String> DEFAULT_MANDATORY_FIELDS = null;

  // The database manager.
  private MetadataDbManager dbManager = null;

  private MetadataManager mdManager;

  // The metadata query manager SQL code executor.
  private MetadataQueryManagerSql mdqManagerSql;

  private List<String> mandatoryMetadataFields = DEFAULT_MANDATORY_FIELDS;

  /**
   * No-argument constructor.
   */
  public MetadataQueryManager() {
  }

  /**
   * Constructor used for generating a testing database.
   *
   * @param dbManager
   *          A MetadataDbManager with the database manager to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public MetadataQueryManager(MetadataDbManager dbManager) throws DbException {
    this.dbManager = dbManager;
    mdManager = new MetadataManager(dbManager);
    mdqManagerSql = new MetadataQueryManagerSql(dbManager);
  }

  /**
   * Starts the MetadataExtractorManager service.
   */
  @Override
  public void startService() {
    super.startService();
    log.debug("Starting MetadataQueryManager");

    dbManager = getManagerByType(MetadataDbManager.class);
    mdManager = getManagerByType(MetadataManager.class);
    try {
      mdqManagerSql = new MetadataQueryManagerSql(dbManager);
    } catch (DbException dbe) {
      log.error("Cannot obtain MetadataManagerSql", dbe);
      return;
    }

    resetConfig();
    log.debug("MetadataQueryManager service successfully started");
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
      if (changedKeys.contains(PARAM_MANDATORY_FIELDS)) {
	mandatoryMetadataFields =
	    (List<String>)config.getList(PARAM_MANDATORY_FIELDS,
		DEFAULT_MANDATORY_FIELDS);

	log.trace("mandatoryMetadataFields = {}", mandatoryMetadataFields);
      }
    }
  }

  /**
   * Provides an indication of whether a metadata set corresponds to a book
   * series.
   * 
   * @param pIssn
   *          A String with the print ISSN in the metadata.
   * @param eIssn
   *          A String with the electronic ISSN in the metadata.
   * @param pIsbn
   *          A String with the print ISBN in the metadata.
   * @param eIsbn
   *          A String with the electronic ISBN in the metadata.
   * @param seriesName
   *          A String with the series name in the metadata.
   * @param volume
   *          A String with the volume in the metadata.
   * @return <code>true</code> if the metadata set corresponds to a book series,
   *         <code>false</code> otherwise.
   */
  static public boolean isBookSeries(String pIssn, String eIssn, String pIsbn,
      String eIsbn, String seriesName, String volume) {
    boolean isBookSeries = MetadataManager.isBook(pIsbn, eIsbn)
        && (!StringUtil.isNullString(seriesName) 
            || !StringUtil.isNullString(volume)
            || !StringUtil.isNullString(pIssn)
            || !StringUtil.isNullString(eIssn));

    log.debug2("isBookSeries = {}", isBookSeries);
    return isBookSeries;
  }

  /**
   * Utility method to provide the database manager.
   * 
   * @return a DbManager with the database manager.
   */
  public MetadataDbManager getDbManager() {
    return dbManager;
  }

  /**
   * Utility method to provide the metadata manager.
   * 
   * @return a MetadataManager with the metadata manager.
   */
  public MetadataManager getMetadataManager() {
    return mdManager;
  }

  /**
   * Provides the list of metadata fields that are mandatory.
   *
   * @return a List<String> with the metadata fields that are mandatory.
   */
  private List<String> getMandatoryMetadataFields() {
    return mandatoryMetadataFields;
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
  public void addMdItemUrls(Connection conn, Long mdItemSeq, String accessUrl,
      Map<String, String> featuredUrlMap) throws DbException {
    if (!StringUtil.isNullString(accessUrl)) {
      // Add the access URL.
      mdManager.addMdItemUrl(conn, mdItemSeq,
	  MetadataManager.ACCESS_URL_FEATURE, accessUrl);
      log.trace("Added feature = {}, URL = {}",
	  MetadataManager.ACCESS_URL_FEATURE, accessUrl);
    }

    // Loop through all the featured URLs.
    for (String feature : featuredUrlMap.keySet()) {
      // Add the featured URL.
      mdManager.addMdItemUrl(conn, mdItemSeq, feature,
	  featuredUrlMap.get(feature));
      log.trace("Added feature = {}, URL = {}", feature,
	  featuredUrlMap.get(feature));
    }
  }

  /**
   * Provides the metadata query manager SQL code executor.
   * 
   * @return a MetadataQueryManagerSql with the SQL code executor.
   */
  public MetadataQueryManagerSql getMetadataQueryManagerSql() {
    return mdqManagerSql;
  }

  /**
   * Provides the full metadata stored for an AU given the AU identifier or a
   * pageful of the metadata defined by the continuation token and size.

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
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public ItemMetadataPage getAuMetadataDetail(String auId, Integer limit,
      ItemMetadataContinuationToken continuationToken) throws DbException {
    log.debug2("auId = {}", auId);
    log.debug2("limit = {}", limit);
    log.debug2("continuationToken = {}", continuationToken);

    return mdqManagerSql.getAuMetadataDetail(auId, limit, continuationToken);
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
  private Long storeAuItemMetadata(ItemMetadata item, ArchivalUnit au,
      Plugin plugin, String auId, long creationTime) throws Exception {
    log.debug2("item = {}", item);
    log.debug2("auId = {}", auId);

    Long mdItemSeq = null;
    Connection conn = null;

    ArticleMetadataBuffer articleMetadataInfoBuffer = null;
    try {
      articleMetadataInfoBuffer =
	  new ArticleMetadataBuffer(new File(PlatformUtil.getSystemTempDir()));

      ArticleMetadata md = populateArticleMetadata(item);
      log.trace("md = {}", md);

      articleMetadataInfoBuffer.add(md);

      Iterator<ArticleMetadataInfo> mditr =
          articleMetadataInfoBuffer.iterator();

      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the mandatory metadata fields.
      List<String> mandatoryFields = getMandatoryMetadataFields();
      log.trace("mandatoryFields = {}", mandatoryFields);

      // Write the AU metadata to the database.
      mdItemSeq = new AuMetadataRecorder(null, this, null, au, plugin, auId)
	  .recordMetadataItem(conn, mandatoryFields, mditr, creationTime);

      // Complete the database transaction.
      MetadataDbManager.commitOrRollback(conn, log);
    } catch (Exception e) {
      log.error("Error storing AU item metadata", e);
      log.error("item = {}", item);
      throw e;
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
      articleMetadataInfoBuffer.close();
    }

    log.debug2("mdItemSeq = {}", mdItemSeq);
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
   * Stores in the database the metadata for an item belonging to an AU.
   * Used for generating a testing database.
   * 
   * @param item
   *          An ItemMetadata with the AU item metadata.
   * @param au
   *          An ArchivalUnit with the AU to be written to the database.
   * @return a Long with the database identifier of the metadata item.
   * @throws Exception
   *           if any problem occurred.
   */
  public Long storeAuItemMetadataForTesting(ItemMetadata item, ArchivalUnit au)
      throws Exception {
    log.debug2("item = {}", item);

    Long mdItemSeq = storeAuItemMetadata(item, au, au.getPlugin(), au.getAuId(),
	AuUtil.getAuCreationTime(au));
    updateAuLastExtractionTime(au.getAuId());

    log.debug2("mdItemSeq = {}", mdItemSeq);
    return mdItemSeq;
 }

  /**
   * Updates the timestamp of the last extraction of an Archival Unit metadata.
   * Used for testing.
   * @param au
   *          The ArchivalUnit whose time to update.
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auMdSeq
   *          A Long with the identifier of the Archival Unit metadata.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void updateAuLastExtractionTime(String auId) throws DbException {
    log.debug2("auId = {}", auId);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      Long auMdSeq = mdqManagerSql.findAuMdByAuId(conn, auId);
      log.trace("auMdSeq = {}", auMdSeq);

      long now = TimeBase.nowMs();
      log.trace("now = {}", now);

      mdqManagerSql.updateAuLastExtractionTime(conn, auMdSeq, now);

      // Complete the database transaction.
      MetadataDbManager.commitOrRollback(conn, log);
    } catch (Exception e) {
      log.error("Error updating AU extraction time", e);
      log.error("auId = {}", auId);
      throw e;
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
    }
  }
}
