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
package org.lockss.metadata;

import static org.lockss.metadata.SqlConstants.*;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.lockss.daemon.PublicationDate;
import org.lockss.extractor.ArticleMetadata;
import org.lockss.extractor.MetadataField;
import org.lockss.metadata.ItemMetadata;
import org.lockss.metadata.MetadataManager;
import org.lockss.metadata.query.MetadataQueryManager;
import org.lockss.util.CloseCallbackInputStream.DeleteFileOnCloseInputStream;
import org.lockss.util.FileUtil;
import org.lockss.util.IOUtil;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;

/**
 * This class is a buffer of ArticleMetadata records 
 * that will be stored in the database once the metadata
 * extraction process has completed.
 * 
 * @author Philip Gust
 *
 */
public class ArticleMetadataBuffer {
  private static Logger log = Logger.getLogger(ArticleMetadataBuffer.class);

  File collectedMetadataFile = null;
  ObjectOutputStream outstream = null;
  ObjectInputStream instream = null;
  long infoCount = 0;
  
  /**
   * This class encapsulates the values extracted from metadata
   * records that will be stored in the metadatabase once the
   * extraction process has completed.
   * 
   * @author Philip Gust
   *
   */
  static public class ArticleMetadataInfo implements Serializable {
    private static final long serialVersionUID = -2372571567706061080L;
    String publisher;
    String provider;
    String seriesTitle;
    String proprietarySeriesIdentifier;
    String publicationTitle;
    String publicationType;
    String isbn;
    String eisbn;
    String issn;
    String eissn;
    String volume;
    String issue;
    String startPage;
    String pubDate; 
    final String pubYear;
    String articleTitle;
    String articleType;
    Collection<String> authors;
    String doi;
    String accessUrl;
    Map<String, String> featuredUrlMap;
    Collection<String> keywords;
    String endPage;
    String coverage;
    String itemNumber;
    String proprietaryIdentifier;
    String fetchTime;
    Map<String, String> mdMap;

    /**
     * Extract the information from the ArticleMetadata
     * that will be stored in the database.
     * 
     * @param md the ArticleMetadata
     */
    public ArticleMetadataInfo(ArticleMetadata md) {
      publisher = md.get(MetadataField.FIELD_PUBLISHER);
      provider = md.get(MetadataField.FIELD_PROVIDER);
      seriesTitle = md.get(MetadataField.FIELD_SERIES_TITLE);
      proprietarySeriesIdentifier =
          md.get(MetadataField.FIELD_PROPRIETARY_SERIES_IDENTIFIER);
      publicationTitle = md.get(MetadataField.FIELD_PUBLICATION_TITLE);
      isbn = md.get(MetadataField.FIELD_ISBN);
      eisbn = md.get(MetadataField.FIELD_EISBN);
      issn = md.get(MetadataField.FIELD_ISSN);
      eissn = md.get(MetadataField.FIELD_EISSN);
      volume = md.get(MetadataField.FIELD_VOLUME);
      issue = md.get(MetadataField.FIELD_ISSUE);
      String allPages = md.get(MetadataField.FIELD_START_PAGE);
      if (allPages != null) {
        String[] pages = allPages.split("\\D");
        if (pages.length == 2) {
          startPage = pages[0];
          if (StringUtil.isNullString(md.get(MetadataField.FIELD_END_PAGE))) {
            endPage = pages[1];
          } else {
            endPage = md.get(MetadataField.FIELD_END_PAGE);
          }
        } else {
          startPage = allPages;
          endPage = md.get(MetadataField.FIELD_END_PAGE);
        }
      } else {
        startPage = null;
        endPage = md.get(MetadataField.FIELD_END_PAGE);
      }
      PublicationDate pd = getDateField(md);
      if (pd == null) {
        pubDate = pubYear = null;
      } else {
        pubDate = pd.toString();
        pubYear = Integer.toString(pd.getYear());
      }
      articleTitle = md.get(MetadataField.FIELD_ARTICLE_TITLE);
      authors = md.getList(MetadataField.FIELD_AUTHOR);
      doi = md.get(MetadataField.FIELD_DOI);
      accessUrl = md.get(MetadataField.FIELD_ACCESS_URL);
      featuredUrlMap = md.getRawMap(MetadataField.FIELD_FEATURED_URL_MAP);
      log.debug3("featuredUrlMap = " + featuredUrlMap);
      keywords = md.getList(MetadataField.FIELD_KEYWORDS);;
      coverage = md.get(MetadataField.FIELD_COVERAGE);
      itemNumber = md.get(MetadataField.FIELD_ITEM_NUMBER);
      proprietaryIdentifier =
          md.get(MetadataField.FIELD_PROPRIETARY_IDENTIFIER);
      fetchTime = md.get(MetadataField.FIELD_FETCH_TIME);
      mdMap = md.getRawMap(MetadataField.FIELD_MD_MAP);
      if (log.isDebug3()) log.debug3("mdMap = " + mdMap);

      // get publication type from metadata or infer it if not set
      publicationType = md.get(MetadataField.FIELD_PUBLICATION_TYPE);
      if (StringUtil.isNullString(publicationType)) {
        if (MetadataQueryManager.isBookSeries(
            issn, eissn, isbn, eisbn, seriesTitle, volume)) {
          // book series if e/isbn and either e/issn or volume fields present
          publicationType = MetadataField.PUBLICATION_TYPE_BOOKSERIES;
        } else if (MetadataManager.isBook(isbn, eisbn)) {
          // book if e/isbn present
          publicationType = MetadataField.PUBLICATION_TYPE_BOOK;
        } else {
          // journal if not book or bookSeries
          publicationType = MetadataField.PUBLICATION_TYPE_JOURNAL;
        }
      }
      
      // get article type from metadata or infer it if not set
      articleType = md.get(MetadataField.FIELD_ARTICLE_TYPE);
      if (StringUtil.isNullString(articleType)) {
        if (MetadataField.PUBLICATION_TYPE_BOOK.equals(publicationType)
            || MetadataField.PUBLICATION_TYPE_BOOKSERIES.equals(publicationType)
           ) {
          if (!StringUtil.isNullString(startPage)
              || !StringUtil.isNullString(endPage)
              || !StringUtil.isNullString(itemNumber)) {
            // assume book chapter if startPage, endPage, or itemNumber present
            articleType = MetadataField.ARTICLE_TYPE_BOOKCHAPTER;
          } else {
            // assume book volume if none of these fields are present
            articleType = MetadataField.ARTICLE_TYPE_BOOKVOLUME;
          }
        } else if (MetadataField.PUBLICATION_TYPE_JOURNAL.
            equals(publicationType)) {
          // assume article for journal
          articleType = MetadataField.ARTICLE_TYPE_JOURNALARTICLE;          
        } else if (MetadataField.PUBLICATION_TYPE_PROCEEDINGS.
            equals(publicationType)) {
          // Assume article for proceedings publication.
          articleType = MetadataField.ARTICLE_TYPE_PROCEEDINGSARTICLE;          
        } else if (MetadataField.PUBLICATION_TYPE_FILE.
            equals(publicationType)) {
          // Assume article for file publication.
          articleType = MetadataField.ARTICLE_TYPE_FILE;          
        }
      } else if (MetadataField.ARTICLE_TYPE_FILE.equals(articleType)
	  && StringUtil.isNullString(publicationType)) {
        publicationType = MetadataField.PUBLICATION_TYPE_FILE;
      }

      if (StringUtil.isNullString(publicationTitle)
	  && MetadataField.PUBLICATION_TYPE_FILE.equals(publicationType)
	  && StringUtil.isNullString(publisher)) {
	publicationTitle = "File from " + publisher;
      }
    }

    /**
     * Provides the name of the publisher.
     * 
     * @return a String with the name of the publisher.
     */
    public String getPublisher() {
      return publisher;
    }

    /**
     * Provides the name of the provider.
     * 
     * @return a String with the name of the provider.
     */
    public String getProvider() {
      return provider;
    }

    /**
     * Provides the title of the book series.
     * 
     * @return a String with the title of the book series.
     */
    public String getSeriesTitle() {
      return seriesTitle;
    }

    /**
     * Provides the proprietary identifier of the book series.
     * 
     * @return a String with the proprietary identifier of the book series.
     */
    public String getProprietarySeriesIdentifier() {
      return proprietarySeriesIdentifier;
    }

    /**
     * Provides the title of the publication.
     * 
     * @return a String with the title of the publication.
     */
    public String getPublicationTitle() {
      return publicationTitle;
    }

    /**
     * Provides the type of the publication.
     * 
     * @return a String with the type of the publication.
     */
    public String getPublicationType() {
      return publicationType;
    }

    /**
     * Provides the print ISBN.
     * 
     * @return a String with the print ISBN.
     */
    public String getIsbn() {
      return isbn;
    }

    /**
     * Provides the online ISBN.
     * 
     * @return a String with the online ISBN.
     */
    public String getEisbn() {
      return eisbn;
    }

    /**
     * Provides the print ISSN.
     * 
     * @return a String with the print ISSN.
     */
    public String getIssn() {
      return issn;
    }

    /**
     * Provides the online ISSN.
     * 
     * @return a String with the online ISSN.
     */
    public String getEissn() {
      return eissn;
    }

    /**
     * Provides the bibliographic volume.
     * 
     * @return a String with the bibliographic volume.
     */
    public String getVolume() {
      return volume;
    }

    /**
     * Provides the bibliographic issue.
     * 
     * @return a String with the bibliographic issue.
     */
    public String getIssue() {
      return issue;
    }

    /**
     * Provides the bibliographic starting page.
     * 
     * @return a String with the bibliographic starting page.
     */
    public String getStartPage() {
      return startPage;
    }

    /**
     * Provides the publication date.
     * 
     * @return a String with the publication date.
     */
    public String getPubDate() {
      return pubDate;
    }

    /**
     * Provides the title of the article.
     * 
     * @return a String with the title of the article.
     */
    public String getArticleTitle() {
      return articleTitle;
    }

    /**
     * Provides the type of the article.
     * 
     * @return a String with the type of the article.
     */
    public String getArticleType() {
      return articleType;
    }

    /**
     * Provides the names of the authors.
     * 
     * @return a Collection<String> with the names of the authors.
     */
    public Collection<String> getAuthors() {
      return authors;
    }

    /**
     * Provides the DOI.
     * 
     * @return a String with the DOI.
     */
    public String getDoi() {
      return doi;
    }

    /**
     * Provides the access URL.
     * 
     * @return a String with the access URL.
     */
    public String getAccessUrl() {
      return accessUrl;
    }

    /**
     * Provides the map of URLs keyed by feature.
     * 
     * @return a Map<String, String> with the map of URLs keyed by feature.
     */
    public Map<String, String> getFeaturedUrlMap() {
      return featuredUrlMap;
    }

    /**
     * Provides the keywords.
     * 
     * @return a Collection<String> with the keywords.
     */
    public Collection<String> getKeywords() {
      return keywords;
    }

    /**
     * Provides the bibliographic ending page.
     * 
     * @return a String with the bibliographic ending page.
     */
    public String getEndPage() {
      return endPage;
    }

    /**
     * Provides the coverage.
     * 
     * @return a String with the coverage.
     */
    public String getCoverage() {
      return coverage;
    }

    /**
     * Provides the bibliographic item number.
     * 
     * @return a String with the bibliographic item number.
     */
    public String getItemNumber() {
      return itemNumber;
    }

    /**
     * Provides the proprietary identifier.
     * 
     * @return a String with the proprietary identifier.
     */
    public String getProprietaryIdentifier() {
      return proprietaryIdentifier;
    }

    /**
     * Provides the fetch time.
     * 
     * @return a String with the fetch time.
     */
    public String getFetchTime() {
      return fetchTime;
    }

    /**
     * Provides the metadata map.
     * 
     * @return a Map<String, String> with the metadata map.
     */
    public Map<String, String> getMdMap() {
      return mdMap;
    }

    /**
     * Provides the publication year.
     * 
     * @return a String with the publication year.
     */
    public String getPubYear() {
      return pubYear;
    }

    /**
     * Return the date field to store in the database. The date field can be
     * nearly anything a MetaData extractor chooses to provide, making it a near
     * certainty that this method will be unable to parse it, even with the help
     * of locale information.
     * 
     * @param md the ArticleMetadata
     * @return the publication date or <code>null</code> if none specified 
     *    or one cannot be parsed from the metadata information
     */
    static private PublicationDate getDateField(ArticleMetadata md) {
      PublicationDate pubDate = null;
      String dateStr = md.get(MetadataField.FIELD_DATE);
      if (dateStr != null) {
        Locale locale = md.getLocale();
        if (locale == null) {
          locale = Locale.getDefault();
        }
        try {
          pubDate = new PublicationDate(dateStr, locale);
        } catch (ParseException ex) {}
      }
      return pubDate;
    }

    /**
     * Populates an ItemMetadata with data provided in an ArticleMetadataInfo
     * object.
     * 
     * @return an ItemMetadata populated with the source data.
     */
    public ItemMetadata populateItemMetadataDetail() {
      final String DEBUG_HEADER = "populateItemMetadataDetail(): ";

      ItemMetadata item = new ItemMetadata();

      Map<String, String> scalarMap = item.getScalarMap();
      Map<String, Set<String>> setMap = item.getSetMap();
      Map<String, List<String>> listMap = item.getListMap();
      Map<String, Map<String, String>> mapMap = item.getMapMap();

      if (publisher != null) {
        scalarMap.put(PUBLISHER_NAME_COLUMN, publisher);
      }

      if (provider != null) {
        scalarMap.put(PROVIDER_NAME_COLUMN, provider);
      }

      if (seriesTitle != null) {
        scalarMap.put("series_title_name", seriesTitle);
      }

      if (proprietarySeriesIdentifier != null) {
        scalarMap.put("proprietary_series_identifier",
  	  proprietarySeriesIdentifier);
      }

      if (publicationTitle != null) {
        scalarMap.put("publication_name", publicationTitle);
      }

      Map<String, String> isbnMap = new HashMap<String, String>();

      if (isbn != null) {
        isbnMap.put(P_ISBN_TYPE, isbn);
      }

      if (eisbn != null) {
        isbnMap.put(E_ISBN_TYPE, eisbn);
      }

      if (isbnMap.size() > 0) {
        mapMap.put(ISBN_COLUMN, isbnMap);
      }

      Map<String, String> issnMap = new HashMap<String, String>();

      if (issn != null) {
        issnMap.put(P_ISSN_TYPE, issn);
      }

      if (eissn != null) {
        issnMap.put(E_ISSN_TYPE, eissn);
      }

      if (issnMap.size() > 0) {
        mapMap.put(ISSN_COLUMN, issnMap);
      }

      if (volume != null) {
        scalarMap.put(VOLUME_COLUMN, volume);
      }

      if (issue != null) {
        scalarMap.put(ISSUE_COLUMN, issue);
      }

      if (startPage != null) {
        scalarMap.put(START_PAGE_COLUMN, startPage);
      }

      if (endPage != null) {
        scalarMap.put(END_PAGE_COLUMN, endPage);
      }

      if (pubDate != null) {
        scalarMap.put(DATE_COLUMN, pubDate);
      }

      if (articleTitle != null) {
        scalarMap.put("item_title", articleTitle);
      }

      if (authors != null && authors.size() > 0) {
        listMap.put(AUTHOR_NAME_COLUMN, new ArrayList<String>(authors));
      }

      if (doi != null) {
        scalarMap.put(DOI_COLUMN, doi);
      }

      Map<String, String> urlMap = featuredUrlMap;

      if (accessUrl != null) {
        urlMap.put("Access", accessUrl);
      }

      if (urlMap.size() > 0) {
        mapMap.put(URL_COLUMN, urlMap);
      }

      if (keywords != null && keywords.size() > 0) {
        setMap.put(KEYWORD_COLUMN, new HashSet<String>(keywords));
      }

      if (coverage != null) {
        scalarMap.put(COVERAGE_COLUMN, coverage);
      }

      if (itemNumber != null) {
        scalarMap.put(ITEM_NO_COLUMN, itemNumber);
      }

      if (proprietaryIdentifier != null) {
        Set<String> pis = new HashSet<String>();
        setMap.put(PROPRIETARY_ID_COLUMN, pis);
        pis.add(proprietaryIdentifier);
      }

      if (fetchTime != null) {
        scalarMap.put(FETCH_TIME_COLUMN, fetchTime);
      }

      Map<String, String> metadataMap = mdMap;

      if (metadataMap.size() > 0) {
        mapMap.put(MD_VALUE_COLUMN, metadataMap);
      }

      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "item = " + item);
      return item;
    }

    /**
     * Provides a printable version of this object.
     * 
     * @return a String with the printable version of the object.
     */
    @Override
    public String toString() {
      return "[ArticleMetadataInfo "
          + "publisher=" + publisher
          + ", provider=" + provider
          + ", seriesTitle=" + seriesTitle
          + ", proprietarySeriesIdentifier=" + proprietarySeriesIdentifier
          + ", publicationTitle=" + publicationTitle
          + ", publicationType=" + publicationType
          + ", isbn=" + isbn
          + ", eisbn=" + eisbn
          + ", issn=" + issn
          + ", eissn=" + eissn
          + ", volume=" + volume
          + ", issue=" + issue
          + ", startPage=" + startPage
          + ", pubDate=" + pubDate
          + ", pubYear=" + pubYear
          + ", articleTitle=" + articleTitle
          + ", articleType=" + articleType
          + ", authorSet=" + authors
          + ", doi="+ doi
          + ", accessUrl=" + accessUrl
          + ", featuredUrlMap=" + featuredUrlMap
          + ", keywordSet=" + keywords
          + ", endPage=" + endPage
          + ", coverage=" + coverage
          + ", itemNumber=" + itemNumber
          + ", proprietaryIdentifier=" + proprietaryIdentifier
          + ", fetchTime=" + fetchTime
          + ", mdMap=" + mdMap + "]";
    }
  }

  /**
   * Constructor.
   * 
   * @param tmpdir
   *          A File withe the temporary directory.
   * @throws IOException
   *           if there are problems.
   */
  public ArticleMetadataBuffer(File tmpdir) throws IOException {
    collectedMetadataFile = 
      FileUtil.createTempFile("MetadataManager", "md", tmpdir);
    outstream =
        new ObjectOutputStream(
            new BufferedOutputStream(
                new FileOutputStream(collectedMetadataFile)));
  }

  /**
   * Add the information from the specified metadata record to the buffer.
   * 
   * @param md
   *          A ArticleMetadata with the metadata record.
   * @throws IOException
   *           if no more items can be added because the iterator has already
   *           been obtained or because of other problems.
   */
  public void add(ArticleMetadata md) throws IOException {
    if (outstream == null) {
      throw new IllegalStateException("collectedMetadataOutputStream closed");
    }
    ArticleMetadataInfo mdinfo = new ArticleMetadataInfo(md);
    outstream.writeObject(mdinfo);
    infoCount++;
  }

  /**
   * Return an iterator to the buffered metadata information.
   * Only one iterator per buffer is available.
   * 
   * @return an iterator to the buffered metadata information
   * @throws IllegalStateException if the iterator cannot be obtained
   *   because the buffer is closed
   */
  public Iterator<ArticleMetadataInfo> iterator() {
    if (!isOpen()) {
      throw new IllegalStateException("Buffer is closed");
    }
    
    if (instream != null) {
      throw new IllegalStateException("Iterator already obtained.");
    }

    // buffer is closed for adding once iterator is obtained.
    IOUtil.safeClose(outstream);
    outstream = null;

    return new Iterator<ArticleMetadataInfo>() {
      {
        try {
          instream = 
            new ObjectInputStream(
                new BufferedInputStream(
                    new DeleteFileOnCloseInputStream(collectedMetadataFile)));
        } catch (IOException ex) {
          log.warning("Error opening input stream", ex);
        }
      }
      
      @Override
      public boolean hasNext() {
        return (instream != null) && (infoCount > 0);
      }
      
      @Override
      public ArticleMetadataInfo next() throws NoSuchElementException {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        try {
          ArticleMetadataInfo info = (ArticleMetadataInfo)instream.readObject();
          infoCount--;
          return info;
        } catch (ClassNotFoundException ex) {
          NoSuchElementException ex2 = 
              new NoSuchElementException("Error reading next element");
          ex2.initCause(ex);
          throw ex2;
        } catch (IOException ex) {
          NoSuchElementException ex2 = 
              new NoSuchElementException("Error reading next element");
          ex2.initCause(ex);
          throw ex2;
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Determines whether buffer is open.
   * @return <code>true</code> if buffer is open
   */
  private boolean isOpen() {
    return (collectedMetadataFile != null);
  }
  
  /**
   * Release the collected metadata.
   */
  public void close() {
    // collectedMetadataOutputStream automatically deleted on close 
    IOUtil.safeClose(outstream);
    IOUtil.safeClose(instream);
    outstream = null;
    instream = null;
    // but delete the file anyway in case it wasn't read (happens in tests)
    FileUtil.safeDeleteFile(collectedMetadataFile);
    collectedMetadataFile = null;
  }
}
