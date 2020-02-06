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

import java.util.*;
import org.lockss.extractor.ArticleMetadata;
import org.lockss.extractor.MetadataField;
import org.lockss.metadata.ArticleMetadataBuffer.ArticleMetadataInfo;
import org.lockss.util.*;
import org.lockss.test.*;

/**
 * Test class for org.lockss.daemon.ArticleMetadataBuffer.
 *
 * @author  Philip Gust
 * @version 1.0
 */
public class TestArticleMetadataBuffer extends LockssTestCase {
  static Logger log = Logger.getLogger(TestArticleMetadataBuffer.class);

  ArticleMetadataBuffer amBuffer;

  public void setUp() throws Exception {
    super.setUp();
    amBuffer = new ArticleMetadataBuffer(getTempDir());
  }

  public void tearDown() throws Exception {
    amBuffer.close();
    super.tearDown();
  }

  /**
   * Variant with ISBN, ISSN, and item number metadata specified.
   * Should result in 'bookSeries' and 'book_chapter' being in metadata item.
   * @throws Exception
   */
  public void testArticleMetadtataBuffer1() throws Exception {
    for (int i = 1; i < 10; i++) {
      ArticleMetadata am = new ArticleMetadata();
      am.put(MetadataField.FIELD_PUBLICATION_TITLE, "Journal Title" + i);
      am.put(MetadataField.FIELD_DATE, "2012-12-0"+i);
      am.put(MetadataField.FIELD_ISSN,"1234-567" + i);
      am.put(MetadataField.FIELD_EISSN,"4321-765" + i);
      am.put(MetadataField.FIELD_ARTICLE_TITLE, "Article Title" + i);
      am.put(MetadataField.FIELD_PUBLISHER, "Publisher"+i);
      am.put(MetadataField.FIELD_AUTHOR, "Author,First"+i);
      am.put(MetadataField.FIELD_AUTHOR, "Author,Second"+i);
      am.put(MetadataField.FIELD_ACCESS_URL, "http://xyz.com/" + i);
      am.put(MetadataField.FIELD_ISBN, "0123456789");
      am.put(MetadataField.FIELD_EISBN, "9876543210");
      am.put(MetadataField.FIELD_ITEM_NUMBER, "3");
      amBuffer.add(am);
    }
    int count = 0;
    Iterator<ArticleMetadataInfo> amitr = amBuffer.iterator();
    while (amitr.hasNext()) {
      ArticleMetadataInfo aminfo = amitr.next();
      assertNotNull(aminfo);
      count++;
      assertEquals("2012",aminfo.pubYear);
      assertEquals("Journal Title"+count,aminfo.publicationTitle);
      assertEquals("2012-12-0"+count,aminfo.pubDate);
      assertEquals("1234-567"+count,aminfo.issn);
      assertEquals("4321-765"+count,aminfo.eissn);
      assertEquals("Article Title"+count,aminfo.articleTitle);
      assertEquals("Publisher"+count,aminfo.publisher);
      assertSameElements(
          new String[] {"Author,First"+count, "Author,Second"+count}, 
          aminfo.authors);
      assertEquals("http://xyz.com/"+count,aminfo.accessUrl);
      assertEquals("0123456789",aminfo.isbn);
      assertEquals("9876543210",aminfo.eisbn);
      assertEquals("3", aminfo.itemNumber);
      assertEquals(MetadataField.PUBLICATION_TYPE_BOOKSERIES, aminfo.publicationType);
      assertEquals(MetadataField.ARTICLE_TYPE_BOOKCHAPTER, aminfo.articleType);
    }
    assertEquals(9, count);
    amBuffer.close();
  }
  
  /**
   * Variant with 'bookSeries' publication type and 'book_chapter' article type.
   * Should result in 'bookSeries' and 'book_chapter' being in metadata item.
   * @throws Exception
   */
  public void testArticleMetadtataBuffer2() throws Exception {
    for (int i = 1; i < 10; i++) {
      ArticleMetadata am = new ArticleMetadata();
      am.put(MetadataField.FIELD_PUBLICATION_TITLE, "Journal Title" + i);
      am.put(MetadataField.FIELD_DATE, "2012-12-0"+i);
      am.put(MetadataField.FIELD_ARTICLE_TITLE, "Article Title" + i);
      am.put(MetadataField.FIELD_PUBLISHER, "Publisher"+i);
      am.put(MetadataField.FIELD_AUTHOR, "Author,First"+i);
      am.put(MetadataField.FIELD_AUTHOR, "Author,Second"+i);
      am.put(MetadataField.FIELD_ACCESS_URL, "http://xyz.com/" + i);
      am.put(MetadataField.FIELD_PUBLICATION_TYPE, "bookSeries");
      am.put(MetadataField.FIELD_ARTICLE_TYPE, "book_chapter");
      amBuffer.add(am);
    }
    int count = 0;
    Iterator<ArticleMetadataInfo> amitr = amBuffer.iterator();
    while (amitr.hasNext()) {
      ArticleMetadataInfo aminfo = amitr.next();
      assertNotNull(aminfo);
      count++;
      assertEquals("2012",aminfo.pubYear);
      assertEquals("Journal Title"+count,aminfo.publicationTitle);
      assertEquals("2012-12-0"+count,aminfo.pubDate);
      assertEquals("Article Title"+count,aminfo.articleTitle);
      assertEquals("Publisher"+count,aminfo.publisher);
      assertSameElements(
          new String[] {"Author,First"+count, "Author,Second"+count}, 
          aminfo.authors);
      assertEquals("http://xyz.com/"+count,aminfo.accessUrl);
      assertEquals(MetadataField.PUBLICATION_TYPE_BOOKSERIES, aminfo.publicationType);
      assertEquals(MetadataField.ARTICLE_TYPE_BOOKCHAPTER, aminfo.articleType);
    }
    assertEquals(9, count);
    amBuffer.close();
  }

  /**
   * Variant with 'bookSeries' publication type and item number metadata.
   * Should result in 'bookSeries' and 'book_chapter' being in metadata item.
   * @throws Exception
   */
  public void testArticleMetadtataBuffer3() throws Exception {
    for (int i = 1; i < 10; i++) {
      ArticleMetadata am = new ArticleMetadata();
      am.put(MetadataField.FIELD_PUBLICATION_TITLE, "Journal Title" + i);
      am.put(MetadataField.FIELD_DATE, "2012-12-0"+i);
      am.put(MetadataField.FIELD_ARTICLE_TITLE, "Article Title" + i);
      am.put(MetadataField.FIELD_PUBLISHER, "Publisher"+i);
      am.put(MetadataField.FIELD_AUTHOR, "Author,First"+i);
      am.put(MetadataField.FIELD_AUTHOR, "Author,Second"+i);
      am.put(MetadataField.FIELD_ACCESS_URL, "http://xyz.com/" + i);
      am.put(MetadataField.FIELD_PUBLICATION_TYPE, "bookSeries");
      am.put(MetadataField.FIELD_ITEM_NUMBER, "3");
      amBuffer.add(am);
    }
    int count = 0;
    Iterator<ArticleMetadataInfo> amitr = amBuffer.iterator();
    while (amitr.hasNext()) {
      ArticleMetadataInfo aminfo = amitr.next();
      assertNotNull(aminfo);
      count++;
      assertEquals("2012",aminfo.pubYear);
      assertEquals("Journal Title"+count,aminfo.publicationTitle);
      assertEquals("2012-12-0"+count,aminfo.pubDate);
      assertEquals("Article Title"+count,aminfo.articleTitle);
      assertEquals("Publisher"+count,aminfo.publisher);
      assertSameElements(
          new String[] {"Author,First"+count, "Author,Second"+count}, 
          aminfo.authors);
      assertEquals("http://xyz.com/"+count,aminfo.accessUrl);
      assertEquals("3", aminfo.itemNumber);
      assertEquals(MetadataField.PUBLICATION_TYPE_BOOKSERIES, aminfo.publicationType);
      assertEquals(MetadataField.ARTICLE_TYPE_BOOKCHAPTER, aminfo.articleType);
    }
    assertEquals(9, count);
    amBuffer.close();
  }

  public void testEmptyArticleMetadtataBuffer() throws Exception {
    Iterator<ArticleMetadataInfo> amitr = amBuffer.iterator();
    assertFalse(amitr.hasNext());
    try {
      amitr.next();
      fail("Should have thrown NoSuchElementException");
    } catch (NoSuchElementException ex) {
    }
    try {
      // cannot get iterator twice
      amBuffer.iterator();
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ex) {
    }
    amBuffer.close();
  }

  public void testClosedArticleMetadtataBuffer() throws Exception {
    amBuffer.close();
    try {
      // cannot add after closing
      amBuffer.add(new ArticleMetadata());
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ex) {      
    }
    try {
      // cannot obtain iterator after closing
      @SuppressWarnings("unused")
      Iterator<ArticleMetadataInfo> amitr = amBuffer.iterator();
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ex) {
    }
    
  }
}
