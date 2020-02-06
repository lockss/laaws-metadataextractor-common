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
package org.lockss.metadata;

import org.lockss.metadata.ArticleMetadataBuffer.ArticleMetadataInfo;

/**
 * Exception specific to the processing of extracted metadata.
 * 
 * @version 1.0
 */
@SuppressWarnings("serial")
public class MetadataIndexingException extends Exception {

  private ArticleMetadataInfo mdinfo;

  MetadataIndexingException() {
    super();
  }

  MetadataIndexingException(String message) {
    super(message);
  }

  MetadataIndexingException(Throwable cause) {
    super(cause);
  }

  MetadataIndexingException(String message, Throwable cause) {
    super(message, cause);
  }

  MetadataIndexingException(String message, ArticleMetadataInfo mdinfo) {
    super(message);
    this.mdinfo = mdinfo;
  }

  /**
   * Provides the article metadata information involved in this exception.
   * @return an ArticleMetadataInfo with the article metadata information.
   */
  public ArticleMetadataInfo getArticleMetadataInfo() {
    return mdinfo;
  }
}
