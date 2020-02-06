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
package org.lockss.metadata;

import org.lockss.config.Configuration;

/**
 * Constants used by the metadata query/extractor code.
 * 
 * @author Fernando García-Loygorri
 */
public class MetadataConstants {

  /**
   * Prefix for configuration properties.
   */
  public static final String PREFIX = Configuration.PREFIX + "metadataManager.";

  /**
   * Metadata REST service configuration prefix.
   */
  public static final String MD_REST_PREFIX = PREFIX + "mdRest.";

  /**
   * Metadata REST service location key.
   */
  public static final String PARAM_MD_REST_SERVICE_LOCATION =
      MD_REST_PREFIX + "serviceLocation";

  /**
   * The initial value of the metadata extraction time for an AU whose metadata
   * has not been extracted yet.
   */
  public static final long NEVER_EXTRACTED_EXTRACTION_TIME = 0L;

  static final String UNKNOWN_TITLE_NAME_ROOT = "UNKNOWN_TITLE";
  static final String UNKNOWN_SERIES_NAME_ROOT = "UNKNOWN_SERIES";
}
