/*

Copyright (c) 2017-2020 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.metadata.MetadataConstants.*;
import static org.lockss.metadata.extractor.MetadataExtractorManager.*;
import java.nio.charset.Charset;
import java.util.Base64;
import org.lockss.config.CurrentConfig;
import org.lockss.metadata.ItemMetadata;
import org.lockss.util.Logger;
import org.lockss.util.rest.RestUtil;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * A client for the REST web service operation that stores the metadata of an
 * Archival Unit item.
 */
public class StoreAuItemClient {
  private static Logger log = Logger.getLogger(StoreAuItemClient.class);

  /**
   * Posts the metadata of an Archival Unit item to be stored.
   * 
   * @param item
   *          An ItemMetadata with the metadata.
   * @return a Long with the database identifier of the metadata item.
   */
  public Long storeAuItem(ItemMetadata item) {
    final String DEBUG_HEADER = "storeAuItem(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "item = " + item);

    // Get the configured REST service location.
    String restServiceLocation =
	CurrentConfig.getParam(PARAM_MD_REST_SERVICE_LOCATION);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "restServiceLocation = "
	+ restServiceLocation);

    // Get the client connection timeout.
    long timeoutValue = CurrentConfig.getIntParam(PARAM_MD_REST_TIMEOUT_VALUE,
	DEFAULT_MD_REST_TIMEOUT_VALUE);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "timeoutValue = " + timeoutValue);

    // Get the authentication credentials.
    String userName = CurrentConfig.getParam(PARAM_MD_REST_USER_NAME);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "userName = '" + userName + "'");
    String password = CurrentConfig.getParam(PARAM_MD_REST_PASSWORD);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "password = '" + password + "'");

    // Initialize the request to the REST service.
    RestTemplate restTemplate =
	RestUtil.getRestTemplate(1000*timeoutValue, 1000*timeoutValue);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    String credentials = userName + ":" + password;
    String authHeaderValue = "Basic " + Base64.getEncoder()
    .encodeToString(credentials.getBytes(Charset.forName("US-ASCII")));
    headers.set("Authorization", authHeaderValue);

    // Make the request to the REST service and get its response.
    ResponseEntity<Long> response =
	restTemplate.exchange(restServiceLocation + "/aus", HttpMethod.POST,
	    new HttpEntity<ItemMetadata>(item, headers), Long.class);

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    Long mdItemSeq = response.getBody();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }
}
