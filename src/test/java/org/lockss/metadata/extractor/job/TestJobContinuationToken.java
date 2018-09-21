/*

Copyright (c) 2018, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

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
package org.lockss.metadata.extractor.job;

import org.junit.Test;
import org.lockss.util.test.LockssTestCase5;

/**
 * Test class for org.lockss.metadata.extractor.job.JobContinuationToken.
 */
public class TestJobContinuationToken extends LockssTestCase5 {

  @Test
  public void testWebRequestContinuationTokenConstructor() {
    JobContinuationToken imct =
	new JobContinuationToken(null);
    assertNull(imct.getQueueTruncationTimestamp());
    assertNull(imct.getLastJobSeq());
    assertNull(imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken("");
    assertNull(imct.getQueueTruncationTimestamp());
    assertNull(imct.getLastJobSeq());
    assertNull(imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken(" ");
    assertNull(imct.getQueueTruncationTimestamp());
    assertNull(imct.getLastJobSeq());
    assertNull(imct.toWebResponseContinuationToken());

    try {
      imct = new JobContinuationToken("-");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken(" - ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("ABC-");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("1234-");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("1234-XYZ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("1234--5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("-XYZ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("ABC-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("-1234-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken("1234-9-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    imct = new JobContinuationToken("1234-0");
    assertEquals(1234, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(0, imct.getLastJobSeq().longValue());
    assertEquals("1234-0", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken("0-5678");
    assertEquals(0, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(5678, imct.getLastJobSeq().longValue());
    assertEquals("0-5678", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken("1234-5678");
    assertEquals(1234, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(5678, imct.getLastJobSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken(" 1234 - 5678 ");
    assertEquals(1234, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(5678, imct.getLastJobSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken("01234-005678");
    assertEquals(1234, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(5678, imct.getLastJobSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken("0-0");
    assertEquals(0, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(0, imct.getLastJobSeq().longValue());
    assertEquals("0-0", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken("00-000");
    assertEquals(0, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(0, imct.getLastJobSeq().longValue());
    assertEquals("0-0", imct.toWebResponseContinuationToken());
  }

  @Test
  public void testMemberConstructor() {
    JobContinuationToken imct =
	new JobContinuationToken(null, null);
    assertNull(imct.getQueueTruncationTimestamp());
    assertNull(imct.getLastJobSeq());
    assertNull(imct.toWebResponseContinuationToken());

    try {
      imct = new JobContinuationToken(null, 5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken(-1234L, 5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken(1234L, null);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken(1234L, -5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new JobContinuationToken(-1234L, -5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    imct = new JobContinuationToken(0l, 0L);
    assertEquals(0, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(0, imct.getLastJobSeq().longValue());
    assertEquals("0-0", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken(1234L, 0L);
    assertEquals(1234, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(0, imct.getLastJobSeq().longValue());
    assertEquals("1234-0", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken(0L, 5678L);
    assertEquals(0, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(5678, imct.getLastJobSeq().longValue());
    assertEquals("0-5678", imct.toWebResponseContinuationToken());

    imct = new JobContinuationToken(1234L, 5678L);
    assertEquals(1234, imct.getQueueTruncationTimestamp().longValue());
    assertEquals(5678, imct.getLastJobSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());
  }
}
