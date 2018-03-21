/*

Copyright (c) 2012-2018 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.metadata.MetadataManager.METADATA_STATUS_TABLE_NAME;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.lockss.daemon.status.OverviewAccessor;
import org.lockss.daemon.status.StatusTable;
import org.lockss.util.StringUtil;

/**
 * This OverviewAccessor sorts the list into three groups: active, by 
 * descending start time; pending, in queue order, and done, by descending 
 * end time.
 * 
 * @author Philip GUst
 *
 */
class MetadataIndexingOverviewAccessor implements OverviewAccessor {
  final MetadataExtractorManager mdxMgr;
  
  public MetadataIndexingOverviewAccessor(MetadataExtractorManager mdxMgr) {
    this.mdxMgr = mdxMgr;
  }

  @Override
  public Object getOverview(String tableName, BitSet options) {
    List<StatusTable.Reference> res = new ArrayList<StatusTable.Reference>();
    String s;
    if (mdxMgr.isIndexingEnabled()) {
      long activeCount = mdxMgr.getActiveReindexingCount();
      long pendingCount = mdxMgr.getPendingAusCount();
      s =   StringUtil.numberOfUnits(
              activeCount, 
              "active metadata indexing operation", 
              "active metadata index operations") + ", "
          + StringUtil.numberOfUnits(
              pendingCount-activeCount, "pending", "pending");
    } else {
      s = "Metadata Indexing Disabled";
    }
    res.add(new StatusTable.Reference(s, METADATA_STATUS_TABLE_NAME));

    return res;
  }
}
