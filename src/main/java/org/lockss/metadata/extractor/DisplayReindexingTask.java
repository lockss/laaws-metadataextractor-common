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
package org.lockss.metadata.extractor;

import org.lockss.metadata.extractor.MetadataExtractorManager.ReindexingStatus;

/**
 * Reindexing task wrapper for display purposes.
 * 
 * @author Fernando García-Loygorri
 */
public class DisplayReindexingTask {

  private String auId = null;
  private String auName = null;
  private boolean auNoSubstance = false;
  private long startClockTime = 0;
  private long startUpdateClockTime = 0;
  private long endClockTime = 0;
  private ReindexingStatus status = null;
  private long indexedArticleCount = 0;
  private long updatedArticleCount = 0;
  private boolean isNewAu = false;
  private boolean needsFullReindex = false;
  private Exception e;

  /**
   * No-argument constructor.
   */
  public DisplayReindexingTask() {
  }

  /**
   * Constructor.
   * 
   * @param reindexingTask A ReindexingTask to be wrapped by this object.
   */
  public DisplayReindexingTask(ReindexingTask reindexingTask) {
    auId = reindexingTask.getAuId();
    auName = reindexingTask.getAuName();
    auNoSubstance = reindexingTask.hasNoAuSubstance();
    startClockTime = reindexingTask.getStartTime();
    startUpdateClockTime = reindexingTask.getStartUpdateTime();
    endClockTime = reindexingTask.getEndTime();
    status = reindexingTask.getReindexingStatus();
    indexedArticleCount = reindexingTask.getIndexedArticleCount();
    updatedArticleCount = reindexingTask.getUpdatedArticleCount();
    isNewAu = reindexingTask.isNewAu();
    needsFullReindex = reindexingTask.needsFullReindex();
    e = reindexingTask.getException();
  }

  /**
   * Returns the auid of the task AU.
   * 
   * @return a String with the auid of the task AU.
   */
  String getAuId() {
    return auId;
  }

  void setAuId(String auId) {
    this.auId = auId;
  }

  /**
   * Returns the name of the task AU.
   * 
   * @return a String with the name of the task AU.
   */
  String getAuName() {
    return auName;
  }

  void setAuName(String auName) {
    this.auName = auName;
  }

  /**
   * Returns the substance state of the task AU.
   * 
   * @return <code>true</code> if AU has no substance, <code>false</code>
   *         otherwise.
   */
  boolean hasNoAuSubstance() {
    return auNoSubstance;
  }

  void setAuNoSubstance(boolean auNoSubstance) {
    this.auNoSubstance = auNoSubstance;
  }

  /**
   * Provides the start time for indexing.
   * 
   * @return a long with the start time in milliseconds since epoch (0 if not
   *         started).
   */
  long getStartTime() {
    return startClockTime;
  }

  void setStartTime(long startClockTime) {
    this.startClockTime = startClockTime;
  }

  /**
   * Provides the update start time.
   * 
   * @return a long with the update start time in milliseconds since epoch (0 if
   *         not started).
   */
  long getStartUpdateTime() {
    return startUpdateClockTime;
  }

  void setStartUpdateTime(long startUpdateClockTime) {
    this.startUpdateClockTime = startUpdateClockTime;
  }

  /**
   * Provides the end time for indexing.
   * 
   * @return a long with the end time in milliseconds since epoch (0 if not
   *         finished).
   */
  long getEndTime() {
    return endClockTime;
  }

  void setEndTime(long endClockTime) {
    this.endClockTime = endClockTime;
  }

  /**
   * Returns the reindexing status of this task.
   * 
   * @return a ReindexingStatus with the reindexing status.
   */
  ReindexingStatus getReindexingStatus() {
    return status;
  }

  void setReindexingStatus(ReindexingStatus status) {
    this.status = status;
  }

  /**
   * Returns the number of articles extracted by this task.
   * 
   * @return a long with the number of articles extracted by this task.
   */
  long getIndexedArticleCount() {
    return indexedArticleCount;
  }

  void setIndexedArticleCount(long indexedArticleCount) {
    this.indexedArticleCount = indexedArticleCount;
  }

  /**
   * Returns the number of articles updated by this task.
   * 
   * @return a long with the number of articles updated by this task.
   */
  public long getUpdatedArticleCount() {
    return updatedArticleCount;
  }

  void setUpdatedArticleCount(long updatedArticleCount) {
    this.updatedArticleCount = updatedArticleCount;
  }

  /**
   * Returns an indication of whether the AU has not yet been indexed.
   * 
   * @return <code>true</code> if the AU has not yet been indexed,
   * <code>false</code> otherwise.
   */
  boolean isNewAu() {
    return isNewAu;
  }

  void setNewAu(boolean isNewAu) {
    this.isNewAu = isNewAu;
  }

  /**
   * Returns an indication of whether the AU needs a full reindex.
   * 
   * @return <code>true</code> if the AU needs a full reindex,
   * <code>false</code> otherwise.
   */
  boolean needsFullReindex() {
    return needsFullReindex;
  }

  void setNeedFullReindex(boolean needFullReindex) {
    this.needsFullReindex = needFullReindex;
  }

  /**
   * Returns an exception resulting from the execution of the task.
   * 
   * @return an Exception resulting from the execution of the task.
   */
  Exception getException() {
    return e;
  }

  void setE(Exception e) {
    this.e = e;
  }

  @Override
  public String toString() {
    return "[DisplayReindexingTask auId=" + auId + ", auName=" + auName
	+ ", auNoSubstance=" + auNoSubstance
	+ ", startClockTime=" + startClockTime
	+ ", startUpdateClockTime=" + startUpdateClockTime
	+ ", endClockTime=" + endClockTime + ", status=" + status
	+ ", indexedArticleCount=" + indexedArticleCount
	+ ", updatedArticleCount=" + updatedArticleCount
	+ ", isNewAu=" + isNewAu + ", needFullReindex=" + needsFullReindex
	+ ", e=" + e + "]";
  }
}
