/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 每个hash桶的大小：4byte
    private static int hashSlotSize = 4;
    // 每个index条目的大小：20byte
    private static int indexSize = 20;
    // 无效索引编号：0 特殊值
    private static int invalidIndex = 0;

    // 默认值：500w
    private final int hashSlotNum;
    // 默认值：2000w
    private final int indexNum;

    // 索引文件使用的 mf
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    // 从mf中获取的 内存映射缓冲区
    private final MappedByteBuffer mappedByteBuffer;
    // 索引头对象
    private final IndexHeader indexHeader;


    /**
     * @param endPhyOffset 上个索引文件 最后一条消息的 物理偏移量
     * @param endTimestamp 上个索引文件 最后一条消息的 存储时间
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // 文件大小  40 + 500w * 4 + 2000w * 20
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);

        // 创建 mf 对象，会在disk上创建文件
        this.mappedFile = new MappedFile(fileName, fileTotalSize);

        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();

        // 创建 索引头对象，传递 索引文件mf 的切片数据
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }


    /**
     * @param key        (msg: 1. uniq_key   2.keys="aaa bbb ccc" 会分别为 aaa bbb ccc 创建索引)
     * @param phyOffset  消息物理偏移量
     * @param storeTimestamp   消息存储时间
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 条件成立：说明索引文件 还有空间
        if (this.indexHeader.getIndexCount() < this.indexNum) {

            // 获取key hash值，这个hash值 是正数
            int keyHash = indexKeyHashMethod(key);

            // 取模  获取 key 对应的 hash桶的下标
            int slotPos = keyHash % this.hashSlotNum;

            // 根据slotPos计算出 keyhash桶的 开始位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);

                // 读取hash桶内的原值 （当hash冲突时  才有值，其它情况 slotValue 是 invalidIndex 0）
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

                // 条件成立：说明 slotValue 是一个无效值..
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 当前msg存储时间 - 索引文件内第一条消息的 存储时间，得到一个差值。 差值使用4byte 表示 就可以了，相对 使用 storeTimestamp 需要8byte 节省了空间。
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                // 转成 秒 表示
                timeDiff = timeDiff / 1000;

                // 第一条索引插入时...timeDiff 是0
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }


                // 计算索引条目写入的开始位置： 40 + 500w * 4 + 索引编号*20
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;


                // key hashcode
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 消息偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 消息存储时间 （第一条索引条目的 差值）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // hash桶的原值（当hash冲突时，会使用到。）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 向当前key 计算出来的 hash桶 内 写入 索引编号
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 索引文件 插入的第一条数据..
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }


                if (invalidIndex == slotValue) {
                    // 占用的hash桶 数量 + 1
                    this.indexHeader.incHashSlotCount();
                }

                // 索引条目 + 1
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }


    /**
     * @param phyOffsets 查询结果 全部 放到该list内
     * @param key 查询key
     * @param maxNum 结果最大数限制
     * @param begin
     * @param end
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {

        if (this.mappedFile.hold()) {// mf 引用记数+1，查询期间 mf 资源不能被释放

            // 获取当前key hash值
            int keyHash = indexKeyHashMethod(key);
            // 取模 计算出key hash 对应的 hash桶下标值
            int slotPos = keyHash % this.hashSlotNum;

            // 计算出 hash桶存储的开始位置：40 + 下标值 * 4
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取hash桶内的值，这个值可能是 无效值 也可能是 索引编号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }


                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                    // 查询未命中..
                } else {// 正常走这里


                    // nextIndexToRead：下一条要读取的 索引编号
                    for (int nextIndexToRead = slotValue; ; ) {

                        // 停止查询条件..
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }


                        // 计算出索引编号对应索引数据的 开始位置：..
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;


                        // 读取索引数据
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);


                        if (timeDiff < 0) {
                            break;
                        }

                        // 转换成毫秒
                        timeDiff *= 1000L;

                        // 计算出 msg 准确的存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;

                        // 时间范围的匹配
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        // 条件成立：说明查询命中，将消息索引的 消息偏移量加入到 list 集合中。
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }


                        // 判断 索引条目的 前驱索引 编号是否是 无效的.. 无效跳出查询逻辑
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }


                        // 赋值给 nextIndexToRead ，继续向前查询！ 解决hash冲突！
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
                // 引用记数 -1
                this.mappedFile.release();
            }
        }
    }
}
