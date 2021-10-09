/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.StatPersisted;

/**
 * 摘要计算器
 * Defines how to calculate the digest for a given node.
 */
public class DigestCalculator {

    // The hardcoded digest version, should bump up this version whenever
    // we changed the digest method or fields.
    /** 硬编码方式定义当前使用的摘要版本，如果以后摘要算法变了，要提升该版本号 */
    private static final int DIGEST_VERSION = 2;


    /**
     * 计算摘要值
     * Calculate the digest based on the given params.
     *
     * Besides the path and data, the following stat fields are included in
     * the digest calculation:
     *
     * - long czxid    8 bytes
     * - long mzxid    8 bytes
     * - long pzxid    8 bytes
     * - long ctime    8 bytes
     * - long mtime    8 bytes
     * - int version   4 bytes
     * - int cversion  4 bytes
     * - int aversion  4 bytes
     * - long ephemeralOwner 8 bytes
     *
     * @param path the path of the node
     * @param data the data of the node
     * @param stat the stat associated with the node
     * @return the digest calculated from the given params
     */
    long calculateDigest(String path, byte[] data, StatPersisted stat) {

        if (!ZooKeeperServer.isDigestEnabled()) {
            /** 如果没有启用摘要，则返回摘要值0 */
            return 0;
        }

        // Quota nodes are updated locally, there is inconsistent issue
        // when we tried to release digest feature at the beginning.
        //
        // Instead of taking time to fix that, we decided to disable digest
        // check for all the nodes under /zookeeper/ first.
        //
        // We can enable this after fixing that inconsistent problem. The
        // digest version in the protocol enables us to change the digest
        // calculation without disrupting the system.
        if (path.startsWith(ZooDefs.ZOOKEEPER_NODE_SUBTREE)) {
            /** /zookeeper/ 路径子的节点都不计算摘要，直接返回0 */
            return 0;
        }

        // "" and "/" are aliases to each other, in DataTree when adding child
        // under "/", it will use "" as the path, but when set data or change
        // ACL on "/", it will use "/" as the path. Always mapping "/" to ""
        // to avoid mismatch.
        if (path.equals("/")) {
            path = "";
        }

        // total = 8 * 6 + 4 * 3 = 60 bytes
        byte[] b = new byte[60];
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.putLong(stat.getCzxid());
        bb.putLong(stat.getMzxid());
        bb.putLong(stat.getPzxid());
        bb.putLong(stat.getCtime());
        bb.putLong(stat.getMtime());
        bb.putInt(stat.getVersion());
        bb.putInt(stat.getCversion());
        bb.putInt(stat.getAversion());
        bb.putLong(stat.getEphemeralOwner());

        /** 使用CRC32计算摘要值 */
        CRC32 crc = new CRC32();
        crc.update(path.getBytes());
        if (data != null) {
            crc.update(data);
        }
        crc.update(b);
        return crc.getValue();
    }

    /**
     * Calculate the digest based on the given path and data node.
     */
    long calculateDigest(String path, DataNode node) {
        if (!node.isDigestCached()) {
            /** 如果没有计算过摘要值，则计算摘要并缓存在节点中 */
            node.setDigest(calculateDigest(path, node.getData(), node.stat));
            node.setDigestCached(true);
        }
        return node.getDigest();
    }

    /**
     * Returns with the current digest version.
     */
    int getDigestVersion() {
        return DIGEST_VERSION;
    }

}
