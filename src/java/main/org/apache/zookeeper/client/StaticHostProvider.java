/**
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

package org.apache.zookeeper.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Most simple HostProvider, resolves only on instantiation.
 * 
 */
public final class StaticHostProvider implements HostProvider {
    private static final Logger LOG = LoggerFactory
            .getLogger(StaticHostProvider.class);

    /** 解析后的Server地址列表 */
    private final List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(
            5);

    /**
     * 最近一个连接成功的地址索引
     */
    private int lastIndex = -1;

    /**
     * 当前提供给zk客户端使用的地址的索引
     */
    private int currentIndex = -1;

    /**
     * Constructs a SimpleHostSet.
     * 
     * @param serverAddresses
     *            possibly unresolved ZooKeeper server addresses
     * @throws UnknownHostException
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses)
            throws UnknownHostException {
        /** 遍历用户配置的Server列表，并解析主机地址 */
        for (InetSocketAddress address : serverAddresses) {
            InetAddress resolvedAddresses[] = InetAddress.getAllByName(address.getHostName());
            for (InetAddress resolvedAddress : resolvedAddresses) {
                /** 构建解析后的地址列表，如果是主机域名，可以解析出多个ip地址 */
                this.serverAddresses.add(new InetSocketAddress(resolvedAddress.getHostAddress(), address.getPort()));
            }
        }

        /** 如果用户配置的Server列表都没有解析成功，抛出异常 */
        if (this.serverAddresses.isEmpty()) {
            throw new IllegalArgumentException("A HostProvider may not be empty!");
        }

        /** 对主机列表做洗牌，打散地址列表，方便实现随机获取下一个地址 */
        Collections.shuffle(this.serverAddresses);
    }

    public int size() {
        return serverAddresses.size();
    }

    /**
     * 获取下一次重试的地址
     * @param spinDelay
     * @return
     */
    public InetSocketAddress next(long spinDelay) {
        /** 当前索引自增，表示随机(地址已经被打散)取下一个地址 */
        ++currentIndex;
        /** 地址形成环 */
        if (currentIndex == serverAddresses.size()) {
            currentIndex = 0;
        }
        /** 如果最新分配的地址就是上次连接成功的地址，说明所有的地址都尝试了一遍，并不能连接成功，有可能是网络有问题，等待一段时间再重试 */
        if (currentIndex == lastIndex && spinDelay > 0) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
            }
        } else if (lastIndex == -1) {
            // We don't want to sleep on the first ever connect attempt.
            lastIndex = 0;
        }
        /** 从列表中取出地址返回 */
        return serverAddresses.get(currentIndex);
    }

    public void onConnected() {
        /** 如果返回的新地址连接成功，则更新最新连接成功的索引 */
        lastIndex = currentIndex;
    }
}
