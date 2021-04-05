/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package start.case9HbaseKafka.case02;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSubmitter {
    protected static final Logger LOG = LoggerFactory.getLogger(LocalSubmitter.class);

    private LocalDRPC drpc;
    private LocalCluster cluster;

    public LocalSubmitter(LocalDRPC drpc, LocalCluster cluster) {
        this.drpc = drpc;
        this.cluster = cluster;
    }

    public static LocalSubmitter newInstance() {
        return new LocalSubmitter(new LocalDRPC(), new LocalCluster());
    }

    public static Config defaultConfig() {
        return defaultConfig(false);
    }

    public static Config defaultConfig(boolean debug) {
        final Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setDebug(debug);
        return conf;
    }

    public void submit(String name, Config config, StormTopology topology) {
        cluster.submitTopology(name, config, topology);
    }

    public void kill(String name) {
        cluster.killTopology(name);
    }

    public void shutdown() {
        cluster.shutdown();
    }

    public LocalDRPC getDrpc() {
        return drpc;
    }

    public LocalCluster getCluster() {
        return cluster;
    }
}
