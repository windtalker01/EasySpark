package com.hailian.java.elasticsearch;

import java.util.Objects;

/**
 * 海联软件大数据
 * FileName: TransportInfoBean
 * Author: Luyj
 * Date: 2020/4/28 上午10:27
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * Luyj              2020/4/28 上午10:27    v1.0              创建
 */
public class TransportInfoBean {
    private String ClusterName;
    private String NodeIP;
    private int Port;

    public TransportInfoBean() {
        ClusterName = null;
        NodeIP = null;
        Port = 0;
    }
    public TransportInfoBean(String clusterName, String nodeIP, int port) {
        ClusterName = clusterName;
        NodeIP = nodeIP;
        Port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransportInfoBean that = (TransportInfoBean) o;
        return Objects.equals(ClusterName, that.ClusterName) &&
                Objects.equals(NodeIP, that.NodeIP) &&
                Objects.equals(Port, that.Port);
    }

    @Override
    public int hashCode() {

        return Objects.hash(ClusterName, NodeIP, Port);
    }

    public String getClusterName() {
        return ClusterName;
    }

    public void setClusterName(String clusterName) {
        ClusterName = clusterName;
    }

    public String getNodeIP() {
        return NodeIP;
    }

    public void setNodeIP(String nodeIP) {
        NodeIP = nodeIP;
    }

    public int getPort() {
        return Port;
    }

    public void setPort(int port) {
        Port = port;
    }
}
