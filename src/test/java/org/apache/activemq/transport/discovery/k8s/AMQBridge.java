package org.apache.activemq.transport.discovery.k8s;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Set;
import org.apache.activemq.broker.jmx.NetworkBridgeViewMBean;

public class AMQBridge {
    public static void main(String[] args) throws Exception {
        JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(url, null);
        connector.connect();
        MBeanServerConnection connection = connector.getMBeanServerConnection();
        ObjectName name = new ObjectName(
                "org.apache.activemq:BrokerName=static-broker1,Type=NetworkBridge,*");
        Set<ObjectName> bridges = connection.queryNames(name, null);
        for (ObjectName bridgeName : bridges) {
            NetworkBridgeViewMBean view =
                    (NetworkBridgeViewMBean) MBeanServerInvocationHandler.newProxyInstance(
                            connection, bridgeName, NetworkBridgeViewMBean.class, true);
            System.out.println("Bridge to " + view.getRemoteBrokerName()
                    + " (" + view.getRemoteAddress() + ") "
                    + ((view.isCreatedByDuplex() ? "- created by duplex" : "")));
        }
    }
}