package org.apache.activemq.transport.discovery.k8s;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.Test;
import javax.jms.ConnectionMetaData;
import java.util.ArrayList;
import java.util.Enumeration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Random;


import static org.junit.Assert.assertTrue;

public class ActiveMqTest {

    @Test
    public void canCreateKubernetesDiscoveryAgent() throws IOException, InterruptedException {
        //thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//
//        Thread.sleep(1000000);


////        while (true) {
            HelloWorldProducer p = new HelloWorldProducer();
            p.run();
           // Thread.sleep(10000);
//        HelloWorldConsumer c = new HelloWorldConsumer() ;
//        c.run();

//        }

    }


    public void readFile(){
        try {
            URL url = new URL("http://127.0.0.1/temp/cp_objectprocessor_docker/jobConfig.yaml");
            URLConnection uc = url.openConnection() ;
            InputStream in = uc.getInputStream() ;

            byte[] buf = new byte[4096];
            for(int br = in.read(buf); br > -1; br = in.read(buf)){
                System.out.write(buf, 0 , br);
            }
            in.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable, ExceptionListener, TransportListener {

        Connection connection = null;
        public void run() {

            Random random = new Random();
            String alphabet = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnO oPpQqRrSsTtUuVvWwXxYyZz";
            int alphabetLength = alphabet.length();
            int charTotal = 9999;

            //ActiveMQConnectionFactory connctionFactory = connctionFactory.setProducerWindowSize(1024000);

//            while (true) {

                try {
                    // Create a ConnectionFactory
                    //ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("fanout:(static:(ssl://activemq-0.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-1.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-2.contentmgmt.stag.pib.dowjones.io:61616))?jms.alwaysSyncSend=true&randomize=true&fanOutQueues=true&minAckCount=3");
                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(ssl://activemq.contentmgmt.prod.pib.dowjones.io:61616)?jms.alwaysSyncSend=true");

                    // Create a Connection
                    connection = connectionFactory.createConnection();
                    connection.start();

                    ((ActiveMQConnection) connection).addTransportListener(this);
                    System.out.println(((ActiveMQConnection) connection).getTransport().getRemoteAddress());

                    // Create a Session
                    BrokerInfo brokerInfo = ((ActiveMQConnection) connection).getBrokerInfo();
                    System.out.println(brokerInfo.getBrokerId());
                    ActiveMQSession session = (ActiveMQSession) connection.createSession(false, ActiveMQSession.AUTO_ACKNOWLEDGE);
                    // Create the destination (Topic or Queue)
                    Destination destination = session.createQueue("mytest2");


                    // Create a MessageProducer from the Session to the Topic or Queue

                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);

                    // Create a messages

                    StringBuilder writeBuilder = new StringBuilder();
                    for (int j = 0; j < charTotal; j++) {
                        writeBuilder.append(alphabet.charAt(random.nextInt(alphabetLength)));
                    }

                    for (int g = 0; g < 10; g++) {
                        String group = "groupy" + g;
                        for (int i = 1; i <= 1000; i++) {


                            String text = writeBuilder + Thread.currentThread().getName() + " : " + this.hashCode();

                            TextMessage message = session.createTextMessage("iteration:" + i + text);
                            message.setStringProperty("JMSXGroupID", group);
                            message.setIntProperty("JMSXGroupSeq", i);

                            if (i == 1000) {
                                message.setIntProperty("JMSXGroupSeq", -1);
                                producer.send(message);
                                Thread.sleep(10000);
                                //System.exit(0);
                            }else {
                                // Tell the producer to send the message

                                producer.send(message);
                                System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
                                Thread.sleep(1);
                            }
                        }

                    }

                        // Clean up
                        session.close();
                        connection.close();
                    } catch(Exception e){
                        System.out.println("Caught and retry: " + e);
                        e.printStackTrace();

                    }

//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            //}

        }

        @Override
        public void onException(JMSException e) {
            System.out.println("Caught: " + e);
            try {
                connection.close();
            } catch (JMSException e1) {
                e1.printStackTrace();
            }
        }

        @Override
        public void onCommand(Object o) {
            System.out.println("on command");
        }

        @Override
        public void onException(IOException e) {
            System.out.println("failover error");
        }

        @Override
        public void transportInterupted() {
            System.out.println("transport interrupted");
        }

        @Override
        public void transportResumed() {
            System.out.println("on resume");
        }
    }


    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        HashMap<String, ArrayList<Integer>> groupCounts = new HashMap<String,  ArrayList<Integer>>();
        public void run() {
            Connection connection = null;

                while (true) {
                    try {

                    // Create a ConnectionFactory

                    //ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(ssl://activemq-0.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-1.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-2.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-3.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-4.contentmgmt.stag.pib.dowjones.io:61616)?randomize=false");

                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(ssl://activemq.contentmgmt.prod.pib.dowjones.io:61616?keepAlive=true&useKeepAlive=true)?initialReconnectDelay=5000");

                    // Create a Connection
                    connection = connectionFactory.createConnection();
                    connection.start();

                    connection.setExceptionListener(this);


                    // Create a Session
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                    // Create the destination (Topic or Queue)
                    //seq_test5?consumer.prefetchSize=1
                    Destination destination = session.createQueue("mytest2?consumer.prefetchSize=1&keepAlive=true");

                    // Create a MessageConsumer from the Session to the Topic or Queue
                    MessageConsumer consumer = session.createConsumer(destination);

                    // Wait for a message


                    for (int i = 1; i < 100000; i++) {

                        Message message = consumer.receive(10000);


                        if (message instanceof TextMessage) {
                            String groupId = message.getStringProperty("JMSXGroupID");
                            int groupSeq = message.getIntProperty("JMSXGroupSeq");
                            if (groupCounts.get(groupId) == null) {
                                groupCounts.put(groupId, new ArrayList<Integer>());
                            }
                            ArrayList<Integer> groupSeqList = groupCounts.get(groupId);
                            groupSeqList.add(groupSeq);
                            TextMessage textMessage = (TextMessage) message;
                            String text = textMessage.getText();

                            if (groupSeqList.size() == 1000) {
                                System.out.println("group " + groupId + "finished succesfully");
                                System.out.println(groupSeqList.toString());
                                groupCounts.put(groupId, null);
                            }
                            System.out.println("Received: " + groupSeq);
                        } else {
                            System.out.println("Received: " + message);
                        }

                        if (i % 100 == 0) {
                            //throw new Exception("custom");
                        }


//                            try {
//                                Thread.sleep(10);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
                    }

                    consumer.close();
                    session.close();
                    connection.close();
                    } catch (Exception e) {
                        try {
                            connection.close();
                            Thread.sleep(40000);
                        } catch (JMSException e1) {
                            e1.printStackTrace();
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        System.out.println("Caught: " + e);
                        e.printStackTrace();

                    }
                }


        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}
