package org.apache.activemq.transport.discovery.k8s;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    public static int groupSize = 1000;
    public static String groupName = "groupz";

    @Test
    public void canCreateKubernetesDiscoveryAgent() throws IOException, InterruptedException {

//        StringBuilder writeBuilder = new StringBuilder();
//        Random random = new Random();
//        String alphabet = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnO oPpQqRrSsTtUuVvWwXxYyZz";
//        int alphabetLength = alphabet.length();
//        int charTotal = 999999;
//
//        for (int j = 0; j < charTotal; j++) {
//            writeBuilder.append(alphabet.charAt(random.nextInt(alphabetLength)));
//        }
//
//        try {
//
//        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616)");
//
//        // Create a Connection
//        //connectionFactory.setAlwaysSyncSend(true);
//        Connection connection = null;
//        try {
//            connection = connectionFactory.createConnection();
//        } catch (JMSException e) {
//            e.printStackTrace();
//        }
//
//        connection.start();
//
//        // Create a Session
//        BrokerInfo brokerInfo = ((ActiveMQConnection) connection).getBrokerInfo();
//        System.out.println(brokerInfo.getBrokerId());
//
//
////        while(!brokerInfo.getBrokerId().toString().equals("activemq-2")){
////            connection.close();
////            connection = connectionFactory.createConnection();
////            connection.start();
////            brokerInfo = ((ActiveMQConnection) connection).getBrokerInfo();
////
////        }
//
//        System.out.println(brokerInfo.getBrokerId());
//
//        for (int i=0; i<20; i++){
//            thread(new HelloWorldProducer(writeBuilder, connection), false);
//        }
//
//
//        while(true){
//            Thread.sleep(1000);
//        }
////        thread(new HelloWorldConsumer(), false);
////
////        Thread.sleep(1000000);
//
//
//////        while (true) {
////            HelloWorldProducer p = new HelloWorldProducer();
////            p.run();
//           // Thread.sleep(10000);
////        HelloWorldConsumer c = new HelloWorldConsumer() ;
////        c.run();
//
//        HelloWorldBrowser b = new HelloWorldBrowser();
//        b.run();
//
////        }
//
//    } catch (JMSException e) {
//        e.printStackTrace();
//    }

                HelloWorldBrowser b = new HelloWorldBrowser();
                b.run();
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

        private StringBuilder writeBuilder;
        HelloWorldProducer(StringBuilder writeBuilder, Connection connection){
            this.writeBuilder = writeBuilder;
            this.connection = connection;
        }

        Connection connection = null;
        public void run() {

            Random random = new Random();
            String alphabet = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnO oPpQqRrSsTtUuVvWwXxYyZz";
            int alphabetLength = alphabet.length();
            int charTotal = 9999999;

            //ActiveMQConnectionFactory connctionFactory = connctionFactory.setProducerWindowSize(1024000);

//            while (true) {

                try {
                    // Create a ConnectionFactory
                    //ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("fanout:(static:(ssl://activemq-0.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-1.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-2.contentmgmt.stag.pib.dowjones.io:61616))?jms.alwaysSyncSend=true&randomize=true&fanOutQueues=true&minAckCount=3");
                    //ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(ssl://activemq-2.contentmgmt.int.pib.dowjones.io:61616)?jms.useAsyncSend=false");

                    // Create a Connection
//                    connectionFactory.setAlwaysSyncSend(true);
//                    connection = connectionFactory.createConnection();

//                    connection.start();

                    //((ActiveMQConnection) connection).onException(this);
                    //((ActiveMQConnection) connection).addTransportListener(this);
                    System.out.println(((ActiveMQConnection) connection).getTransport().getRemoteAddress());

                    ActiveMQSession session = (ActiveMQSession) connection.createSession(false, ActiveMQSession.AUTO_ACKNOWLEDGE);
                    // Create the destination (Topic or Queue)
                    Destination destination = session.createQueue("myqueue");

                    //Destination destination = session.createTopic("VirtualTopic.MetaData");
                    // Create a MessageProducer from the Session to the Topic or Queue

                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);

                    // Create a messages


                    for (int g = 0; g < 10; g++) {
                        String group = groupName + g;
                        for (int i = 1; i <= groupSize; i++) {


                            String text = writeBuilder + Thread.currentThread().getName() + " : " + this.hashCode();

                            TextMessage message = session.createTextMessage("iteration:" + i + text);
                            message.setStringProperty("JMSXGroupID", group);
                            message.setIntProperty("JMSXGroupSeq", i);

                            if (i == groupSize) {
                                message.setIntProperty("JMSXGroupSeq", -1);
                                producer.send(message);
                                Thread.sleep(1000);
                                //System.exit(0);
                            }else {
                                // Tell the producer to send the message

                                producer.send(message);
                                System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
                                //Thread.sleep(1);
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
            System.out.println(((ConnectionError)o).getException());
        }

        @Override
        public void onException(IOException e) {
            System.out.println("Caught: " + e);
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


    public static class HelloWorldBrowser implements Runnable, ExceptionListener {
        HashMap<String, ArrayList<Integer>> groupCounts = new HashMap<String,  ArrayList<Integer>>();
        public void run() {
            Connection connection = null;


                try {

                    // Create a ConnectionFactory

                    //ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(ssl://activemq-0.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-1.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-2.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-3.contentmgmt.stag.pib.dowjones.io:61616,ssl://activemq-4.contentmgmt.stag.pib.dowjones.io:61616)?randomize=false");

                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(ssl://activemq.contentmgmt.int.pib.dowjones.io:61616?keepAlive=true&useKeepAlive=true)?initialReconnectDelay=5000");

                    // Create a Connection
                    connection = connectionFactory.createConnection();
                    connection.start();




                    // Create a Session
                    BrokerInfo brokerInfo = ((ActiveMQConnection) connection).getBrokerInfo();
                    System.out.println(brokerInfo.getBrokerId());

                    connection.setExceptionListener(this);


                    // Create a Session
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                    Queue queue = session.createQueue("Consumer.dac.VirtualTopic.daq2dac");
                    
                    QueueBrowser browser = session.createBrowser(queue);

                    Enumeration msgs = browser.getEnumeration();

                    if ( !msgs.hasMoreElements() ) {
                        System.out.println("No messages in queue");
                    } else {
                        while (msgs.hasMoreElements()) {
                            Message message = (Message)msgs.nextElement();

                            String groupId = message.getStringProperty("JMSXGroupID");
                            int groupSeq = message.getIntProperty("JMSXGroupSeq");
                            if(message.getJMSRedelivered()){
                                System.out.println("redelivered");
                            }

                            if (groupCounts.get(groupId) == null) {
                                groupCounts.put(groupId, new ArrayList<Integer>());
                            }

                            ArrayList<Integer> groupSeqList = groupCounts.get(groupId);
                            groupSeqList.add(groupSeq);


                            System.out.println("Message: " + message);
                        }


                    }

                    System.out.print(groupCounts.keySet());
                    for (int i = 0; i < groupCounts.keySet().size() ; i++) {
                        String key = (String)groupCounts.keySet().toArray()[i];
                        System.out.print(key + ":");
                        System.out.print(((ArrayList<Integer>)groupCounts.get(key)).size());
                        System.out.println();
                    }
//                    System.out.print(((ArrayList<Integer>)groupCounts.get("BOARDR")).size());
//                    System.out.print(((ArrayList<Integer>)groupCounts.get("BOARDO")).size());

                    browser.close();
                    session.close();
                    connection.close();
                } catch (Exception e) {
                    try {
                        connection.close();
                        Thread.sleep(1000);
                    } catch (JMSException e1) {
                        e1.printStackTrace();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    System.out.println("Caught: " + e);
                    e.printStackTrace();

                }


        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
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

                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(ssl://activemq.contentmgmt.int.pib.dowjones.io:61616?keepAlive=true&useKeepAlive=true)?initialReconnectDelay=5000");

                        // Create a Connection
                    connection = connectionFactory.createConnection();
                    connection.start();




                    // Create a Session
                    BrokerInfo brokerInfo = ((ActiveMQConnection) connection).getBrokerInfo();
                    System.out.println(brokerInfo.getBrokerId());

                    connection.setExceptionListener(this);


                    // Create a Session
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);



                    // Create the destination (Topic or Queue)
                    //seq_test5?consumer.prefetchSize=1
                    Destination destination = session.createQueue("test?consumer.prefetchSize=1&keepAlive=true");

                    // Create a MessageConsumer from the Session to the Topic or Queue
                    MessageConsumer consumer = session.createConsumer(destination);

                    // Wait for a message


                    for (int i = 1; i < 100000; i++) {

                        Message message = consumer.receive(1000);


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

                            if (groupSeqList.size() == groupSize) {
                                try {
                                    Files.write(Paths.get(groupId.substring(0, groupId.length()-1)+".txt"), (groupId+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                                }catch (IOException e) {
                                    //exception handling left as an exercise for the reader
                                }
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
                            Thread.sleep(1000);
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
