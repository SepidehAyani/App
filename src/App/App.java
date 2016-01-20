package App;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

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
 

public class App {
 
	//the term exception is shorthand for the phrase "exceptional event."
	//Definition: An exception is an event, which occurs during the execution of a program, 
	//that disrupts the normal flow of the program's instructions.
	//When an error occurs within a method, the method creates an object and hands it off to the runtime system. 
	//The object, called an exception object, contains information about the error, including its type and the state of the program when the error occurred. 
	//Creating an exception object and handing it to the runtime system is called throwing an exception.

	//After a method throws an exception, the runtime system attempts to find something to handle it. 
	//The set of possible "somethings" to handle the exception is the ordered list of methods that had been called to get to the method where the error occurred. 
	//The list of methods is known as the call stack 
	
    public static void main(String[] args) throws Exception {
        thread(new AttackDetectorProducer(), false);
        Thread.sleep(1000);
        thread(new AttackDetectorConsumer(), false);

    }
 
    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
 
    public static class AttackDetectorProducer implements Runnable {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
 
                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();
         
                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");
 
                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
 
                // reads the apache access log file, line by line
                // send each line as a message to the activeMQ broker
                // Close the file and exit the producer thread
                
                
                //The FileReader class creates a Reader that you can use to read the contents of a file
                //The BufferedReader is buffering the text to read line by line
        		try {
        			File file = new File("Developer Project - apache-access-log.txt");
        			FileReader fileReader = new FileReader(file);
        			BufferedReader bufferedReader = new BufferedReader(fileReader);
        			String line;
        			while ((line = bufferedReader.readLine()) != null) {
        				TextMessage message = session.createTextMessage(line);
                        producer.send(message);
    				}
        			fileReader.close();
        			
        		} catch (IOException e) {
        			e.printStackTrace();
        		}
 
                
                System.out.println("Sent all messages: "+ " : " + Thread.currentThread().getName());
 
                // Clean up
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }
 
    public static class AttackDetectorConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {
 
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
 
                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();
 
                connection.setExceptionListener(this);
 
                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");
 
                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);
 
                // Declare a message 
                Message message;
                
                // open output file to write
                PrintWriter outputFile = new PrintWriter(new File("Output File.txt"));

                // Loop through 1000 message blocks until timeout
                // Timeout is defined as one minute NOT receiving any messages  
                
                //First loop: making a block of 1000 messages until timeout which is one minute
                
                Boolean timeout = false;
                while(timeout == false)
                {
                	// create a map ip to count (number of repetition of this ip)
                	//In programming terms, a map is a set of associations between pairs of objects. 
                	//A Map is an object that maps keys to values. 
                	//A map cannot contain duplicate keys: Each key can map to at most one value. 
                	//It models the mathematical function abstraction. 
                	//The Map interface includes methods for basic operations (such as put, get, remove, 
                	//containsKey, containsValue, size, and empty), bulk operations (such as putAll and clear), 
                	//and collection views (such as keySet, entrySet, and values). 
                	
                	Map<String, Integer> myMap = new HashMap<String, Integer>();
                	
                	// Second Loop:
                	// receive one block of 1000 messages 
                	// detect attacker
                	// the definition of attacker is the ip address from which 
                	// we have received too many messages (above 3 in a block of 1000)
                	
                	for(int i=0; i<1000; i++) {
                		message = consumer.receive(1000*60); 
                		if(message == null)
                		{
                			System.out.println("Message receive timeout.");
                			timeout = true;
                			break;
                		}

                		TextMessage textMessage = (TextMessage) message;
                		String text = textMessage.getText();

                		// extracting ip address 

                		//parsing: dividing a string into tokens based on the given delimiters
                		//token: one piece of information, a "word"
                		//delimiter: one (or more) characters used to separate tokens
                		//When we have a situation where strings contain multiple pieces of information 
                		//(for example, when reading in data from a file on a line-by-line basis), 
                		//then we will need to parse (i.e., divide up) the string to extract the individual pieces.
                		//Parsing Strings in Java: Strings in Java can be parsed using the split method of the String class.
                		
                		String delims = "[ - - ]";
                		String[] tokens = text.split(delims);
                		String ip = tokens[0].trim();
                		
                		// update the number of occurrence of the ip
                		if(myMap.containsKey(ip))
                		{
                			Integer count = myMap.get(ip);
                			count++;
                			myMap.put(ip, count);
                		}
                		else
                		{
                			Integer count = 1;
                			myMap.put(ip, count);
                		}
                	}
                	
                	// Third Loop:
                	// iterating over ips in the map
                	// detect attackers based on the ip counts~above threshold
                	// write to output file
                	for (Map.Entry<String, Integer> entry : myMap.entrySet())
                	{

                		Integer THRESHOLD = 3;

                		if ( entry.getValue() >= THRESHOLD)
                		{
                			System.out.println("ip = " + entry.getKey() + "\tcount:" + entry.getValue());
                			outputFile.println("ip = " + entry.getKey() + "\tcount:" + entry.getValue());
                            outputFile.flush();
                		}
                	}
                }
                
                // clean up
                outputFile.close();
                consumer.close();
                session.close();
                connection.close();
                System.out.println("ActiveMQ connection closed.");
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
                
                
            }
        }
 
        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}