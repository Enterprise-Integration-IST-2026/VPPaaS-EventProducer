import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer; 
import org.apache.kafka.clients.producer.ProducerRecord; 
import org.apache.kafka.common.PartitionInfo;


public class VPPaaSMessageProvider {

	static String brokerList = "localhost:9092";
	static int throughput = 10;
	static String filterprefix = "";
	
	static Map<String, List<PartitionInfo> > topics;

	private static String RandomTopic() 
	{
		String Topic = new String("");
		int index = (new Random()).nextInt(topics.size());		
		Set<String> keys = topics.keySet();
		Iterator<String> it = keys.iterator();
		for (int idx= 0 ; idx < index ;idx++) it.next();
		Topic =  (String) it.next();
		System.out.println("Topic randomized: " + Topic);
		return Topic;
	}
	
	private static Message CreateMessage( String topicToSend , Timestamp ts)
	{
		if ( topicToSend.contains("-") == false ) return(null); // AssetLinkId-at-UtilityOperator not conformed

		Message newMessage = null;

		String[] GridCells = {
		"TOKYO-NW" , "BERLIN-CE" , "AUSTIN-DT" , "LONDON-SE" , "MUMBAI-WP",
		"SYDNEY-HB" , "MADRID-NE" , "DENVER-MT" , "SEOUL-IT" , "LAGOS-VI" , 
		"PARIS-RG" , "MEXICO-SZ" , "DUBAI-MR" , "CAIRO-ND" , "LISBON-EX",
		"TORONTO-QU" , "VIENNA-BZ" , "SANTOS-IN" , "OSLO-FJ" , "SINGA-GP"};

		switch (ThreadLocalRandom.current().nextInt(0, 3)) {
		case 0:
			newMessage = new BatteryEnergyStorage( 	ts.toLocalDateTime(),
													topicToSend.substring(0, topicToSend.indexOf('-')),
													GridCells[new Random().nextInt(GridCells.length)],
													Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 100.0) ),
													Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 20.0) ),	
													Double.valueOf( ThreadLocalRandom.current().nextDouble(-10.0, 10.0) ),	
													Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 5.0) ),		
													Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 100.0) ),	
													new BatteryEnergyStorage().randomLevel() );
			break;
		case 1:
			newMessage = new SolarInverter( ts.toLocalDateTime(),
											topicToSend.substring(0, topicToSend.indexOf('-')),
											GridCells[new Random().nextInt(GridCells.length)],  
											Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 7.5) ),
											Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 150) ),
											Double.valueOf( ThreadLocalRandom.current().nextDouble(245, 255) ),
											Double.valueOf( ThreadLocalRandom.current().nextDouble(49.5, 50.5) ) );
			break;
		case 2:
			newMessage = new EVCharger(  ts.toLocalDateTime(),
										 topicToSend.substring(0, topicToSend.indexOf('-')),
										 GridCells[new Random().nextInt(GridCells.length)],  
										 Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 20.5) ),
										 Double.valueOf( ThreadLocalRandom.current().nextDouble(0.0, 17.8) ),
										 Double.valueOf( ThreadLocalRandom.current().nextDouble(0, 100) ),
										 new EVCharger().randomLevel() );
			break;									
		}
		return (newMessage);
	}
	
	private static void CheckArguments()
	{
		System.out.println(  
							 "--broker-list=" + brokerList + "\n" +							 
							 "--throughput=" + throughput + "\n" +
							 "--filterprefix=" + filterprefix);
	}
	
	private static boolean VerifyArgs(String[] cabecalho)
	{
		for (int i=0 ; i < cabecalho.length ; i=i+2)
		{
			if (cabecalho[i].compareTo("--broker-list") == 0) brokerList = cabecalho[i+1];
			else if (cabecalho[i].compareTo("--throughput") == 0) throughput = Integer.valueOf(cabecalho[i+1]).intValue();
			else if (cabecalho[i].compareTo("--filterprefix") == 0) filterprefix = cabecalho[i+1]; 
			else 
			{
				System.out.println("Bad argument name: " + cabecalho[i]);
				return(false);
			}
		}		

		if (brokerList.length() == 0) System.out.println ("Broker-list argument is mandatory!");
		else return (true);
			
		return (false);
	}
	
	private static void SendMessage( Message msg ,  KafkaProducer<String, String> prd , String topicTarget)
	{		
		System.out.println("This is the message to send = " + msg.toStringAsJSON());
		String seqkey = new String("");
		seqkey = msg.getSeqkey().toString();		
		System.out.println("Sending new message to Kafka, to the topic = " + topicTarget + ", with key=" + seqkey);	
		ProducerRecord<String, String> record = new ProducerRecord<>(topicTarget, seqkey, msg.toStringAsJSON());		
		prd.send(record);
		System.out.print("Sent...Fire-and-forget stopped...");
	}
	
	private static void CheckTopicsAvailable()
	{
		/*** check all topics in kafka cluster from JAVA  ******/				
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		topics = consumer.listTopics();
		consumer.close();
		
		
		topics.remove("__consumer_offsets");
		System.out.print("Topics discovered = { ");
		for( String topicName : topics.keySet() )	System.out.print(topicName + " ; ");
		System.out.println(" } ");
		/******************************************************/

	}
	public static void main(String[] args) {


		String usage = "The usage of the Message Producer for VPPaaS 2026, for Enterprise Integration 2026 course, is the following.\n" + //
						"\n" + //
						"VPPaaSSimulator --broker-list <brokers> --throughput <value> --filterprefix <value> \n" + //
						"where, \n" + //
						"--broker-list: is a broker list with ports (e.g.: kafka02.example.com:9092,kafka03.example.com:9092), default value is localhost:9092\n" + //
						"--throughput: is the approximate maximum messages to be produced by minute, default value is 10\n" + //
						"--filterprefix: is the prefix to be filtered. Only the topics starting with this prefix will be considered to sending messages.\n";
						
				
		Properties kafkaProps = new Properties();
		if (args.length == 0) System.out.println(usage);
		else 
		{
			if (VerifyArgs(args))
			{		
				System.out.println ("The following arguments are accepted:");
				CheckArguments();
				System.out.println ("------- Processing starting -------");
				
				kafkaProps.put("bootstrap.servers", brokerList); 
				kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer"); 
				kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer"); 
				KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
			
				CheckTopicsAvailable();
				
				Timestamp mili;
				
				while (true)
				{
					try {
						mili = new Timestamp(System.currentTimeMillis());				
						
						if (!topics.isEmpty() )
						{
							String topic_to_send = RandomTopic();
							
							if (topic_to_send.startsWith(filterprefix))
							{							
								Message messageToSend = CreateMessage( topic_to_send , mili );						
								if (messageToSend != null) SendMessage( messageToSend , producer , topic_to_send );
							}
							else System.out.println("Topic = " + topic_to_send + " has been filtered. Therefore, not sending message.");
						}
						else System.out.println("Empty list of Topics. Therefore, no message to send.");
							
						Timestamp timestamp = new Timestamp(System.currentTimeMillis());
						System.out.println("...Time spent for sending: " + (timestamp.getTime() - mili.getTime()) );
						Thread.sleep(60000/throughput);  
						CheckTopicsAvailable();
					}
					catch (Exception e) { e.printStackTrace();}					
				}
			}
			else System.out.println("Application Arguments bad usage.\n\nPlease check syntax.\n\n" + usage);
		}
	}
}
