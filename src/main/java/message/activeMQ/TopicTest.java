package message.activeMQ;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Test;

/**
 * ActiveMQ 消息的传递方式二：Topic（广播形式） 
 * 与点到点形式不同的是： 
 * 1.在发送消息前必须有消费者在等待，否则发送的消息会丢失
 * 2.可以有多个消费者接受同一条消息
 */
public class TopicTest {

	// ActiveMQ服务的地址
	public static final String brokerURL = "tcp://192.168.5.205:61616";

	/**
	 * 生产者
	 */
	@Test
	public void TopicProducer() throws Exception {
		// 1、创建一个连接工厂对象，需要指定服务的ip及端口(即activeMQ所安装的地址)。
		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerURL);
		// 2、使用工厂对象创建一个Connection对象。
		Connection connection = activeMQConnectionFactory.createConnection();
		// 3、开启连接，调用Connection对象的start方法。
		connection.start();
		// 4、创建一个Session对象。
		// 参数1:是否开启事务。如果true开启事务，第二个参数无意义。一般不开启事务false。
		// 参数2：应答模式。自动应答或者手动应答。一般自动应答。
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// 5、使用Session对象创建一个Destination对象。两种形式queue、topic，现在应该使用queue
		Topic topic = session.createTopic("test-topic");
		// 6、使用Session对象创建一个Producer对象。
		MessageProducer producer = session.createProducer(topic);
		// 7、创建一个Message对象，可以使用TextMessage
		TextMessage textMessage = new ActiveMQTextMessage();
		textMessage.setText("my destination is 'test-topic' ,so i want to go 'test-topic'");
		// 8、发送消息
		producer.send(textMessage);
		// 9、关闭资源
		producer.close();
		session.close();
		connection.close();
	}
	
	/**
	 * 消费者
	 */
	@Test
	public void TopicConsumer() throws Exception {
		// 1、创建一个ConnectionFactory对象连接MQ服务器
		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerURL);
		// 2、使用工厂对象创建一个Connection对象。
		Connection connection = activeMQConnectionFactory.createConnection();
		// 3、开启连接，调用Connection对象的start方法。
		connection.start();
		// 4、创建一个Session对象。
		// 参数1:是否开启事务。如果true开启事务，第二个参数无意义。一般不开启事务false。
		// 参数2：应答模式。自动应答或者手动应答。一般自动应答。
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// 5、使用Session对象创建一个Destination对象。两种形式queue、topic，现在应该使用queue（和发送端的类型、名称保持一致）
		Topic topic= session.createTopic("test-topic");
		// 6、使用Session对象创建一个消费者对象。
		MessageConsumer consumer = session.createConsumer(topic);
		// 7、创建一个消息监听者
		consumer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				// 打印结果
				TextMessage textMessage = (TextMessage) message;
				String text = "";
				try {
					text = textMessage.getText();
					System.out.println(text);
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		});
		System.out.println("consumer2启动");
		// 等待接收消息(敲击键盘才会向下执行)
		System.in.read();
		// 关闭资源
		consumer.close();
		session.close();
		connection.close();
	}
}
