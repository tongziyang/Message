package message.activeMQ.withItself;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Test;

/**
 * activeMQ 消息的传递方式一：Queue（点到点）
 * 消息类型： 
 * StreamMessage -- Java原始值的数据流 
 * MapMessage--一套名称-值对 
 * TextMessage--一个字符串对象 
 * ObjectMessage--一个序列化的 Java对象 
 * BytesMessage--一个字节的数据流
 * 
 * @author Administrator
 *
 */
public class QueueTest {

	public static final String brokerURL="tcp://192.168.5.205:61616";
	
	
	/**
	 * 生产者
	 */
	@Test
	public void QueueProducer() throws Exception {
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
		Queue queue = session.createQueue("queue1");
		// 6、使用Session对象创建一个Producer对象。
		MessageProducer producer = session.createProducer(queue);
		// 7、创建一个Message对象，可以使用TextMessage
		TextMessage textMessage = new ActiveMQTextMessage();
		textMessage.setText("my destination is 'queue1' ,so i want to go 'queue1'");
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
	public void QueueConsumer() throws Exception {
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
		Queue queue = session.createQueue("queue1");
		// 6、使用Session对象创建一个消费者对象。
		MessageConsumer consumer = session.createConsumer(queue);
		// 7、创建一个消息监听者
		consumer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				// 打印结果
				TextMessage textMessage = (TextMessage) message;
				String text="";
				try {
					text = textMessage.getText();
					System.out.println(text);
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		});
		// 等待接收消息(敲击键盘才会向下执行)
		System.in.read();
		// 关闭资源
		consumer.close();
		session.close();
		connection.close();
	}
}
