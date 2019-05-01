package message.activeMQ;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

/**
 * Spring 整合ActiveMQ
 *
 */
public class QueueSpringTest {

	public static final ApplicationContext applicationContext1 = new ClassPathXmlApplicationContext(
			"classpath:applicationContext-activemq-send.xml");
	public static final ApplicationContext applicationContext2 = new ClassPathXmlApplicationContext(
			"classpath:applicationContext-activemq-receive.xml");

	/**
	 * 发送方
	 */
	@Test
	public void QueueProducer() throws Exception {
		// 从容器中获得JmsTemplate对象(负责发送或接收消息)
		JmsTemplate jmsTemplate = applicationContext1.getBean(JmsTemplate.class);
		// 从容器中获得一个Destination对象
		Queue queue = (Queue) applicationContext2.getBean("queueDestination");
		// 获得一个消息对象
		MessageCreator messageCreator = getMessageCreator();
		// 发送消息
		jmsTemplate.send(queue, messageCreator);

	}
    
	/**
	 * 消费方
	 */
	@Test
	public void QueueConsumer() throws Exception {
		// 初始化Spring容器后，等待即可
		System.in.read();
	}
    
    
	// 获得一个消息对象
	private MessageCreator getMessageCreator() {
		MessageCreator messgeCreator = new MessageCreator() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				Message message = session.createTextMessage("send message");
				return message;
			}
		};
		return messgeCreator;
	}
}
