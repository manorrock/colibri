# Manorrock Colibri JMS implementation

The Manorrock Colibri JMS implementation delivers the implementation to use if
you want to send to a JMS server and receive from a JMS server.

## Example Usage

```java
import com.sun.messaging.ConnectionFactory;

public class JmsExample {
    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        try (JmsTextMessageEventSender<String> sender
                = new JmsTextMessageEventSender<>(connectionFactory, "colibri")) {
            sender.send("Send me");
        }
        try (JmsTextMessageEventReceiver<String> receiver
                = new JmsTextMessageEventReceiver<>(connectionFactory, "colibri")) {
            String event = receiver.receive();
            System.out.println("Received event: " + event);
        }
    }
}
