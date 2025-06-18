#!/usr/bin/env python
import pika, sys, uuid

# 基本操作
def init_rabbit_mq(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create a channel
    channel = connection.channel()
    # Declare a queue named 'hello' if it doesn't already exist
    channel.queue_declare(queue='hello')
    # Publish a message to the 'hello' queue
    channel.basic_publish(exchange='', routing_key='hello', body=message)
    print("[x] Sent '{}'".format(message))
    # Close the connection and channel
    connection.close()

# 持久化队列
def init_durable_queue(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create a channel
    channel = connection.channel()
    # Declare a queue named 'task_queue' if it doesn't already exist
    # durable=True 和 persistent 确保即使 RabbitMQ 重新启动，task_queue 队列也不会丢失。
    channel.queue_declare(queue='task_queue', durable=True)
    channel.basic_publish(exchange='', 
                          routing_key='task_queue', 
                          body=message, 
                          properties = pika.BasicProperties(
                              delivery_mode=pika.DeliveryMode.Persistent #将消息标记为持久性（值为 2）或瞬态（任何其他值）。
                        ))
    print("[x] Sent '{}'".format(message))
    # Close the connection and channel
    connection.close()
    
# 日志系统，将日志消息广播给多个接收者。
def emit_log(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # 创建 fanout 交换机
    channel.exchange_declare(exchange='logs', exchange_type='fanout')
    # 发送消息到 logs 交换机
    channel.basic_publish(exchange='logs', routing_key='', body=message)
    print("[x] Sent '{}' to logs exchange".format(message))
    connection.close();

# 日志系统，将不同级别的日志广播到相应的的队列。
def emit_log_direct():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # 创建 direct 交换机
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    
    severity = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] == 'critical' or sys.argv[1] == "error" or sys.argv[1] == 'warning' else 'info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'
    
    channel.basic_publish(exchange='direct_logs', routing_key=severity, body=message)
    print(f" [x] Sent {severity}:{message}")
    connection.close()
    
def emit_log_topic():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # 创建 topic 交换机
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
    routing_key = sys.argv[1] if len(sys.argv) > 1 else 'anonymous.info'
    message =''.join(sys.argv[2:]) or 'Hello World!'
    channel.basic_publish(exchange='topic_logs', routing_key=routing_key, body=message)
    print(f" [x] Sent {routing_key}:{message}")
    channel.close()
 
class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        
        self.callback_queue = result.method.queue
        self.channel.basic_consume(queue=self.callback_queue,
                                   on_message_callback=self.on_response,
                                   auto_ack=True)
        
        self.response = None
        self.corr_id = None
        
    def on_response(self, ch, method, props, body):
        # 对于每个响应消息，它都会检查 correlation_id 是否是我们正在寻找的那个。如果是，它会将响应保存在 self.response 中并中断消费循环。
        if self.corr_id == props.correlation_id:
            self.response = body
         
    # 执行实际的 RPC 请求
    def call(self, n):
        self.response = None
        # 生成唯一的 ID 用于标识请求和响应
        self.corr_id = str(uuid.uuid4())
        # 发布请求消息，其中包含两个属性：reply_to 和 correlation_id。
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties = pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.corr_id,
                                   ),
                                   body=str((n))
                                   )
        # 不断调用 process_data_events，让程序持续处理网络事件，同时等待 self.response 被赋值（通常是在某个回调函数中赋值），从而实现异步等待响应的效果，避免程序阻塞或卡死。
        while self.response is None:
            self.connection.process_data_events(time_limit=None)
        
        return int(self.response)
        
        
def rpc_request():
    fibonacci_rpc = FibonacciRpcClient()

    print(" [x] Requesting fib(500)")
    response = fibonacci_rpc.call(500)
    print(f" [.] Got {response}")
        
if __name__ == '__main__':
    message = ' '.join(sys.argv[1:]) or "info: Hello World!"
    # init_rabbit_mq(message)
    # init_durable_queue(message)
    # emit_log(message)
    # emit_log_direct() # python .\producer.py error this is an error string. 
    # emit_log_topic() # python .\producer.py quick.orange.rabbit 
    rpc_request()
    

# 消息持久性
# 将消息标记为持久性并不能完全保证消息不会丢失。尽管它告诉 RabbitMQ 将消息保存到磁盘，但在 RabbitMQ 接受消息但尚未保存消息时，仍然存在一个短暂的时间窗口。此外，RabbitMQ 不会对每条消息执行 fsync(2) -- 它可能只是保存到缓存，而不是真正写入磁盘。持久性保证并不强，但对于我们简单的任务队列来说已经足够了。如果您需要更强的保证，那么您可以使用发布者确认。
# rabbitmqctl.bat list_queues messages_ready messages_unacknowledged

# 公平分发
# 您可能已经注意到，分发仍然无法完全按照我们的意愿工作。例如，在有两个工作进程的情况下，当所有奇数消息都很重而偶数消息很轻时，一个工作进程将一直很忙，而另一个工作进程几乎不做任何工作。好吧，RabbitMQ 对此一无所知，仍然会均匀地分发消息。
# 发生这种情况是因为 RabbitMQ 只是在消息进入队列时分发消息。它不查看消费者的未确认消息数量。它只是盲目地将每第 n 条消息分发给第 n 个消费者。
# 为了克服这个问题，我们可以使用 Channel#basic_qos 通道方法，并将 prefetch_count=1 设置为 1。这使用 basic.qos 协议方法来告诉 RabbitMQ 一次不要给一个工作进程超过一条消息。或者，换句话说，在工作进程处理并确认上一条消息之前，不要向其分发新消息。相反，它会将其分发给下一个尚未繁忙的工作进程。
# channel.basic_qos(prefetch_count=1)
# 如果所有工作进程都很忙，您的队列可能会填满。您需要密切关注这一点，并可能添加更多工作进程，或使用消息 TTL(https://rabbitmq.org.cn/docs/ttl)。

# RabbitMQ 消息传递模型中的核心思想是生产者永远不会直接向队列发送任何消息。实际上，生产者通常甚至不知道消息是否会被传递到任何队列。
# 相反，生产者只能将消息发送到交换机。交换机是一个非常简单的东西。一方面，它从生产者接收消息，另一方面，它将消息推送到队列。交换机必须确切地知道如何处理它接收到的消息。应该将其附加到特定队列吗？应该将其附加到多个队列吗？还是应该将其丢弃。这些规则由交换机类型定义。几种交换机类型可用：direct、topic、headers 和 fanout。<rabbitmqctl list_exchanges>(列出交换机)

# 主题绑定键 Topic
# 发送到 topic 交换机的消息不能具有任意的 routing_key - 它必须是由点分隔的单词列表。单词可以是任何内容，但通常它们指定与消息相关的某些特征。一些有效的路由键示例：stock.usd.nyse、nyse.vmw、quick.orange.rabbit。路由键中可以包含任意数量的单词，最多 255 个字节。
# 绑定键也必须采用相同的形式。topic 交换机背后的逻辑类似于 direct 交换机 - 使用特定路由键发送的消息将传递到所有绑定了匹配绑定键的队列。但是，绑定键有两个重要的特殊情况
# *（星号）可以代替一个单词。
# #（井号）可以代替零个或多个单词。
# 在这个例子中，我们将发送所有描述动物的消息。消息将使用由三个单词（两个点）组成的路由键发送。路由键中的第一个单词将描述速度，第二个单词描述颜色，第三个单词描述物种：<celerity>.<colour>.<species>。
# 我们创建了三个绑定：Q1 绑定了绑定键 *.orange.*，Q2 绑定了 *.*.rabbit 和 lazy.#
# 这些绑定可以总结为
# Q1 对所有橙色动物感兴趣。
# Q2 想听到关于兔子的一切，以及关于懒惰动物的一切。
# 路由键设置为 quick.orange.rabbit 的消息将传递到两个队列。消息 lazy.orange.elephant 也将同时发送到这两个队列。另一方面，quick.orange.fox 将仅发送到第一个队列，而 lazy.brown.fox 仅发送到第二个队列。lazy.pink.rabbit 将仅传递到第二个队列一次，即使它匹配两个绑定。quick.brown.fox 不匹配任何绑定，因此将被丢弃。
# 如果我们违反约定并发送一个或四个单词的消息，例如 orange 或 quick.orange.new.rabbit 会发生什么？好吧，这些消息将不匹配任何绑定，并且会丢失。

# 主题交换机
# 主题交换机功能强大，可以像其他交换机一样工作。
# 当队列绑定了 #（井号）绑定键时 - 它将接收所有消息，而不管路由键如何 - 就像 fanout 交换机一样。
# 当绑定中未使用特殊字符 *（星号）和 #（井号）时，主题交换机的行为就像 direct 交换机一样。

# 消息属性
# AMQP 0-9-1 协议预定义了一组与消息相关的 14 个属性。大多数属性很少使用，以下属性除外
# delivery_mode：将消息标记为持久性（值为 2）或瞬态（任何其他值）。您可能还记得第二个教程中的这个属性。
# content_type：用于描述编码的 mime 类型。例如，对于常用的 JSON 编码，最好将此属性设置为：application/json。
# reply_to：通常用于命名回调队列。
# correlation_id：用于将 RPC 响应与请求关联起来。

# RPC 将像这样工作
# 当客户端启动时，它会创建一个独占回调队列。
# 对于 RPC 请求，客户端发送一条消息，其中包含两个属性：reply_to，设置为回调队列；correlation_id，设置为每个请求的唯一值。
# 请求被发送到 rpc_queue 队列。
# RPC 工作者（又名：服务器）正在等待该队列上的请求。当请求出现时，它会完成工作并将结果消息发送回客户端，使用来自 reply_to 字段的队列。
# 客户端等待回调队列上的数据。当消息出现时，它会检查 correlation_id 属性。如果它与请求中的值匹配，则将响应返回给应用程序。