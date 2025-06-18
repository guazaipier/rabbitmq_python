#!/usr/bin/env python
import pika, sys, os, time

def init_rabbit_mq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    # rabbitmqctl.bat list_queues messages_ready messages_unacknowledged
    def callback(ch, method, properties, body):
        print(f"[x] Received {body.decode()}")
        time.sleep(body.count(b'.'))
        print(f"[x] Done")
        # 主动ack，表示消费完成;确保即使消费者崩溃，任务也不会丢失。
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 手动确认消息已处理
    channel.basic_consume(queue='hello', on_message_callback=callback)#, auto_ack=True)#手动确认，避免worker fetch后worker没有process
    print('[x] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

# 不绑定 queue 到交换机，直接消费队列中的消息。    
def init_durable_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='logs', exchange_type='fanout')  # 声明交换机
    channel.queue_declare(queue='task_queue', durable=True)  # 确保队列持久化
    print('[x] Waiting for messages. To exit press CTRL+C')
    def callback(ch, method, properties, body):
        print(f"[x] Received {body.decode()}")
        time.sleep(body.count(b'.'))
        print(f"[x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 手动确认消息已处理
    # 公平分发
    channel.basic_qos(prefetch_count=1)  # 设置每个工作进程一次只分发一条消息
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()

# fanout模式，不需要关心 routing_key，所有绑定到交换机的队列都会接收到消息。
def receive_logs():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='logs', exchange_type='fanout')  # 声明交换机
    # 创建一个具有随机名称的队列, exclusive=True 确保队列在连接关闭时被删除; result.method.queue 包含一个随机队列名称
    result = channel.queue_declare(queue='', exclusive=True)
    # 告诉交换机将消息发送到我们的队列。交换机和队列之间的这种关系称为绑定
    channel.queue_bind(exchange='logs', queue=result.method.queue)
    # rabbitmqctl list_bindings 列出绑定
    print(f"Waiting for messages. To exit press CTRL+C. Queue name is {result.method.queue}")
    def callback(ch, method, properties, body):
        print(f"[x] Received {body.decode()}")
    channel.basic_consume(queue=result.method.queue, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
   
# 日志系统，对应队列接收对应级别的日志（direct模式,交换机需要绑定routine_key）
def receive_logs_direct(severities):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')  # 声明交换机
    result = channel.queue_declare(queue='', exclusive=True)  # 创建一个具有随机名称的队列, exclusive=True 确保队列在连接关闭时被删除; result.method.queue 包含一个随机队列名称
    if not severities:
        sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
        sys.exit(1)
    for severity in severities:
        channel.queue_bind(exchange='direct_logs', queue=result.method.queue, routing_key=severity)
    print(f"Waiting for messages. To exit press CTRL+C. Queue name is {result.method.queue}")
    
    def callback(ch, method, properties, body):
        print(f" [x] {method.routing_key}: {body.decode()}")
        
    channel.basic_consume(queue=result.method.queue, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
     
def receive_logs_topic(binding_keys):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')  # 声明交换机
    result = channel.queue_declare(queue='', exclusive=True)  # 创建一个具有随机名称的队列, exclusive=True 确保队列在连接关闭时被删除; result.method.queue 包含一个随机队列名称

    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)
    
    for binding_key in binding_keys:
        channel.queue_bind(exchange='topic_logs', queue=result.method.queue, routing_key=binding_key)
    
    print(f"Waiting for messages. To exit press CTRL+C. Queue name is {result.method.queue}")
    
    def callback(ch, method, properties, body):
        print(f" [x] {method.routing_key}: {body.decode()}")
        
    channel.basic_consume(queue=result.method.queue, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


# rpc 在远程计算机上运行一个函数并等待结果呢？嗯，那又是另一回事了。这种模式通常被称为远程过程调用或 RPC。
# 一个客户端和一个可扩展的 RPC 服务器。由于我们没有任何值得分配的耗时任务，我们将创建一个虚拟 RPC 服务，该服务返回斐波那契数。
def rpc_server():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='rpc_queue')
    
    def fib(n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return fib(n-1) + fib(n-2)
    
    def fib2(n):
        fibs = 0
        for i in range(n):
            if i == 0:
                fibs = 0
            elif i == 1:
                fibs = 1
            else:
                fibs = fibs + i
                
        return fibs

    def on_request(ch, method, properties, body):
        n = int(body)
        print(f" [.] fib({n})")
        response = fib2(n)
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=str(response)
                         )
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 手动确认消息已处理
        
    # 运行多个服务器进程。为了将负载平均分配到多个服务器上，我们需要设置 prefetch_count 设置。
    channel.basic_qos(prefetch_count=1)  # 设置每个工作进程一次只分发一条消息
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
    print('[x] Awaiting RPC requests')
    channel.start_consuming()
  
def main():
    # init_rabbit_mq()
    # init_durable_queue()
    # receive_logs()
    # receive_logs_direct(sys.argv[1:]) # eg: python .\consumer.py info warning
    # receive_logs_topic(sys.argv[1:])  # eg: python .\consumer.py "kern.*" "*.critical"
    rpc_server()  # 启动 RPC 服务器

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
        