import thread
import threading
import time
import task
import json
import logging
import pika
import traceback
import AsyncConsumer
import Queue

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print 'elapsed time: %f ms' % self.msecs

class TaskRuntime:
    
    def init(self, runtime_config, task_config):
        self.config = runtime_config
        self.task = task.Task();
        self.task.init(task_config);

        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host = self.config['broker_host'],
            virtual_host = self.config['broker_vhost'],
            credentials = pika.PlainCredentials(self.config['broker_username'], 
                self.config['broker_password'])
            ))
        channel = connection.channel()
        result = channel.queue_declare(queue = self.config['subscript_queue'],
                              durable=True, auto_delete=False)
        channel.basic_qos(prefetch_count=10)
        qname = result.method.queue
        channel.queue_bind(exchange=self.config['broker_exchange'],
                   queue=qname, routing_key=self.config['subscript_topic'])
        self.config['subscript_queue'] = qname

        self.channel = channel
        self.stat = {
            'messages' : 0,
            'messages_success' : 0,
            'messages_failed' : 0
            }
        
    def stop(self, reason):
        dissubscript()
        self.task.stop()
        if not running:
            exit(0)
    
    def start_consuming(self):
        self.consumer.run()
        
    def start(self):
        #channel = self.channel
        #channel.basic_consume(self.on_message, queue = self.config['subscript_queue'], no_ack=False)
        
        self.consumer = AsyncConsumer.AsyncConsumer('amqp://guest:guest@localhost:5672/%2F','quotation_daily', 'auction', '#')
        import threading
        thread = threading.Thread(target = self.start_consuming)
        thread.start()
        try:
            #channel.start_consuming()
            starttime = time.time()
            message_count = 0
            start_message_count = message_count
            while True:
                try:
                    msg = self.consumer.msg_queue.get(False)
                except Queue.Empty:
                    continue
                if msg is not None:
                    message_count += 1
                    self.on_message2(msg)
                else:
                    print "No data available"
                now = time.time()
                if now - starttime > 5:
                    timespan = now - starttime
                    print "%s msgs processed, avg time %s"%(message_count - start_message_count, (message_count - start_message_count)/timespan)
                    starttime = now
                    start_message_count = message_count
                    
                
                
        except KeyboardInterrupt:
            #channel.stop_consuming()
            #channel.cancel()
            self.consumer.stop()
        channel.connection.close()
    def on_message2(self, msg):
        header = msg['headers']
        data = json.loads(msg['body'])
        ret = self.task.run(header, data)
        if ret:
            self.consumer.ack_queue.put(msg['delivery_tag'])
        
        
    def on_message(self, channel, method_frame, header_frame, body):
        running = True
        if header_frame.headers is None:
            header = {}
        else:
            header = header_frame.headers.copy()
        header['routing_key'] = method_frame.routing_key
        delivery_tag = method_frame.delivery_tag
        data = json.loads(body)
        with Timer(False) as t:
            try:
                ret = self.task.run(header, data)
                if ret:
                    self.stat['messages'] += 1
                    self.stat['messages_success'] += 1
                    #logging.info('messages : %s, success : %d, failed : %d', self.stat['messages'],
                    #         self.stat['messages_success'], self.stat['messages_failed'])
                else:
                    self.stat['messages'] += 1
                    self.stat['messages_failed'] += 1
                    logging.info('messages : %s, success : %d, failed : %d', self.stat['messages'],
                                 self.stat['messages_success'], self.stat['messages_failed'])
            except Exception as err:
                logging.error(err)
                self.stat['messages'] += 1
                self.stat['messages_failed'] += 1
                logging.info('messages : %s, success : %d, failed : %d', self.stat['messages'],
                     self.stat['messages_success'], self.stat['messages_failed'])
                channel.basic_nack(delivery_tag=delivery_tag)
                tb = traceback.format_exc()
                print tb
                raise SystemExit("I need arguments!")
            else:
                channel.basic_ack(delivery_tag=delivery_tag)
            
                
        running = False;
        
        
    def log(self, level, message):
        #send log info to server
        pass

    def send_message(self, topic, header, data):
        #create an AMQP message and send to broker
        pass

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    level=logging.INFO)
    runtime = TaskRuntime()
    with open('runtime.config', 'r') as f:
        runtime_config = json.loads(f.read())
    with open('task.config', 'r') as f:
        task_config = json.loads(f.read())    
    runtime.init(runtime_config, task_config)
    runtime.start()

