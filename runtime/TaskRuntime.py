import thread
import threading
import time
import task
import json
import logging
import pika
import traceback

class Stat(object):
    def __init__(self, interval = 5):
        self._start_time = time.time()
        self._process_count = 0
        self._interval = interval
        
    def increase(self, count=1):
        self._process_count += count
        
        now = time.time()
        if now - self._start_time > self._interval:
            self.show()
            self.reset()
        
    def show(self):
        print "%s msg processed, %s/s" % (self._process_count, round(self._process_count / (time.time() - self._start_time)))
    
    def reset(self):
        self._start_time = time.time()
        self._process_count = 0

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
        self._stat = Stat()

        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host = self.config['broker_host'],
            virtual_host = self.config['broker_vhost'],
            credentials = pika.PlainCredentials(self.config['broker_username'], 
                self.config['broker_password'])
            ))
        channel = connection.channel()
        channel.exchange_declare(exchange=self.config['broker_exchange'], type='topic',
            durable=True, auto_delete=False)
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
        
    def start(self):
        channel = self.channel
        channel.basic_consume(self.on_message, queue = self.config['subscript_queue'], no_ack=False)
        self._stat.reset()
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
            channel.cancel()
        channel.connection.close()
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
                    self._stat.increase(1)
                    channel.basic_ack(delivery_tag=delivery_tag)
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
                self.task.stop()
                raise SystemExit("Error in task!")
                
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

