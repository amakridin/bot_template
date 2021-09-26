import pika
import requests
from requests.auth import HTTPBasicAuth
from typing import Any
import config


class Rabbitmq:
    def __init__(self, host: str = config.RABBITMQ_SERVER,
                 login: str = config.RABBITMQ_LOGIN, password: str = config.RABBITMQ_PASSWORD):
        self.__host = host
        self.__login = login
        self.__password = password
        self.__create_connection()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def create_queue(self, queue, ttl_hours=None):
        if not self.connection.is_open:
            self.__create_connection()
        if not self.__check_queue_exists(queue=queue):
            if ttl_hours:
                self.channel.queue_declare(queue=queue, arguments={'x-message-ttl': 1000 * 60 * 60 * ttl_hours})
            else:
                self.channel.queue_declare(queue=queue)
            return True
        raise ValueError(f"queue <{queue}> already exists")

    def send_to_queue(self, queue: str, msg: Any):
        if not self.connection.is_open:
            self.__create_connection()
        # self.queue_create(queue=queue)
        try:
            self.channel.basic_publish(exchange='', routing_key=queue, body=str(msg))
            status = {"status": "ok"}
        except Exception as ex:
            status = {"error": ex.__str__()}
        return status

    def get_awaiting_from_queue(self, queue):
        if not self.connection.is_open:
            self.__create_connection()
        ret_body = None

        def callback(ch, method, properties, body):
            nonlocal ret_body
            ret_body = body
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.channel.stop_consuming()
        self.channel.basic_consume(on_message_callback=callback, queue=queue, auto_ack=False)
        self.channel.start_consuming()
        return ret_body.decode('utf8')

    def get_now_from_queue(self, queue):
        if not self.connection.is_open:
            self.__create_connection()
        channel = self.connection.channel()
        method_frame, header_frame, body = channel.basic_get(queue=queue)
        if method_frame:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            return body.decode('utf-8')
        else:
            return None

    def get_queue_msg_count(self, queue):
        if not self.connection.is_open:
            self.__create_connection()
        q = self.channel.queue_declare(queue=queue)
        return q.method.message_count

    def close_connection(self):
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()

    def __create_connection(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.__host,
                                      credentials=pika.PlainCredentials(self.__login, self.__password)))
        self.channel = self.connection.channel()

    def __check_queue_exists(self, queue):
        res = requests.get(url=f'http://{self.__host}:15672/api/queues',
                           auth=HTTPBasicAuth(self.__login, self.__password),
                           verify=False).json()
        for row in res:
            if queue == row['name']:
                return True
        return False


if __name__ == "__main__":
    with Rabbitmq() as rmq:
        queue = 'common_in'
        # try:
        #     rmq.create_queue(queue=queue, ttl_hours=1)
        # except ValueError:
        #     pass
        rmq.send_to_queue(queue=queue, msg="hello")
        msg = rmq.get_awaiting_from_queue(queue)
        print(msg)
