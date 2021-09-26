import threading

from rabbitmq import Rabbitmq
import logging
from ast import literal_eval
from time import sleep
import config


class QueueMessagesHandler:

    def run(self):
        self.create_queue(config.QUEUE_IN)
        self.create_queue(config.QUEUE_OUT)
        with Rabbitmq() as rmq:
            while True:
                self.check_thread_count()
                message = rmq.get_awaiting_from_queue(queue=config.QUEUE_IN)
                threading.Thread(target=self.message_handler(literal_eval(message)))

    def message_handler(self, message: dict):
        chat_id = message["message"]["recipient"]["chat_id"]
        json_init = {"text": message["message"]["body"]["text"],
                     "format": "markdown"}
        self.send_messages_to_queue({"chat_id": chat_id, "jsn": json_init})

    def check_thread_count(self) -> None:
        while True:
            if len(threading.enumerate()) - 1 < config.THREADS_COUNT:
                return
            sleep(0.1)

    def send_messages_to_queue(self, message):
        with Rabbitmq() as rmq:
            rmq.send_to_queue(queue=config.QUEUE_OUT, msg=message)

    def create_queue(self, queue_name):
        with Rabbitmq() as rmq:
            try:
                rmq.create_queue(queue=queue_name, ttl_hours=config.QUEUE_TTL_MESSAGE)
                logging.info(f"Queue {queue_name} is created")
            except ValueError:
                pass
            else:
                logging.error("Error: RabbitMQ is broken")
                raise Exception


if __name__ == "__main__":
    h = QueueMessagesHandler()
    h.run()
