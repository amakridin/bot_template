import threading

from rabbitmq import Rabbitmq
import logging
from ast import literal_eval
from time import sleep


class QueueMessagesHandler:
    QUEUE_IN = "common_in"
    QUEUE_OUT = "common_out"
    QUEUE_TTL_MESSAGE = 1  # 1 hour

    def run(self):
        self.create_queue(self.QUEUE_IN)
        self.create_queue(self.QUEUE_OUT)
        with Rabbitmq() as rmq:
            while True:
                self.check_thread_count()
                message = rmq.get_awaiting_from_queue(queue=self.QUEUE_IN)
                threading.Thread(target=self.message_handler(literal_eval(message)))

    def message_handler(self, message: dict):
        chat_id = message["message"]["recipient"]["chat_id"]
        json_init = {"text": message["message"]["body"]["text"],
                     "format": "markdown"}
        self.send_messages_to_queue({"chat_id": chat_id, "jsn": json_init})

    def check_thread_count(self, threads_limit: int = 10) -> None:
        while True:
            if len(threading.enumerate()) + 1 < threads_limit:
                return
            sleep(0.1)

    def send_messages_to_queue(self, message):
        with Rabbitmq() as rmq:
            rmq.send_to_queue(queue=self.QUEUE_OUT, msg=message)

    def create_queue(self, queue_name):
        with Rabbitmq() as rmq:
            try:
                rmq.create_queue(queue=queue_name, ttl_hours=self.QUEUE_TTL_MESSAGE)
                logging.info(f"Queue {queue_name} is created")
            except ValueError:
                pass
            else:
                logging.error("Error: RabbitMQ is broken")
                raise Exception


if __name__ == "__main__":
    h = QueueMessagesHandler()
    h.run()
