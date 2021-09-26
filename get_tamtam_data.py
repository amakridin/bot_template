from urllib.parse import urljoin
import requests
from rabbitmq import Rabbitmq
import logging
import send2tamtam
import threading
from time import sleep
from ast import literal_eval
import config

logging.basicConfig(filename="bot.log", level=logging.INFO)



class GetTamTamData:
    def run(self):
        self.create_queue(config.QUEUE_IN)
        self.create_queue(config.QUEUE_OUT)
        threading.Thread(target=self.read_messages_from_tamtam).start()
        threading.Thread(target=self.send_message_to_chat).start()

    def read_messages_from_tamtam(self):
        marker = 0
        while True:
            try:
                r = requests.get(url=self.get_tamram_url(marker=marker),
                                 timeout=config.REQUEST_TIMEOUT, stream=True).json()
                marker = r['marker']
                self.send_messages_to_queue(messages=r)
            except Exception as ex:
                logging.error(ex.__str__())

    def send_message_to_chat(self):
        while True:
            self.check_thread_count()
            with Rabbitmq() as rmq:
                jsn = literal_eval(rmq.get_awaiting_from_queue(config.QUEUE_OUT))
            send2tamtam.send_json(chat_id=jsn["chat_id"], jsn=jsn["jsn"])

    def get_tamram_url(self, marker: int = 0) -> str:
        if marker == 0:
            return urljoin(config.TAM_TAM_URL, f"updates?access_token={config.TAM_TAM_TOKEN}")
        return urljoin(config.TAM_TAM_URL, f"updates?access_token={config.TAM_TAM_TOKEN}&marker={marker}")

    def create_queue(self, queue_name):
        with Rabbitmq() as rmq:
            try:
                rmq.create_queue(queue=queue_name, ttl_hours=config.QUEUE_TTL_MESSAGE)
                logging.info(f"Queue {queue_name} is created")
            except ValueError:
                pass
            else:
                logging.error("Error: RabbitMQ is broken")

    def send_messages_to_queue(self, messages):
        with Rabbitmq() as rmq:
            for message in messages['updates']:
                rmq.send_to_queue(queue=config.QUEUE_IN, msg=message)
                logging.info(message)

    def check_thread_count(self) -> None:
        while True:
            if len(threading.enumerate()) - 2 < config.THREADS_COUNT:
                return
            sleep(0.1)



if __name__ == "__main__":
    get_data = GetTamTamData()
    get_data.run()