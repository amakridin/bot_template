from urllib.parse import urljoin
import requests
from rabbitmq import Rabbitmq
import logging
import send2tamtam
import threading
from time import sleep
from ast import literal_eval

logging.basicConfig(filename="bot.log", level=logging.INFO)



class GetTamTamData:
    TOKEN = "f8ZvlzlxnVUr48QLxu5FrzldYfs6P_6kwGFeYjoHdNA"
    TAM_TAM_URL = "https://botapi.tamtam.chat"
    QUEUE_IN = "common_in"
    QUEUE_OUT = "common_out"
    QUEUE_TTL_MESSAGE = 1  # 1 hour
    REQUEST_TIMEOUT = 30  # 30 sec

    def run(self):
        self.create_queue(self.QUEUE_IN)
        self.create_queue(self.QUEUE_OUT)
        threading.Thread(target=self.read_messages_from_tamtam).start()
        threading.Thread(target=self.send_message_to_chat).start()

    def read_messages_from_tamtam(self):
        marker = 0
        while True:
            try:
                r = requests.get(url=self.get_tamram_url(marker=marker),
                                 timeout=self.REQUEST_TIMEOUT, stream=True).json()
                marker = r['marker']
                self.send_messages_to_queue(messages=r)
            except Exception as ex:
                logging.error(ex.__str__())

    def send_message_to_chat(self):
        while True:
            self.check_thread_count()
            with Rabbitmq() as rmq:
                jsn = literal_eval(rmq.get_awaiting_from_queue(self.QUEUE_OUT))
            send2tamtam.send_json(chat_id=jsn["chat_id"], jsn=jsn["jsn"], token=self.TOKEN)

    def get_tamram_url(self, marker: int = 0) -> str:
        if marker == 0:
            return urljoin(self.TAM_TAM_URL, f"updates?access_token={self.TOKEN}")
        return urljoin(self.TAM_TAM_URL, f"updates?access_token={self.TOKEN}&marker={marker}")

    def create_queue(self, queue_name):
        with Rabbitmq() as rmq:
            try:
                rmq.create_queue(queue=queue_name, ttl_hours=self.QUEUE_TTL_MESSAGE)
                logging.info(f"Queue {queue_name} is created")
            except ValueError:
                pass
            else:
                logging.error("Error: RabbitMQ is broken")

    def send_messages_to_queue(self, messages):
        with Rabbitmq() as rmq:
            for message in messages['updates']:
                rmq.send_to_queue(queue=self.QUEUE_IN, msg=message)
                logging.info(message)

    def check_thread_count(self, threads_limit: int = 10) -> None:
        while True:
            if len(threading.enumerate()) + 2 < threads_limit:
                return
            sleep(0.1)



if __name__ == "__main__":
    get_data = GetTamTamData()
    get_data.run()