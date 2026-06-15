import asyncio

from reactivex import Subject

from src.logger import heartbeat_logger
from src.service.rabbitmq import HEARTBEAT, Rabbit_client_async
from src.service.rabbitmq.queues import HEARTBEAT_EX
from src.service.rabbitmq.transaction_wrapper import send_heartbeat_res

from .type import AMR_INFO


class Heartbeat:
    def __init__(
        self,
        amr_info: AMR_INFO,
        receive_request_record: dict[str, str],
        rabbit_service: Rabbit_client_async,
        heartbeat_sub: Subject[HEARTBEAT],
    ):
        self.amr_info = amr_info
        self.rb = rabbit_service
        self.receive_request_record = receive_request_record
        heartbeat_sub.subscribe(self.__heartbeat_with_qams)

    def __heartbeat_with_qams(self, action: HEARTBEAT):
        payload = action['payload']
        heartbeat = payload['heartbeat']
        req = {'heartbeat': heartbeat}
        heartbeat_logger.bind(title=self.amr_info.amrId, state='heartbeat').info(
            f'Receive heartbeat  from QAMS {payload}'
        )
        # self.__qams_last_heartbeat_time = int(time.time() * 1000)
        res_heartbeat = heartbeat + 1 if (heartbeat + 1) <= 9999 else 0
        payload = action['payload']
        # self.__qams_update_heartbeat.on_next(True)
        asyncio.create_task(
            self.rb.res_publish(
                exchange_name=HEARTBEAT_EX,
                routing_key=f'qams.heartbeat.pong.{self.amr_info.mac_address}',
                last_receive_req=self.receive_request_record,
                mac_address=self.amr_info.mac_address,
                message=send_heartbeat_res(
                    heartbeat=res_heartbeat, id=action['id'], amrId=payload['amrId']
                ),
            )
        )
