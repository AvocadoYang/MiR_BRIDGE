import asyncio

import httpx
import reactivex
from reactivex import Subject, timer
from reactivex.operators import do_action, switch_map
from reactivex.scheduler.eventloop import AsyncIOScheduler
from reactivex.subject import BehaviorSubject

from src.logger import heartbeat_logger, logger
from src.service.rabbitmq import HEARTBEAT, Rabbit_client_async
from src.service.rabbitmq.queues import HEARTBEAT_EX
from src.service.rabbitmq.transaction_wrapper import Heartbeat_Response
from src.service.webService.httpx_set import headers

from ..type import AMR_INFO


class Heartbeat:
    def __init__(
        self,
        amr_info: AMR_INFO,
        receive_request_record: dict[str, str],
        rabbit_service: Rabbit_client_async,
        heartbeat_sub: Subject[HEARTBEAT],
    ):
        self.scheduler = AsyncIOScheduler(asyncio.get_running_loop())

        self.amr_info = amr_info
        self.rb = rabbit_service
        self.receive_request_record = receive_request_record

        self.start_heartbeat_watchdog: Subject[bool] = Subject()
        self.qams_timeout_signal: Subject[bool] = Subject()
        self.__heartbeat_timer_update: BehaviorSubject[bool] = BehaviorSubject(True)

        heartbeat_sub.subscribe(self.__heartbeat_with_qams)
        self.start_heartbeat_watchdog.pipe(
            switch_map(lambda action: self.__qams_heartbeat_watch_dog(action))
        ).subscribe()

    def __heartbeat_with_qams(self, action: HEARTBEAT):
        payload = action['payload']
        heartbeat = payload['heartbeat']
        heartbeat_logger.bind(title=self.amr_info.amrId, state='heartbeat').info(
            f'Receive heartbeat  from QAMS {payload}'
        )
        res_heartbeat = heartbeat + 1 if (heartbeat + 1) <= 9999 else 0
        payload = action['payload']
        asyncio.create_task(
            self.wait_heartbeat_response(res_heartbeat=res_heartbeat, action=action)
        )

    async def wait_heartbeat_response(self, res_heartbeat: int, action: HEARTBEAT):
        try:
            async with httpx.AsyncClient() as client:
                url = f'http://{self.amr_info.ip}/api/v2.0.0/users/me'
                await client.get(url=url, headers=headers, timeout=2)
                self.__heartbeat_timer_update.on_next(True)
        except (httpx.HTTPStatusError, Exception) as e:
            print(e)
        payload = action['payload']
        heartbeat_res = Heartbeat_Response(
            id=payload['id'],
            cmd_id=payload['cmd_id'],
            amrId=payload['amrId'],
            heartbeat=res_heartbeat,
        )
        await self.rb.res_publish(
            exchange_name=HEARTBEAT_EX,
            routing_key=f'qams.heartbeat.pong.{self.amr_info.mac_address}',
            last_receive_req=self.receive_request_record,
            mac_address=self.amr_info.mac_address,
            message=heartbeat_res,
        )

    def __qams_heartbeat_watch_dog(self, action: bool):
        if action:
            logger.bind(title=self.amr_info.amrId).info(
                'connect with QAMS, start heartbeat detection'
            )
            return self.__heartbeat_timer_update.pipe(
                switch_map(lambda _: timer(5.0, 5.0, scheduler=self.scheduler)),
                do_action(lambda _: self.qams_timeout_process()),
            )
        return reactivex.empty()

    def qams_timeout_process(self):
        logger.warning('(QAMS) heartbeat timeout, disconnect')
        self.qams_timeout_signal.on_next(True)
