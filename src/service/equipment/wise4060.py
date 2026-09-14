import hashlib
import re
from dataclasses import dataclass

import httpx


@dataclass
class DIChannel:
    ch: int
    mode: int
    val: int
    stat: int  # 0=LOW, 1=HIGH
    cnting: int
    ovlch: int


@dataclass
class DOChannel:
    ch: int
    mode: int
    val: int
    stat: int  # 0=LOW, 1=HIGH
    ps_ctn: int
    ps_stop: int
    ps_iv: int


class WISE4060:
    """
    Async HTTP client for the Advantech WISE-4060 IO module used to drive the elevator relays.

    Call ``await login()`` once after construction; the underlying httpx client keeps the
    session cookie alive across calls, the same way the WISE-4060 web UI expects.
    """

    def __init__(
        self, ip: str, username: str = 'root', password: str = '00000000', timeout: float = 3.0
    ):
        self.ip = ip
        self.base_url = f'http://{ip}'
        self.username = username
        self.password = password
        self.timeout = timeout
        self._client = httpx.AsyncClient(timeout=timeout)

    async def login(self) -> None:
        resp = await self._client.get(f'{self.base_url}/')
        match = re.search(r'name="seeddata"\s*value="([^"]+)"', resp.text)
        if not match:
            raise RuntimeError(f'[{self.ip}] login failed: seeddata not found')
        seeddata = match.group(1)
        raw = f'{seeddata}:{self.username}:{self.password[:8]}'
        authdata = hashlib.md5(raw.encode()).hexdigest()
        await self._client.post(
            f'{self.base_url}/index.html',
            data={'seeddata': seeddata, 'authdata': authdata},
        )
        if 'adamsessionid' not in self._client.cookies:
            raise RuntimeError(f'[{self.ip}] login failed: invalid username/password')

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str) -> dict:
        resp = await self._client.get(f'{self.base_url}{path}')
        resp.raise_for_status()
        return resp.json()

    async def _put(self, path: str, data: dict) -> None:
        resp = await self._client.put(f'{self.base_url}{path}', json=data)
        resp.raise_for_status()

    async def _patch(self, path: str, data: dict) -> None:
        resp = await self._client.patch(f'{self.base_url}{path}', json=data)
        resp.raise_for_status()

    # ── DI ──────────────────────────────────────────────

    async def get_all_di(self) -> list[DIChannel]:
        data = await self._get('/di_value/slot_0')
        return [
            DIChannel(
                ch=ch['Ch'],
                mode=ch['Md'],
                val=ch['Val'],
                stat=ch['Stat'],
                cnting=ch['Cnting'],
                ovlch=ch['OvLch'],
            )
            for ch in data['DIVal']
        ]

    async def get_di(self, ch: int) -> DIChannel:
        data = await self._get(f'/di_value/slot_0/ch_{ch}')
        return DIChannel(
            ch=data['Ch'],
            mode=data['Md'],
            val=data['Val'],
            stat=data['Stat'],
            cnting=data['Cnting'],
            ovlch=data['OvLch'],
        )

    async def is_di_high(self, ch: int) -> bool:
        return (await self.get_di(ch)).stat == 1

    # ── DO ──────────────────────────────────────────────

    async def get_all_do(self) -> list[DOChannel]:
        data = await self._get('/do_value/slot_0')
        return [
            DOChannel(
                ch=ch['Ch'],
                mode=ch['Md'],
                val=ch['Val'],
                stat=ch['Stat'],
                ps_ctn=ch['PsCtn'],
                ps_stop=ch['PsStop'],
                ps_iv=ch['PsIV'],
            )
            for ch in data['DOVal']
        ]

    async def get_do(self, ch: int) -> DOChannel:
        data = await self._get(f'/do_value/slot_0/ch_{ch}')
        return DOChannel(
            ch=data['Ch'],
            mode=data['Md'],
            val=data['Val'],
            stat=data['Stat'],
            ps_ctn=data['PsCtn'],
            ps_stop=data['PsStop'],
            ps_iv=data['PsIV'],
        )

    async def set_do(self, ch: int, on: bool) -> None:
        val = 1 if on else 0
        await self._patch(f'/do_value/slot_0/ch_{ch}', {'Ch': ch, 'Val': val})

    async def set_all_do(self, states: list[bool]) -> None:
        """states: list of 4 bools, index = channel number"""
        await self._put(
            '/do_value/slot_0',
            {
                'DOVal': [
                    {
                        'Ch': i,
                        'Md': 0,
                        'Val': 1 if on else 0,
                        'Stat': 1 if on else 0,
                        'PsCtn': 0,
                        'PsStop': 0,
                        'PsIV': 0,
                    }
                    for i, on in enumerate(states)
                ]
            },
        )

    async def turn_on(self, ch: int) -> None:
        await self.set_do(ch, True)

    async def turn_off(self, ch: int) -> None:
        await self.set_do(ch, False)

    async def check_alive(self) -> bool:
        """Lightweight connectivity probe used before each elevator request."""
        try:
            await self.get_all_di()
            return True
        except (httpx.HTTPError, ValueError):
            return False


__all__ = ['WISE4060', 'DIChannel', 'DOChannel']
