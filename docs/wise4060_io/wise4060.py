import hashlib
import re

import requests
from dataclasses import dataclass


@dataclass
class DIChannel:
    ch: int
    mode: int
    val: int
    stat: int      # 0=LOW, 1=HIGH
    cnting: int
    ovlch: int


@dataclass
class DOChannel:
    ch: int
    mode: int
    val: int
    stat: int      # 0=LOW, 1=HIGH
    ps_ctn: int
    ps_stop: int
    ps_iv: int


class WISE4060:
    def __init__(self, ip: str, username: str = "root", password: str = "00000000", timeout: float = 3.0):
        self.base_url = f"http://{ip}"
        self.username = username
        self.password = password
        self.timeout = timeout
        self.session = requests.Session()
        self._login()

    def _login(self) -> None:
        resp = self.session.get(f"{self.base_url}/", timeout=self.timeout)
        match = re.search(r'name="seeddata"\s*value="([^"]+)"', resp.text)
        if not match:
            raise RuntimeError("找不到 seeddata")
        seeddata = match.group(1)
        raw = f"{seeddata}:{self.username}:{self.password[:8]}"
        authdata = hashlib.md5(raw.encode()).hexdigest()
        self.session.post(
            f"{self.base_url}/index.html",
            data={"seeddata": seeddata, "authdata": authdata},
            timeout=self.timeout,
        )
        if "adamsessionid" not in self.session.cookies:
            raise RuntimeError(f"登入失敗，請確認帳密。seeddata={seeddata}, authdata={authdata}")

    def _get(self, path: str) -> dict:
        resp = self.session.get(f"{self.base_url}{path}", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, data: dict) -> None:
        resp = self.session.put(
            f"{self.base_url}{path}",
            json=data,
            timeout=self.timeout,
        )
        resp.raise_for_status()

    def _patch(self, path: str, data: dict) -> None:
        resp = self.session.patch(
            f"{self.base_url}{path}",
            json=data,
            timeout=self.timeout,
        )
        resp.raise_for_status()

    # ── DI ──────────────────────────────────────────────

    def get_all_di(self) -> list[DIChannel]:
        data = self._get("/di_value/slot_0")
        return [
            DIChannel(
                ch=ch["Ch"],
                mode=ch["Md"],
                val=ch["Val"],
                stat=ch["Stat"],
                cnting=ch["Cnting"],
                ovlch=ch["OvLch"],
            )
            for ch in data["DIVal"]
        ]

    def get_di(self, ch: int) -> DIChannel:
        data = self._get(f"/di_value/slot_0/ch_{ch}")
        return DIChannel(
            ch=data["Ch"],
            mode=data["Md"],
            val=data["Val"],
            stat=data["Stat"],
            cnting=data["Cnting"],
            ovlch=data["OvLch"],
        )

    def is_di_high(self, ch: int) -> bool:
        return self.get_di(ch).stat == 1

    # ── DO ──────────────────────────────────────────────

    def get_all_do(self) -> list[DOChannel]:
        data = self._get("/do_value/slot_0")
        return [
            DOChannel(
                ch=ch["Ch"],
                mode=ch["Md"],
                val=ch["Val"],
                stat=ch["Stat"],
                ps_ctn=ch["PsCtn"],
                ps_stop=ch["PsStop"],
                ps_iv=ch["PsIV"],
            )
            for ch in data["DOVal"]
        ]

    def get_do(self, ch: int) -> DOChannel:
        data = self._get(f"/do_value/slot_0/ch_{ch}")
        return DOChannel(
            ch=data["Ch"],
            mode=data["Md"],
            val=data["Val"],
            stat=data["Stat"],
            ps_ctn=data["PsCtn"],
            ps_stop=data["PsStop"],
            ps_iv=data["PsIV"],
        )

    def set_do(self, ch: int, on: bool) -> None:
        val = 1 if on else 0
        self._patch(f"/do_value/slot_0/ch_{ch}", {"Ch": ch, "Val": val})

    def set_all_do(self, states: list[bool]) -> None:
        """states: list of 4 bools, index = channel number"""
        self._put(
            "/do_value/slot_0",
            {
                "DOVal": [
                    {"Ch": i, "Md": 0, "Val": 1 if on else 0,
                     "Stat": 1 if on else 0, "PsCtn": 0, "PsStop": 0, "PsIV": 0}
                    for i, on in enumerate(states)
                ]
            },
        )

    def turn_on(self, ch: int) -> None:
        self.set_do(ch, True)

    def turn_off(self, ch: int) -> None:
        self.set_do(ch, False)


if __name__ == "__main__":
    from elevator_io import ElevatorIO, Floor

    device   = WISE4060("10.0.0.1", username="root", password="kenmec123")
    elevator = ElevatorIO(device)

    def print_all_di():
        for di in device.get_all_di():
            print(f"  DI{di.ch}: {'HIGH' if di.stat else 'LOW'}")

    def print_all_do():
        for do in device.get_all_do():
            print(f"  DO{do.ch}: {'HIGH' if do.stat else 'LOW'}")

    def set_single_do():
        ch  = int(input("  Channel (0-3): "))
        val = input("  ON/OFF: ").strip().lower()
        device.set_do(ch, val == "on")

    def print_elevator_status():
        print(f"  專屬啟動: {'YES' if elevator.is_exclusive_active() else 'NO'}")
        print(f"  到達訊號: {'YES' if elevator.is_floor_arrived() else 'NO'}")

    menu = {
        "1":  ("讀取所有 DI",      print_all_di,                                           ""),
        "2":  ("讀取所有 DO",      print_all_do,                                           ""),
        "3":  ("控制單一 DO",      set_single_do,                                          ""),
        "4":  ("全部 DO OFF",      lambda: device.set_all_do([False, False, False, False]), "0000"),
        "──": None,
        "5":  ("電梯狀態",         print_elevator_status,                                  ""),
        "6":  ("申請專用模式",     elevator.request_exclusive,                             "0001"),
        "7":  ("前往 A 層",        lambda: elevator.go_to(Floor.A),                        "0010"),
        "8":  ("前往 B 層",        lambda: elevator.go_to(Floor.B),                        "0100"),
        "9":  ("前往 C 層",        lambda: elevator.go_to(Floor.C),                        "1000"),
        "10": ("保持開門",         elevator.hold_door,                                     "0011"),
        "11": ("關門 / 釋放",      elevator.release_door,                                  "0101"),
        "12": ("清除所有 DO",      elevator.clear,                                         "0000"),
        "q":  ("離開",             None,                                                   ""),
    }

    while True:
        print("\n--- WISE-4060 / 電梯控制 ---")
        for key, val in menu.items():
            if val is None:
                print("  ──────────────────")
            else:
                label, _, bits = val
                suffix = f"  [{bits}]" if bits else ""
                print(f"  [{key}] {label}{suffix}")

        choice = input("\n請選擇: ").strip().lower()

        if choice not in menu or menu[choice] is None:
            print("無效選項")
            continue

        label, action, _ = menu[choice]
        if action is None:
            break

        action()
        print(f"完成: {label}")
