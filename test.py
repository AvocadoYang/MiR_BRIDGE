import asyncio

from src.configs import config
from src.service.equipment import Elevator_Machine, Floor

ip, username, password = config.ELEVATORS[0]
elevator = Elevator_Machine(ip=ip, username=username, password=password)

menu = {
    '1': ('前往 A 層', lambda: elevator.go_to(Floor.A)),
    '2': ('前往 B 層', lambda: elevator.go_to(Floor.B)),
    '3': ('前往 C 層', lambda: elevator.go_to(Floor.C)),
    '4': ('保持開門', elevator.hold_door),
    '5': ('關門 / 釋放', elevator.release_door),
    'q': ('離開', None),
}


async def main():
    try:
        while True:
            print(f'\n--- 電梯狀態機 [目前狀態: {elevator.current_state_value}] ---')
            for key, (label, _) in menu.items():
                print(f'  [{key}] {label}')

            choice = input('\n請選擇: ').strip().lower()
            if choice not in menu:
                print('無效選項')
                continue

            label, action = menu[choice]
            if action is None:
                break

            result = await action()
            print(f'完成: {label} -> {result}')
    finally:
        await elevator.close()


if __name__ == '__main__':
    asyncio.run(main())
