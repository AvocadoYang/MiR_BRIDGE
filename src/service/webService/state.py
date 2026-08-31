from typing import cast

from fastapi import Request
from reactivex import Subject
from starlette.datastructures import State

from src.actions import ALL_Web_Action_Type
from src.types.amr import REGISTER_TABLE
from src.types.equipment import ELEVATOR_TABLE


class AppState(State):
    """Type-only shape of `request.state`; never instantiated (Starlette's real
    `State` object still backs it at runtime)."""

    register_table: REGISTER_TABLE
    elevator_table: ELEVATOR_TABLE
    output: Subject[ALL_Web_Action_Type]


class AppRequest(Request):
    @property
    def state(self) -> AppState:
        return cast(AppState, super().state)
