from typing import List, Union

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel, RootModel
from pydantic import ValidationError as PydanticValidationError

from src.logger import logger
from src.types.amr import REGISTER_TABLE
from src.types.map import PERIPHERAL_TYPE_MAP, Footprint, PeripheralType
from src.types.web import ALL_Groups, Maps

from ...handler import CustomSuccessRoute, ExternalServiceError, ValidationError
from ...httpx_set import headers

router = APIRouter(prefix='/map', route_class=CustomSuccessRoute)


class NewPosition(BaseModel):
    guid: str
    name: str
    pos_x: float
    pos_y: float
    orientation: float
    type_id: int
    map_id: str
    created_by_id: Union[str, None]


class Location(BaseModel):
    id: str
    locationId: str
    x: float
    y: float
    offset_x: float
    offset_y: float
    canRotate: bool
    rotate: float
    areaType: PeripheralType
    cost: int
    connectedRoadIds: List[str]
    footprint: Footprint
    neighborIds: List[str]

    map_id: str


class LocationIDs(BaseModel):
    locationIds: List[str]


class MapDetail(BaseModel):
    guid: str
    session_id: str
    name: str
    base_map: str
    resolution: float
    origin_x: float
    origin_y: float
    origin_theta: float
    positions: str
    paths: str
    path_guides: str
    created_by_id: str
    created_by: str


class SessionDetail(BaseModel):
    guid: str
    name: str
    description: str
    maps: str
    export: str
    created_by_id: str
    created_by: str
    active: bool
    allowed_methods: List[str]
    created_by_name: str


@router.post('/add_position')
async def add_position(request: Request, new_position: Location):
    try:
        register_table: dict[str, REGISTER_TABLE] = request.state.register_table
        async with httpx.AsyncClient() as client:
            for mac_address, info in register_table.items():
                amr = info['amr']
                if amr is None:
                    logger.warning(f'AMR info is missing for MAC: {mac_address}')
                    continue
                url = f'http://{info["ip"]}/api/v2.0.0/positions'
                send_position = NewPosition(
                    guid=new_position.id,
                    name=new_position.locationId,
                    pos_x=new_position.x,
                    pos_y=new_position.y,
                    orientation=new_position.rotate,
                    type_id=PERIPHERAL_TYPE_MAP.get(new_position.areaType, 0),
                    map_id=new_position.map_id,
                    created_by_id=amr.user_uuid,
                )
                await client.post(
                    url=url, headers=headers, json=send_position.model_dump(), timeout=3
                )
        logger.bind(state='[POST]').info(f'create new position: {new_position.model_dump_json()}')
        return new_position
    except (httpx.HTTPStatusError, Exception) as e:
        print(e)


@router.delete('/delete_position')
async def delete_position(request: Request, payload: LocationIDs):
    try:
        register_table: dict[str, REGISTER_TABLE] = request.state.register_table
        async with httpx.AsyncClient() as client:
            for locationId in payload.locationIds:
                for mac_address, info in register_table.items():
                    amr = info['amr']
                    if amr is None:
                        logger.warning(f'AMR info is missing for MAC: {mac_address}')
                        continue
                    url = f'http://{info["ip"]}/api/v2.0.0/positions/{locationId}'
                    await client.delete(url=url, headers=headers, timeout=3)
            logger.bind(state='[POST]').info(f'delete positions: {payload.locationIds}')
            return payload.locationIds

    except (httpx.HTTPStatusError, Exception) as e:
        print(e)
    return


@router.put('/update_position')
async def update_position():
    pass


class MapUsingFormat(BaseModel):
    map_id: str


@router.put('/switch-map')
async def change_map_use(request: Request, payload: MapUsingFormat):
    try:
        register_table: dict[str, REGISTER_TABLE] = request.state.register_table
        for mac_address, info in register_table.items():
            amr = info['amr']
            if amr is not None:
                if not amr.amr_info.connect_w_amr:
                    continue
            url = f'http://{info["ip"]}/api/v2.0.0/status'
            async with httpx.AsyncClient() as client:
                res = await client.put(url=url, headers=headers, json=payload.model_dump())
        logger.bind(state='[PUT]').info(f'switch map to {payload.map_id}')
        return payload.map_id
    except (httpx.HTTPStatusError, Exception) as e:
        print(e)


@router.get('/all_groups', response_model=List[ALL_Groups])
async def get_all_groups(request: Request):
    res: List[ALL_Groups] = []
    register_table: dict[str, REGISTER_TABLE] = request.state.register_table

    class Session(BaseModel):
        guid: str
        name: str

    class SessionsSchema(RootModel[List[Session]]):
        pass

    seen_ids: set[str] = set()

    for item in list(register_table.values()):
        try:
            url = f'http://{item["ip"]}/api/v2.0.0/sessions'
            async with httpx.AsyncClient() as client:
                response = await client.get(url=url, headers=headers, timeout=2)
                sessions = SessionsSchema.model_validate(response.json())
                for session in sessions.root:
                    if session.guid in seen_ids:
                        continue
                    seen_ids.add(session.guid)
                    res.append(ALL_Groups(id=session.guid, name=session.name))
                logger.bind(state='[GET]').info(f'{item["amrId"]} return all map groups info')
        except Exception:
            pass

    return res


@router.get('/sync_map', response_model=List[Maps])
async def async_map(request: Request):
    res: List[Maps] = []
    register_table: dict[str, REGISTER_TABLE] = request.state.register_table

    class Info(BaseModel):
        url: str
        guid: str
        name: str

    class Map_Info(BaseModel):
        info: List[Info]

    for item in list(register_table.values()):
        if item['amr'] is None:
            continue
        if not item['amr'].connect_status['mir_service_is_connect']:
            continue
        try:
            url = f'http://{item["ip"]}/api/v2.0.0/maps'
            async with httpx.AsyncClient() as client:
                response = await client.get(url=url, headers=headers, timeout=2)
                maps = response.json()
                valid_maps = Map_Info(info=maps)
                print_map_info = {}
                if len(valid_maps.info):
                    for map in valid_maps.info:
                        get_map_info_url = f'http://{item["ip"]}/api/v2.0.0/maps/{map.guid}'
                        info_res = await client.get(url=get_map_info_url, headers=headers)
                        map_detail = info_res.json()
                        print_map_info[map_detail['name']] = {
                            'origin_x': map_detail['origin_x'],
                            'origin_y': map_detail['origin_y'],
                        }
                        valid_map_detail = MapDetail(**map_detail)
                        get_session_info_url = (
                            f'http://{item["ip"]}/api/v2.0.0/sessions/{valid_map_detail.session_id}'
                        )
                        session_res = await client.get(get_session_info_url, headers=headers)
                        session_info = session_res.json()
                        valid_session_info = SessionDetail(**session_info)
                        r: Maps = Maps(
                            guid=valid_map_detail.guid,  # map id
                            session_id=valid_map_detail.session_id,  # site id or group id
                            group_name=valid_session_info.name,
                            name=valid_map_detail.name,
                            base_map=valid_map_detail.base_map,
                            resolution=valid_map_detail.resolution,
                            origin_x=valid_map_detail.origin_x,
                            origin_y=valid_map_detail.origin_y,
                            origin_theta=valid_map_detail.origin_theta,
                        )
                        res.append(r)
            logger.bind(state='[GET]').info(f'{item["amrId"]} return sync maps info')
        except PydanticValidationError as e:
            raise ValidationError(
                message=f'msg: {e.errors()[0]["msg"]}, input: {e.errors()[0]["input"]}'
            )
        except (httpx.HTTPStatusError, Exception):
            raise ExternalServiceError(service=item['ip'])
    return res


"""
map upload api
[POST] /maps
payload:
{
  "guid": "string",
  "session_id": "string",
  "name": "string",
  "base_map": "string",
  "resolution": 0,
  "origin_x": 0,
  "origin_y": 0,
  "origin_theta": 0,
  "created_by_id": "string"
}
"""
