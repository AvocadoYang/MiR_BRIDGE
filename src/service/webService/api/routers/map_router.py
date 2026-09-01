from typing import List, Union

import httpx
from fastapi import APIRouter, Path, Query
from pydantic import BaseModel, RootModel
from pydantic import ValidationError as PydanticValidationError

from src.logger import logger
from src.types.amr import AMR_REGISTER_INFO, REGISTER_TABLE
from src.types.map import PERIPHERAL_TYPE_MAP, Footprint, PeripheralType
from src.types.web import ALL_Groups, Maps

from ...handler import (
    CustomSuccessRoute,
    ExternalServiceError,
    NotFoundError,
    ValidationError,
)
from ...httpx_set import headers
from ...state import AppRequest

router = APIRouter(prefix="/map", tags=['map'], route_class=CustomSuccessRoute)


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


class MapListItem(BaseModel):
    guid: str
    url: str
    name: str


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


@router.post("/add_position")
async def add_position(request: AppRequest, new_position: Location):
    try:
        register_table = request.state.register_table
        async with httpx.AsyncClient() as client:
            for mac_address, info in register_table.items():
                amr = info["amr"]
                if amr is None:
                    logger.warning(f"AMR info is missing for MAC: {mac_address}")
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
        logger.bind(state="[POST]").info(
            f"create new position: {new_position.model_dump_json()}"
        )
        return new_position
    except (httpx.HTTPStatusError, Exception) as e:
        print(e)


@router.delete("/delete_position")
async def delete_position(request: AppRequest, payload: LocationIDs):
    try:
        register_table = request.state.register_table
        async with httpx.AsyncClient() as client:
            for locationId in payload.locationIds:
                for mac_address, info in register_table.items():
                    amr = info["amr"]
                    if amr is None:
                        logger.warning(f"AMR info is missing for MAC: {mac_address}")
                        continue
                    url = f'http://{info["ip"]}/api/v2.0.0/positions/{locationId}'
                    await client.delete(url=url, headers=headers, timeout=3)
            logger.bind(state="[POST]").info(f"delete positions: {payload.locationIds}")
            return payload.locationIds

    except (httpx.HTTPStatusError, Exception) as e:
        print(e)
    return


@router.put("/update_position")
async def update_position():
    pass


class MapUsingFormat(BaseModel):
    mac_address: str
    map_id: str


@router.put("/switch-map")
async def change_map_use(request: AppRequest, payload: MapUsingFormat):
    register_table = request.state.register_table

    info = register_table.get(payload.mac_address)
    if info is None:
        raise ExternalServiceError(
            service=payload.mac_address,
            message=f"target AMR {payload.mac_address} is not registered",
        )

    amr = info["amr"]
    if amr is not None and not amr.amr_info.connect_w_amr:
        raise ExternalServiceError(
            service=payload.mac_address,
            message=f"target AMR {payload.mac_address} is offline",
        )

    url = f'http://{info["ip"]}/api/v2.0.0/status'
    try:
        async with httpx.AsyncClient() as client:
            res = await client.put(
                url=url, headers=headers, json={"map_id": payload.map_id}, timeout=3
            )
            result = res.json()
    except (httpx.HTTPStatusError, Exception) as e:
        raise ExternalServiceError(
            service=payload.mac_address,
            message=f"failed to switch map on {payload.mac_address}: {e}",
        )

    if "error_code" in result:
        raise ExternalServiceError(
            service=payload.mac_address,
            message=f"AMR rejected map switch to {payload.map_id}: {result}",
        )

    logger.bind(state="[PUT]").info(f'{info["amrId"]} switch map to {payload.map_id}')
    return payload.map_id


@router.get("/all_groups", response_model=List[ALL_Groups])
async def get_all_groups(request: AppRequest):
    res: List[ALL_Groups] = []
    register_table = request.state.register_table

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
                logger.bind(state="[GET]").info(
                    f'{item["amrId"]} return all map groups info'
                )
        except Exception:
            pass

    return res


def _resolve_target_MiRs(
    register_table: REGISTER_TABLE, serialNum: Union[str, None]
) -> List[AMR_REGISTER_INFO]:
    """Pick which connected AMRs a read endpoint should query.

    Without serialNum: every connected AMR (fleet-wide aggregate — the existing
    behaviour). With serialNum: exactly that one AMR, so QAMS can read back a
    single vehicle for per-vehicle reported refresh / strong verification; an
    unknown or offline serialNum raises so QAMS fails loud instead of silently
    reading an empty fleet.
    """
    if serialNum is None:
        return [
            item
            for item in register_table.values()
            if item["amr"] is not None
            and item["amr"].connect_status["mir_service_is_connect"]
        ]

    for item in register_table.values():
        if item["serialNum"] == serialNum:
            amr = item["amr"]
            if amr is None or not amr.connect_status["mir_service_is_connect"]:
                raise ExternalServiceError(
                    service=serialNum,
                    message=f"target AMR {serialNum} is offline",
                )
            return [item]

    raise ExternalServiceError(
        service=serialNum,
        message=f"target AMR {serialNum} is not registered",
    )


@router.get("/sync_map", response_model=List[MapListItem])
async def sync_map_list(request: AppRequest, serialNum: Union[str, None] = None):
    res: List[MapListItem] = []
    register_table = request.state.register_table
    targets = _resolve_target_MiRs(register_table, serialNum)

    class MapListSchema(RootModel[List[MapListItem]]):
        pass

    seen_ids: set[str] = set()

    for item in targets:
        try:
            url = f'http://{item["ip"]}/api/v2.0.0/maps'
            async with httpx.AsyncClient() as client:
                response = await client.get(url=url, headers=headers, timeout=2)
                maps = MapListSchema.model_validate(response.json())
                for map_item in maps.root:
                    if map_item.guid in seen_ids:
                        continue
                    seen_ids.add(map_item.guid)
                    res.append(map_item)
            logger.bind(state="[GET]").info(f'{item["amrId"]} return map list')
        except (httpx.HTTPStatusError, Exception) as e:
            # a specific vehicle was requested: fail loud so QAMS does not read
            # an error as "this vehicle holds no maps"
            if serialNum is not None:
                raise ExternalServiceError(
                    service=serialNum,
                    message=f"failed to read maps from {serialNum}: {e}",
                )
            # fleet aggregate: skip this AMR and keep going
            continue

    return res


@router.get(
    "/sync_map/{guid}",
    response_model=Maps,
    summary='Sync a single map by GUID',
    description=(
        'Fetch map details for the given map GUID. If `serialNum` is omitted, '
        'every registered AMR is tried until one of them holds the map.'
    ),
)
async def sync_map(
    request: AppRequest,
    guid: str = Path(..., description='GUID of the map to sync, as reported by MiR.'),
    serialNum: Union[str, None] = Query(
        default=None,
        description='Serial number of a specific AMR to query. If omitted, all registered AMRs are tried.',
    ),
):
    register_table = request.state.register_table
    targets = _resolve_target_MiRs(register_table, serialNum)

    for item in targets:
        try:
            async with httpx.AsyncClient() as client:
                get_map_info_url = f'http://{item["ip"]}/api/v2.0.0/maps/{guid}'
                info_res = await client.get(
                    url=get_map_info_url, headers=headers, timeout=2
                )
                map_detail = info_res.json()
                if "error_code" in map_detail:
                    # this AMR does not hold the map, try the next one
                    continue
                valid_map_detail = MapDetail(**map_detail)
                get_session_info_url = f'http://{item["ip"]}/api/v2.0.0/sessions/{valid_map_detail.session_id}'
                session_res = await client.get(
                    url=get_session_info_url, headers=headers, timeout=2
                )
                valid_session_info = SessionDetail(**session_res.json())
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
                logger.bind(state="[GET]").info(
                    f'{item["amrId"]} return sync map {guid}'
                )
                return r
        except PydanticValidationError as e:
            raise ValidationError(
                message=f'msg: {e.errors()[0]["msg"]}, input: {e.errors()[0]["input"]}'
            )
        except (httpx.HTTPStatusError, Exception) as e:
            # a specific vehicle was requested: fail loud so QAMS does not read
            # an error as "this vehicle does not hold the map"
            if serialNum is not None:
                raise ExternalServiceError(
                    service=serialNum,
                    message=f"failed to read map {guid} from {serialNum}: {e}",
                )
            # fleet aggregate: fall through to the next AMR
            continue

    if serialNum is not None:
        raise NotFoundError(message=f"map {guid} not found on AMR {serialNum}")
    raise NotFoundError(message=f"map {guid} not found on any connected AMR")


class MapPush(BaseModel):
    serialNum: str
    guid: str
    session_id: str
    name: str
    base_map: str
    resolution: float
    origin_x: float
    origin_y: float
    origin_theta: float = 0
    group_name: Union[str, None] = None


class MapUpload(BaseModel):
    guid: str
    session_id: str
    name: str
    base_map: str
    resolution: float
    origin_x: float
    origin_y: float
    origin_theta: float
    created_by_id: str


@router.post("/push")
async def push_map(request: AppRequest, payload: MapPush):
    register_table = request.state.register_table

    target: Union[AMR_REGISTER_INFO, None] = None
    for item in register_table.values():
        if item["serialNum"] == payload.serialNum:
            target = item
            break

    if target is None:
        raise ExternalServiceError(
            service=payload.serialNum,
            message=f"target AMR {payload.serialNum} is not registered",
        )

    amr = target["amr"]
    if amr is None or not amr.connect_status["mir_service_is_connect"]:
        raise ExternalServiceError(
            service=payload.serialNum,
            message=f"target AMR {payload.serialNum} is offline",
        )

    ip = target["ip"]
    base = f"http://{ip}/api/v2.0.0"

    try:
        async with httpx.AsyncClient() as client:
            session_res = await client.get(
                url=f"{base}/sessions/{payload.session_id}", headers=headers, timeout=3
            )
            if "error_code" in session_res.json():
                if not payload.group_name:
                    raise ExternalServiceError(
                        service=payload.serialNum,
                        message=(
                            f"session {payload.session_id} does not exist on AMR "
                            f"{payload.serialNum} and group_name is missing, "
                            "refusing to create an unnamed site"
                        ),
                    )
                await client.post(
                    url=f"{base}/sessions",
                    headers=headers,
                    json={
                        "guid": payload.session_id,
                        "name": payload.group_name,
                    },
                    timeout=3,
                )

            map_res = await client.get(
                url=f"{base}/maps/{payload.guid}", headers=headers, timeout=3
            )
            exists = "error_code" not in map_res.json()

            upload = MapUpload(
                guid=payload.guid,
                session_id=payload.session_id,
                name=payload.name,
                base_map=payload.base_map,
                resolution=payload.resolution,
                origin_x=payload.origin_x,
                origin_y=payload.origin_y,
                origin_theta=payload.origin_theta,
                created_by_id=amr.user_uuid,
            )

            if exists:
                write_res = await client.put(
                    url=f"{base}/maps/{payload.guid}",
                    headers=headers,
                    json=upload.model_dump(exclude={"guid"}),
                    timeout=5,
                )
                result = "updated"
            else:
                write_res = await client.post(
                    url=f"{base}/maps",
                    headers=headers,
                    json=upload.model_dump(),
                    timeout=5,
                )
                result = "created"

            if "error_code" in write_res.json():
                raise ExternalServiceError(
                    service=payload.serialNum,
                    message=f"AMR rejected map {payload.guid}: {write_res.json()}",
                )
    except ExternalServiceError:
        raise
    except (httpx.HTTPStatusError, Exception) as e:
        raise ExternalServiceError(
            service=payload.serialNum,
            message=f"failed to push map {payload.guid} to {payload.serialNum}: {e}",
        )

    logger.bind(state="[POST]").info(f'{target["amrId"]} {result} map {payload.guid}')
    return {"target": payload.serialNum, "guid": payload.guid, "result": result}
