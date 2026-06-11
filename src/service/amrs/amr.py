class AMR:
    def __init__(self, mac_address, ip, amrId, api_token):
        self.ip: str = api_token
        self.mac_address: str = mac_address
        self.ip: str = ip
        self.amrId: str = amrId

    async def get_MiR_info(self, data):
        from src.dtypes import AMR_INFO_DETAIL

        amr_info: AMR_INFO_DETAIL = data
        print(amr_info)
