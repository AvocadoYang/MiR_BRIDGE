from .connect_impl import Connect_impl


class Rabbit_client_async(Connect_impl):
    def __init__(self):
        super().__init__()
