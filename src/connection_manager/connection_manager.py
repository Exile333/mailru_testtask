import asyncio
import asynctnt

class ConnectionManager(object):

    def __init__(self):
        self._connections_pool = None
        self._gc_rask = None
        self._killed = False

    async def init(self):
        self._connections_pool = asyncio.Queue()
        self._gc_rask = asyncio.create_task(self._gc())

    async def close_all(self):
        self._gc_rask.cancel()
        self._killed = True
        await self._kill_all_connections()

    async def get_connection(self, host='127.0.0.1', port=3301):
        if self._killed:
            return None

        if self._connections_pool.empty():
            new_conn = asynctnt.Connection(host=host, port=port)
            await new_conn.connect()
            return new_conn
        else:
            return await self._connections_pool.get()
            
    def close_connection(self, connection):
        if self._killed:
            return None

        self._connections_pool.put_nowait(connection)

    def conn_cnt(self):
        if self._killed:
            return 0

        return self._connections_pool.qsize()

    async def _kill_all_connections(self):
        self._killed = True
        self._gc_rask = None

        while not self._connections_pool.empty():
            try:
                conn = self._connections_pool.get_nowait()
                await conn.disconnect()
            except QueueEmpty:
                break

    async def _gc(self):
        while True:
            await asyncio.sleep(5)
            n = self.conn_cnt()
            n += n % 2
            for _ in range(n):
                if self.conn_cnt() == 0:
                    break
                conn = await self._connections_pool.get()
                await conn.disconnect()
