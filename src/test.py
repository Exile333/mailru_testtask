import asynctnt
import asyncio

async def a():
    conn = asynctnt.Connection(host='127.0.0.1', port=3301)
    await conn.connect()

    print(await conn.delete('kv', ['1']))

asyncio.run(a())
