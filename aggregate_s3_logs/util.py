from asyncio import get_running_loop


async def run_in_thread(f, *args):
    loop = get_running_loop()
    return await loop.run_in_executor(None, f, *args)
