import asyncio
from uop.mongo import mongouop
from uop import db_interface as dbi

db = mongouop.MongoUOP('pkm_app')


async def get_metadata():
    print('running')
    di = await dbi.get_user_interface(db, user_id='333')
    metadata = await di.metadata()
    print(metadata)


loop = asyncio.get_event_loop()
loop.run_until_complete(get_metadata())
