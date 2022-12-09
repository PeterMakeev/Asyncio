import asyncio
import datetime
from aiohttp import ClientSession
from more_itertools import chunked
from db import *


CHUNK_SIZE = 10


async def get_person(people_id: int, session: ClientSession):
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json(content_type=None)
        json_data['id'] = people_id
        return json_data


async def get_characters():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 101), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            yield await asyncio.gather(*coroutines)


async def get_name(url):
    async with ClientSession() as session:
        async with session.get(url) as response:
            response = await response.json(content_type=None)
            if 'title' in response:
                return response['title']
            if 'name' in response:
                return response['name']


async def create_character_dict(characters):
    character_dict = {}
    for person in characters:
        if 'detail' in person:
            characters.remove(person)
        else:
            person.pop('created', None)
            person.pop('edited', None)
            person.pop('url', None)
            person['homeworld'] = await get_name(person['homeworld'])
            person['films'] = ', '.join([await get_name(i) for i in person['films']])
            person['species'] = ', '.join([await get_name(i) for i in person['species']])
            person['starships'] = ', '.join([await get_name(i) for i in person['starships']])
            person['vehicles'] = ', '.join([await get_name(i) for i in person['vehicles']])
            character_dict[f'{person["id"]}'] = person
    return character_dict


async def insert_character(people_dict):
    async with Session() as session:
        for value in people_dict.values():
            coroutines = await save_character_in_db(value)
            #print(results)
        await session.commit()


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    tasks = []
    async for character in get_characters():
        task_1 = asyncio.create_task(create_character_dict(character))
        tasks.append(task_1)
        task_2 = asyncio.create_task(insert_character(await task_1))
        tasks.append(task_2)
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    start = datetime.datetime.now()
    print('Процесс идёт...')
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    print('Процесс завершился за', datetime.datetime.now() - start)
