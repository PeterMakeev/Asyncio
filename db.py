from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


PG_DSN = 'postgresql+asyncpg://user:1234@127.0.0.1:5431/db'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Characters(Base):
    __tablename__ = 'characters'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), index=True)
    birth_year = Column(String(32), index=True)
    eye_color = Column(String(32), index=True)
    films = Column(String, index=True)
    gender = Column(String(16), index=True)
    hair_color = Column(String(32), index=True)
    height = Column(String(32), index=True)
    homeworld = Column(String(128), index=True)
    mass = Column(String(8), index=True)
    skin_color = Column(String(32), index=True)
    species = Column(String, index=True)
    starships = Column(String, index=True)
    vehicles = Column(String, index=True)


async def save_character_in_db(character: dict):
    async with Session() as session:
        async with session.begin():
            id = character['id']
            birth_year = character['birth_year']
            eye_color = character['eye_color']
            films = character['films']
            gender = character['gender']
            hair_color = character['hair_color']
            height = character['height']
            homeworld = character['homeworld']
            mass = character['mass']
            name = character['name']
            skin_color = character['skin_color']
            species = character['species']
            starships = character['starships']
            vehicles = character['vehicles']
            new_character = Characters(id=id, name=name, birth_year=birth_year, eye_color=eye_color, skin_color=skin_color,
                              height=height, gender=gender, mass=mass, hair_color=hair_color,  vehicles=vehicles,
                              homeworld=homeworld, films=films, starships=starships, species=species)
            session.add(new_character)



