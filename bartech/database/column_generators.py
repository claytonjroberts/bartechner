from sqlalchemy import Column, Integer, String


def name():
    return Column("name", String)


def index():
    return Column("index", Integer)
