# -*- coding: utf-8 -*-

import logging

import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base


def get_logger(filename):
    logger = logging.getLogger("MYSCRIPT")
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    logger.setLevel("DEBUG")


    file_handler = logging.FileHandler(r'E:\_job\_logs\try_sql.log')
    file_handler.setLevel("INFO")

    # https://docs.python.org/2/library/logging.html#logrecord-attributes

    file_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(lineno)-4s <%(funcName)s> - %(name)s: %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    logger.addHandler(file_handler)

    logger.info("info message")
    logger.warning("info message")
    logger.critical("critical message")

# Base = declarative_base()
# engine = db.create_engine('postgresql://postgres:123456@localhost:5432/postgres', echo=True)

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String, BigInteger, Date, Unicode

from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.sql import and_
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base

# engine = get_database_connection()
engine = db.create_engine('postgresql://postgres:123456@localhost:5432/postgres', echo=True, client_encoding='utf8')
Session = sessionmaker()
Base = declarative_base()

Session.configure(bind=engine)
session = Session()


class PosIncidentTypes(Base):
    __tablename__ = "pos_incident_types"
    #  __tablename__ = "persons_images_t"

    incident_type_id = Column(String, primary_key=True)  # char length 128
    incident_type_name = Column(Unicode)  # text
    incident_type_description = Column(String)  # text
    incident_type_created_by_user = Column(String)  # bool
    incident_type_created_by_detector = Column()  # bool
    #incident_type_deleted = Column()  # bool

    # guid = Column(String, primary_key=True)
    # name = Column(String)
    # birth_date = Column(Date)
    # gender = Column(Integer)
    # contact_info = Column(String)
    # comment = Column(String)
    # folder_guid = Column(String)
    # image_guid = Column(String)
    # image_change_ts = Column(BigInteger)
    # remote_server_guid = Column(String)
    # modification_id = Column(BigInteger)
    # deleted_ts = Column(BigInteger)

    def __repr__(self):
        # return "<PosIncidentTypes(incident_type_id=%s," \
        #        "incident_type_name=%s," \
        #        "incident_type_description=%s," \
        #        "incident_type_created_by_user=s" \
        #        "incident_type_created_by_detector=s" \
        #        "incident_type_deleted=6>" % (
        #     self.incident_type_id,
        #     self.incident_type_name,
        #     type(self.incident_type_description),
        #     #self.incident_type_created_by_user,
        #     #self.incident_type_created_by_detector
        #  )
        print(self.incident_type_name.encode('utf-8'))
        return self.incident_type_name.encode('utf-8')

    def __str__(self):
        return self.__repr__()


def show_data():
    return session.query(PosIncidentTypes).all()


print(show_data())
