# -*- coding: utf-8 -*-
import time
import sqlalchemy
from sqlalchemy import MetaData, create_engine
from sqlalchemy import Table, select, and_



class DataBaseManager:
    """
    Оформлени в стиле ООП
    """

    def __init__(self):
        self.db_name = "trassir"
        self.db_login = "postgres"
        self.db_pass = "123456"
        self.metadata = MetaData()

        self.engine = self.connect_to_database()
        self.persisting_the_tables()

        self.pos_incidents = self.initialize_pos_incidents_table()
        self.pos_incident_types = self.initialize_pos_incident_types_table()
        self.pos_incidents_related_ts = self.initialize_pos_incidents_related_ts_table()




    def connect_to_database(self):
        engine = create_engine(
            'postgresql://{}:{}@localhost:5432/{}'.format(self.db_login,
                                                          self.db_pass,
                                                          self.db_name
                                                          ))
        try:
            return engine.connect()
        except Exception as err:
            raise ValueError(unicode(err[0], 'cp1251'))

    def persisting_the_tables(self):
        """
        Создаёт таблицу, если её нет
        """
        self.metadata.create_all(self.engine)


    def initialize_pos_incidents_table(self):
        return Table('pos_incidents', self.metadata, autoload=True, autoload_with=self.engine)

    def initialize_pos_incident_types_table(self):
        return Table('pos_incident_types', self.metadata, autoload=True, autoload_with=self.engine)

    def initialize_pos_incidents_related_ts_table(self):
        return Table('pos_incidents_related_ts', self.metadata, autoload=True, autoload_with=self.engine)


    def make_query_where_1(self):
        """
            select * from pos_incidents_related_ts where incident_id in
            (select incident_id from pos_incidents where incident_type_id like 'id_imitating_barcode_scanning')
            and (incident_related_ts >= 1558527653677870 and incident_related_ts <= 1558527756095432)
        """

        # s = select([self.cookies]).where(self.cookies.c.cookie_name == 'peanut butter')
        # s = select([pos_incidents_related_ts],pos_incidents_related_ts.c.incident_id.in_ )
        s = select([self.pos_incidents_related_ts]).where(
            self.pos_incidents_related_ts.c.incident_id.in_(
                select([self.pos_incidents.c.incident_id]).where(
                    self.pos_incidents.c.incident_type_id == 'id_imitating_barcode_scanning')))
        s = s.order_by(self.pos_incidents_related_ts.c.incident_related_ts)

        rp = self.engine.execute(s)
        record = rp.fetchall()
        print(len(record))

    def make_query_where_2(self):
        s = select([self.pos_incidents_related_ts]).where(self.pos_incidents_related_ts.c.incident_id.in_(
                select([self.pos_incidents.c.incident_id]).where(
                    self.pos_incidents.c.incident_type_id.like('%id_imitating_barcode_scanning%'))))

        rp = self.engine.execute(s)
        record = rp.fetchall()
        print(len(record))



    def make_query_where_3(self):
        s = select([self.pos_incidents_related_ts]).where(
            self.pos_incidents_related_ts.c.incident_id.in_(
                select([self.pos_incidents.c.incident_id]).where(and_(
                    self.pos_incidents.c.incident_type_id == 'id_imitating_barcode_scanning',
                    self.pos_incidents.c.incident_related_ts

                ))))

db_manager = DataBaseManager()
db_manager.make_query_where_1()
#db_manager.make_query_where_2()