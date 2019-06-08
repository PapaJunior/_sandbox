# -*- coding: utf-8 -*-
import logging
import os

from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, Numeric, String, DateTime, ForeignKey, create_engine
from sqlalchemy import insert, select
from datetime import datetime


def get_logger(log_level='WARNING', filename=None):
    logger_ = logging.getLogger("MYSCRIPT")

    if logger_.handlers:
        for handler in logger_.handlers[:]:
            handler.close()
            logger_.removeHandler(handler)

    logger_.setLevel(log_level)

    if filename is None:
        filename = os.path.basename(__file__).split(".")[0]

    log_path = os.path.join(r'E:\_job\_logs', filename)
    file_handler = logging.FileHandler(log_path)

    file_handler.setLevel("DEBUG")

    file_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(lineno)-4s <%(funcName)s> - %(name)s: %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    file_handler.setFormatter(file_formatter)
    logger_.addHandler(file_handler)
    return logger_


logger = get_logger(log_level='DEBUG')


class DataBaseManager:
    """
    Оформлени в стиле ООП
    """

    def __init__(self):
        self.db_name = "my_db_1"
        self.db_login = "postgres"
        self.db_pass = "123456"
        self.metadata = MetaData()

        self.cookies = self.initialize_cookies_table()
        self.users = self.initialize_users_table()
        self.orders = self.initialize_orders_table()
        self.line_items = self.initialize_line_items_table()

        self.connection = self.connect_to_database()
        self.persisting_the_tables()

    def connect_to_database(self):
        self.engine = create_engine(
            'postgresql://{}:{}@localhost:5432/{}'.format(self.db_login,
                                                          self.db_pass,
                                                          self.db_name
                                                          ))
        try:
            return self.engine.connect()
        except Exception as err:
            raise ValueError(unicode(err[0], 'cp1251'))

    def persisting_the_tables(self):
        """
        Создаёт таблицу, если её нет
        """
        self.metadata.create_all(self.engine)

    def initialize_cookies_table(self):
        _cookies = Table('cookies', self.metadata,
                         Column('cookie_id', Integer(), primary_key=True),
                         Column('cookie_name', String(50), index=True),
                         Column('cookie_recipe_url', String(255)),
                         Column('cookie_sku', String(55)),
                         Column('quantity', Integer()),
                         Column('unit_cost', Numeric(12, 2))
                         )
        return _cookies

    def initialize_users_table(self):
        _users = Table('users', self.metadata,
                       Column('user_id', Integer(), primary_key=True),
                       Column('customer_number', Integer(), autoincrement=True),
                       Column('username', String(15), nullable=False, unique=True),
                       Column('email_address', String(255), nullable=False),
                       Column('phone', String(20), nullable=False),
                       Column('password', String(25), nullable=False),
                       Column('created_on', DateTime(), default=datetime.now),
                       Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
                       )
        return _users

    def initialize_orders_table(self):
        _orders = Table('orders', self.metadata,
                        Column('order_id', Integer(), primary_key=True),
                        Column('user_id', ForeignKey('users.user_id'))
                        )
        return _orders

    def initialize_line_items_table(self):
        _line_items = Table('line_items', self.metadata,
                            Column('line_items_id', Integer(), primary_key=True),
                            Column('order_id', ForeignKey('orders.order_id')),
                            Column('cookie_id', ForeignKey('cookies.cookie_id')),
                            Column('quantity', Integer()),
                            Column('extended_cost', Numeric(12, 2))
                            )
        return _line_items

    def insert_data(self, channel_name, zone_name, people_quantity, event_ts):
        ins = self.people_quantity.insert().values(
            channel_name=channel_name,
            zone_name=zone_name,
            people_quantity=people_quantity,
            event_ts=event_ts
        )
        # ins.compile().params - to
        result = self.connection.execute(ins)
        logger.info(result)

    def add_data(self, data):
        """
        Format data before insert to database
        """
        if not isinstance(data, dict):
            return
        event_ts = data.get('event_ts')
        channel_name = data.get('channel_name')
        people_quantity = data.get('people_quantity')
        zone_name = data.get('zone_name')
        if not (event_ts and channel_name and people_quantity and zone_name):
            return
        logger.debug('Start insert data in DB')
        self.insert_data(channel_name, zone_name, people_quantity, event_ts)

    def try_insert(self):
        # Var.1
        # ins = self.cookies.insert().values(
        #     cookie_name="chocolate chip 3",
        #     cookie_recipe_url="http://some.aweso.me/cookie/recipe3.html",
        #     cookie_sku="CC03",
        #     quantity="11",
        #     unit_cost="0.20"
        # )

        # Var.2
        # Notice the table is now the argument to the insert function

        # ins = insert(self.cookies).values(
        #     cookie_name="chocolate chip 4",
        #     cookie_recipe_url="http://some.aweso.me/cookie/recipe4.html",
        #     cookie_sku="CC04",
        #     quantity="142",
        #     unit_cost="0.10"
        # )
        # result = self.connection.execute(ins)

        # Var.3 Multiple inserts
        # ins = self.cookies.insert()
        # inventory_list = [
        #     {
        #         'cookie_name': 'peanut butter',
        #         'cookie_recipe_url': 'http://some.aweso.me/cookie/peanut.html',
        #         'cookie_sku': 'PB01',
        #         'quantity': '24',
        #         'unit_cost': '0.25'
        #     },
        #     {
        #         'cookie_name': 'oatmeal raisin',
        #         'cookie_recipe_url': 'http://some.okay.me/cookie/raisin.html',
        #         'cookie_sku': 'EWW01',
        #         'quantity': '100',
        #         'unit_cost': '1.00'
        #     }
        # ]
        # result = self.connection.execute(ins, inventory_list)

        # Var.4
        inventory_list = [
            {
                'cookie_name': 'peanut butter',
                'cookie_recipe_url': 'http://some.aweso.me/cookie/peanut.html',
                'cookie_sku': 'PB01',
                'quantity': '24',
                'unit_cost': '0.25'
            },
            {
                'cookie_name': 'oatmeal raisin',
                'cookie_recipe_url': 'http://some.okay.me/cookie/raisin.html',
                'cookie_sku': 'EWW01',
                'quantity': '100',
                'unit_cost': '1.00'
            }
        ]
        ins = insert(self.cookies)
        result = self.connection.execute(ins, inventory_list)

        print(result)

    def make_query(self):
        s = select([self.cookies])
        rp = self.connection.execute(s)  # This tells rp, the ResultProxy, to return all the rows.
        results = rp.fetchall()
        """
         also available first(),
         fetchone() - Returns one row, and leaves the cursor open
         for you to make additional fetch calls 
         scalar()
        """

        first_row = results[0]
        # first_row[1] # access column by index
        fr = first_row.cookie_name  # access column by name
        # fr = first_row[self.cookies.c.cookie_name] # access column by Column object
        print(fr)

    def make_query_controlling_the_columns_in_the_query(self):
        s = select([self.cookies.c.cookie_name, self.cookies.c.quantity])
        rp = self.connection.execute(s)
        print(rp.keys())
        result = rp.first()

    def make_query_ordering(self):
        s = select([self.cookies.c.cookie_name, self.cookies.c.quantity])
        s = s.order_by(self.cookies.c.quantity)
        # s = select([...]).order_by(...)
        # s = s.order_by(desc(self.cookies.c.quantity)) # descending order
        rp = self.connection.execute(s)

        print(rp.fetchall())

    def make_query_limiting(self):
        s = select([self.cookies.c.cookie_name, self.cookies.c.quantity])
        s = s.order_by(self.cookies.c.quantity)
        s = s.limit(2) # ограничение количества строк в результате
        # rp = self.connection.execute(s)
        # print([result.cookie_name for result in rp])
        #print(s) #  shows us the actual SQL statement

    def make_guery_count(self):
        from sqlalchemy.sql import func
        s = select([func.sum(self.cookies.c.quantity)])
        rp = self.connection.execute(s)
        print(rp.scalar())  # Notice the use of scalar, which will return only the leftmost column in the first record.

    def make_query_sum(self):
        from sqlalchemy.sql import func
        # s = select([func.count(self.cookies.c.quantity)])
        # rp = self.connection.execute(s)
        # record = rp.first()
        # print(record.keys()) #  This will show us the columns in the ResultProxy. (>>> [u'count_1'])
        # print(record.count_1)  # The column name is autogenerated and is commonly <func_name>_<position>.

        """
        use lable function        
        """
        s = select([func.count(self.cookies.c.cookie_name).label('inventory_count')])
        rp = self.connection.execute(s)
        record = rp.first()
        print(record.keys())
        print(record.inventory_count)

    def make_query_where(self):
        s = select([self.cookies]).where(self.cookies.c.cookie_name == 'peanut butter')
        # s = select([self.cookies]).where(self.cookies.c.cookie_name.like('%peanut butter%')) #  2-й вариант
        rp = self.connection.execute(s)
        record = rp.fetchall()
        print(record)

    # Operators

    def make_query_cast(self):
        from sqlalchemy import cast
        s = select([self.cookies.c.cookie_name,
                    cast((self.cookies.c.quantity * self.cookies.c.unit_cost),
                         Numeric(12, 2)).label('inv_cost')])
        for row in self.connection.execute(s):
            print('{} - {}'.format(row.cookie_name, row.inv_cost))
        # s = select([self.cookies.c.cookie_name, (self.cookies.c.quantity * self.cookies.c.unit_cost).label('inv_cost')])
        # for row in self.connection.execute(s):
        #     print('{} - {}'.format(row.cookie_name, row.inv_cost))





db_manager = DataBaseManager()
# db_manager.try_insert()
#db_manager.make_query()
db_manager.make_query_cast()
