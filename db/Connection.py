import itertools
from os import getenv
import pymssql

class Conn:
  def __init__(self):
      self.server = getenv("PYMSSQL_TEST_SERVER")
      self.user = getenv("PYMSSQL_TEST_USERNAME")
      self.password = getenv("PYMSSQL_TEST_PASSWORD")
      self.conn = pymssql.connect(server="sc2sp.ddns.net", user="god", password="a", database="TMTDB")
      self.cursor = self.conn.cursor()
  def getCursor(self):
    return self.cursor
  def commit(self):
    self.conn.commit()