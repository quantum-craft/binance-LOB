from config import CONFIG
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import (
    ArrayField,
    DateTime64Field,
    StringField,
    UInt8Field,
    Float64Field,
    UInt64Field,
    LowCardinalityField,
)
from model import (
    DepthSnapshot,
    DiffDepthStreamDispatcher,
    Logger,
    LoggingLevel,
    DiffDepthStream,
    LoggingMsg,
)
from infi.clickhouse_orm.engines import MergeTree, ReplacingMergeTree
from clickhouse_driver import Client


class Person(Model):
    first_name = StringField()
    last_name = StringField()
    age = UInt8Field()
    height = Float64Field()
    weight = Float64Field()

    engine = MergeTree(partition_key=["first_name"], order_by=["age"])


def main():
    database_name = CONFIG.db_name

    client = Client(host=CONFIG.host_name)
    db = Database(database_name, db_url=f"http://{CONFIG.host_name}:8123/")

    client.execute(f"USE archive")

    for table in client.execute("SHOW TABLES"):
        print(table)

    cnt = DepthSnapshot.objects_in(db).count()
    cnt2 = DiffDepthStream.objects_in(db).count()
    cnt3 = LoggingMsg.objects_in(db).count()

    print(cnt)
    print(cnt2)
    print(cnt3)


if __name__ == "__main__":
    main()
