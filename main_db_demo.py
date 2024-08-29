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
    # for database in client.execute("SHOW DATABASES "):
    #     print(database)

    client.execute(f"USE archive")

    client.execute("INSERT INTO person VALUES ('Benny', 'Wu', '33', '179', '92')")
    client.execute("INSERT INTO person VALUES ('Benny', 'Wu', '33', '179', '92')")

    qs = Person.objects_in(db).filter(Person.first_name == "Benny").order_by("height")
    for benny in qs:
        print(benny.first_name, benny.last_name, benny.age, benny.height, benny.weight)

    # for benny in client.execute(qs.as_sql()):
    #     print(benny)

    # for benny in client.execute("SELECT * FROM person WHERE first_name = 'Benny'"):
    #     print(benny)

    # for table in client.execute(f"SHOW TABLES"):
    #     print(table)

    # for row in client.execute("SELECT * FROM person"):
    #     print(row)

    # client.execute(f"")

    # db = Database(database_name, db_url=f"http://{CONFIG.host_name}:8123/")
    # qs = Person.objects_in(db).filter(Person.age > 30).order_by("height")

    # for row in client.execute(qs.as_sql()):
    #     print(row)

    # db.create_table(Person)
    # db.insert(
    #     [
    #         Person(
    #             first_name="Hallo", last_name="Q", age=44, height=185.0, weight=95.0
    #         ),
    #         Person(
    #             first_name="Buffalo",
    #             last_name="Chou",
    #             age=30,
    #             height=183.0,
    #             weight=98.0,
    #         ),
    #         Person(
    #             first_name="Tarou",
    #             last_name="Yamada",
    #             age=35,
    #             height=178.0,
    #             weight=115.0,
    #         ),
    #     ]
    # )

    # queryset = Person.objects_in(db)
    # cnt = queryset.filter(Person.age > 30).count()
    # total = queryset.filter()

    # print(cnt)

    # # for person in total:
    # #     print(
    # #         person.first_name,
    # #         person.last_name,
    # #         person.age,
    # #         person.height,
    # #         person.weight,
    # #     )


if __name__ == "__main__":
    main()
