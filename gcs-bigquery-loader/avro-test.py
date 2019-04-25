import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.Parse(open('user.avsc', "r").read())

writer = DataFileWriter(open("users_2.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa2", "favorite_number": 2})
writer.append({"name": "Ben2", "favorite_number": 8, "favorite_color": "red"})
writer.close()

reader = DataFileReader(open("users_2.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()



new_schema = avro.schema.Parse(open('user_new_schema.avsc', "r").read())

writer = DataFileWriter(open("users_new_schema.avro", "wb"), DatumWriter(), new_schema)
writer.append({"name": "Alyssa2", "surname": "test", "favorite_number": 2})
writer.close()