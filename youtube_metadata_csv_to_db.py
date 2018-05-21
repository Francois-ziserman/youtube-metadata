from sqlalchemy import *
import csv
from datetime import datetime

DT_FORMAT = "%Y-%m-%dT%H:%M:%S"
CSV_PATH = "/Users/francois/Google Drive/these_pauline/csv/"


class Db:
    def __init__(self):
        self.engine = create_engine('sqlite:///dbyoutube.sql')
        self.metadata = MetaData()
        self.table_video = Table('video', self.metadata,
                                 Column('category', String(10), nullable=False),
                                 Column('name', String(25), nullable=False),
                                 Column('id', String(20), nullable=False),
                                 Column('title', String(160), primary_key=True, nullable=False),
                                 Column('description', String(500), nullable=False),
                                 Column('publishedAt', DateTime(), nullable=False),
                                 Column('duration', Integer(), nullable=False),
                                 Column('views', Integer(), nullable=False),
                                 Column('likes', Integer(), nullable=False),
                                 Column('dislikes', Integer(), nullable=False),
                                 Column('comments', Integer(), nullable=False),
                                 Column('commentLock', Boolean(), nullable=False)
                                 )
        self.table_comment = Table('comment', self.metadata,
                                   Column('videoId', String(20), nullable=False),
                                   Column('id', String(25), primary_key=True, nullable=False),
                                   Column('author', String(60), nullable=False),
                                   Column('text', String(700), nullable=False),
                                   Column('replies', Integer(), nullable=False),
                                   Column('likes', Integer(), nullable=False),
                                   Column('publishedAt', DateTime(), nullable=False)
                                   )
        self.metadata.create_all(self.engine)
        self.conn = self.engine.connect()

    def read_csv(self, list_file):
        with open(list_file, 'r') as file_csv_list:
            for line in file_csv_list:
                file_name = line.replace("\n", "")
                if line.startswith("video"):
                    self.read_videos(file_name)
                else:
                    self.read_comments(file_name)

    def read_videos(self, file_name):
        with open(CSV_PATH + file_name) as file_videos:
            print("read_videos. Start reading csv : " + file_name)
            csv_reader_videos = csv.reader(file_videos, delimiter=',')
            i = 0
            ok = 0
            errors = 0
            for row in csv_reader_videos:
                if i == 0:
                    i += 1
                    continue  # do not add the header in the db
                try:
                    ins = self.table_video.insert()
                    self.conn.execute(ins,
                                      category=row[0],
                                      name=row[1],
                                      id=row[2],
                                      title=row[3],
                                      description=row[4],
                                      publishedAt=self.get_datetime(row[5]),
                                      duration=self.get_int(row[6]),
                                      views=self.get_int(row[7]),
                                      likes=self.get_int(row[8]),
                                      dislikes=self.get_int(row[9]),
                                      comments=self.get_int(row[10]),
                                      commentLock=self.get_bool(row[11])
                                      )
                    ok += 1
                    if ok % 1000 == 0:
                        print("  read_videos. lines: " + str(ok/1000) + " k")
                except:
                    print("  read_videos. error on line " + str(i) + " " + str(row))
                    errors += 1
                i += 1
        print("read_videos. Add lines: " + str(ok) + " Errors: " + str(errors))

    def get_datetime(self, input):
        return datetime.strptime(input[:-5], DT_FORMAT)

    def get_int(self, input):
        return int(input)

    def get_bool(self, input):
        if input=='False':
            return False
        return True

    def read_comments(self, file_name):
        with open(CSV_PATH + file_name) as file_videos:
            print("read_comments. Start reading csv : " + file_name)
            csv_reader_comments = csv.reader(file_videos, delimiter=',')
            i = 0
            ok = 0
            errors = 0
            for row in csv_reader_comments:
                if i == 0:
                    i += 1
                    continue  # do not add the header in the db
                try:
                    ins = self.table_comment.insert()
                    self.conn.execute(ins,
                                      videoId=row[0],
                                      id=row[1],
                                      author=row[2],
                                      text=row[3],
                                      replies=self.get_int(row[4]),
                                      likes=self.get_int(row[5]),
                                      publishedAt=self.get_datetime(row[6])
                                      )
                    ok += 1
                    if ok % 1000 == 0:
                        print("  read_comments. lines: " + str(ok/1000) + " k")
                except:
                    print("  read_comments. error on line " + str(i) + " " + str(row))
                    errors += 1
                i += 1
            print("read_comments. Add lines: " + str(ok) + " Errors: " + str(errors))

        # s = select([table_video])
        # r = conn.execute(s)

        # for row in r:
        #    print(row)



db = Db()
#db.read_csv("csv_list.txt")
