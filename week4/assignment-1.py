# Colab Python 코드 개선하기
import psycopg2

# Redshift connection 함수
def get_Redshift_connection():
    host = "redacted"
    redshift_user = "redacted"
    redshift_pass = "redacted"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=False) # 수정
    return conn # 수정


# Transform 함수
def transform(text):
    lines = text.split("\n")
    return lines[1:] # 수정

# Load 함수
def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('kyle_oh95', 'MALE');....;END;
    conn = get_Redshift_connection() # 수정
    with conn.cursor() as cur: # 수정
      delete_sql = "DELETE FROM kyle_oh95.name_gender"
      cur.execute(delete_sql)
      for r in lines:
          if r != '':
              (name, gender) = r.split(",")
              print(name, "-", gender)
              sql = "INSERT INTO kyle_oh95.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
              print(sql)
              cur.execute(sql)
    conn.commit() # 수정


'''
Transaction 처리가 정상적으로 이루어졌는지의 확인은 load() 함수에서 INSERT INTO sql에 의도적으로 오타를 내서
DELETE 문만 먼저 실행되고, INSERT INTO가 무조건 실패하도록 정의해서 실험했습니다.

autocommit=True인 동안에는 INSERT가 실패하더라도 DELETE는 무조건 성공해서 데이터가 없는 결과가 나왔지만
autocommit=False로 설정하고 conn.commit()을 수동으로 처리해주니, INSERT가 실패하면 DELETE도 실패하도록 잘 처리 되었습니다.
'''