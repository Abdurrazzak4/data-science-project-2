import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = 'postgres'


def connect_db():
    conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password='postgres')
    return conn


def question_1_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from students where age > 22')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_2_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from courses where category = \'Veritabanı\'')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_3_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from students where first_name ilike \'a%\'')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_4_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from courses where course_name ilike \'%SQL%\'')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_5_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from students where age between 22 and 24')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_6_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select s.first_name, s.last_name from enrollments as e join courses as c on c.course_id = e.course_id join students as s on s.student_id = e.student_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_7_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select c.course_name, count(e.enrollment_id) as student_count from enrollments as e join courses as c on c.course_id = e.course_id join students as s on s.student_id = e.student_id group by c.course_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_8_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select i.name as instructor_name, c.course_name from instructors as i join course_instructors as ci on ci.instructor_id = i.instructor_id join courses as c on c.course_id = ci.course_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_9_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from students as s join enrollments as e on e.student_id = s.student_id join courses as c on c.course_id = e.course_id where s.student_id IS NULL')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_10_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select c.course_name, avg(s.age) from students as s join enrollments as e on e.student_id = s.student_id join courses as c on c.course_id = e.course_id group by c.course_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_11_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select s.first_name, s.last_name, count(c.course_name) as total_courses from students as s join enrollments as e on e.student_id = s.student_id join courses as c on c.course_id = e.course_id group by s.student_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_12_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select i.name, count(ci.instructor_id) from enrollments as e join course_instructors as ci on ci.course_id = e.course_id join instructors as i on ci.instructor_id = i.instructor_id group by i.name having count(ci.instructor_id) > 1')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_13_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT c.course_name, COUNT(DISTINCT e.student_id) AS unique_students FROM courses c JOIN enrollments e ON c.course_id = e.course_id GROUP BY c.course_name;')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_14_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select s.first_name, s.last_name from course_instructors as ci join courses as c on ci.course_id = c.course_id join instructors as i on i.instructor_id = ci.instructor_id join enrollments as e on e.course_id = ci.course_id join students as s on s.student_id = e.student_id where course_name ilike \'%sql%\'')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_15_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select s.first_name, s.last_name, c.course_name, i.name, e.enrollment_date from course_instructors as ci join courses as c on ci.course_id = c.course_id join instructors as i on i.instructor_id = ci.instructor_id join enrollments as e on e.course_id = ci.course_id join students as s on s.student_id = e.student_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data