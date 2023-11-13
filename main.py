import sqlite3
import pandas as pd
# создаем базу данных и устанавливаем соединение с ней
con = sqlite3.connect("library.sqlite")

print("----------------1 задание---------------")
df = pd.read_sql('''
 SELECT
 title AS Название,
 genre_name AS Жанр,
 publisher_name AS Издательство,
 year_publication AS Год_издания
 FROM book
 JOIN genre USING (genre_id)
 JOIN publisher USING (publisher_id)
 WHERE LENGTH(title) - LENGTH(REPLACE(title, ' ', '')) = 0 AND year_publication < :p_year
 ORDER BY
    Год_издания ASC,
    Название ASC
''', con, params={"p_year": 2020})
print(df)

print("----------------2 задание---------------")
df = pd.read_sql('''
 SELECT genre.genre_name AS Жанр,
 CASE WHEN count(book.book_id) > 0 THEN count(book.book_id)
      else 'нет' END AS Количество 
FROM genre
LEFT JOIN book ON genre.genre_id = book.genre_id AND book.available_numbers = 0 
GROUP BY genre.genre_name
ORDER BY genre.genre_name;
''', con)
print(df)



print("----------------3 задание---------------")
df = pd.read_sql('''
WITH popular_publisher(p_id, t_id)
 AS(
    SELECT publisher_id, COUNT(title)
    FROM book
    GROUP BY publisher_id
 ),
 get_max(max_count)
AS (
   SELECT MAX(t_id)
   FROM popular_publisher
),
get_id_publisher(pp_id)
AS (
 SELECT p_id
FROM 
    popular_publisher 
    JOIN get_max ON max_count = t_id
    )
SELECT title, GROUP_CONCAT(author_name)
FROM book
JOIN book_author ON book.book_id = book_author.book_id
JOIN author ON book_author.author_id = author.author_id
JOIN get_id_publisher ON book.publisher_id = pp_id
GROUP BY title;
''', con)
print(df)


print("----------------4 задание---------------")
"""
df = pd.read_sql('''
DROP TABLE IF EXISTS rating;
''', con)
print(df)
df = pd.read_sql('''
CREATE TABLE rating AS

SELECT reader_id, SUM(
  CASE
    WHEN Juliandate(return_date) - Juliandate(borrow_date) < 14 THEN 5
    WHEN Juliandate(return_date) - Juliandate(borrow_date) > 30 THEN -2
    WHEN return_date IS NULL THEN 1
    ELSE 2.............
  END) AS points
FROM book_reader
GROUP BY reader_id;
''', con)
print(df)
"""

df = pd.read_sql('''
SELECT *
FROM rating;
''', con)
print(df)

print("----------------5 задание---------------")
df = pd.read_sql('''
SELECT title, genre_name, 
publisher_name,
CASE
    WHEN available_numbers > ROUND(AVG(available_numbers) OVER win_test) 
        THEN 'больше на ' || (available_numbers - ROUND(AVG(available_numbers) OVER win_test))
    WHEN available_numbers < ROUND(AVG(available_numbers) OVER win_test)
        THEN 'меньше на ' || (ROUND(AVG(available_numbers) OVER win_test) - available_numbers)
    ELSE 'равно среднему'
END AS Отклонение
FROM book
JOIN genre ON genre.genre_id = book.genre_id
JOIN publisher ON book.publisher_id = publisher.publisher_id
WINDOW win_test AS (PARTITION BY 1)
ORDER BY title ASC, Отклонение ASC;
''', con)
print(df)
con.close()