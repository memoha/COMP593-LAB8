"""
Description:
 Creates the people table in the Social Network database
 and populates it with 200 fake people.

Usage:
 python create_db.py
"""
import os
import inspect
import sqlite3 
import pandas 
from random import randint, choice
from datetime import datetime
from pprint import pprint
from faker import Faker
fake = Faker("en_CA")
con = sqlite3.connect('social_network.db')
cur = con.cursor()


def main():
    global db_path
    db_path = os.path.join(get_script_dir(), 'social_network.db')
    populate_people_table()
    con.close()

def create_people_table():
    """Creates the people table in the database"""

    create_ppl_tbl_query = """
        CREATE TABLE IF NOT EXISTS people
        (
            id   INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL,
            address     TEXT NOT NULL,
            city        TEXT NOT NULL,
            province    TEXT NOT NULL,
            bio         TEXT,
            age         INTEGER,
            created_at  DATETIME NOT NULL,
            updated_at  DATETIME NOT NULL
        );"""
 

    cur.execute(create_ppl_tbl_query)
    con.commit()

    # TODO: Create function body
    add_person_query = """
        INSERT INTO people
        (
            name,
            email,
            address,
            city,
            province,
            bio,
            age,
            created_at,
            updated_at
        )
        VALUES 
        (
            'Megha Mohan',
            'megha.mohan@whatever.net',
            '123 Fake St.','Fakesville',
            'Fake Prince Island',
            'Enjoys to imitate how other people are talking.',
            19,
            datetime.now(),
            datetime.now()
        );"""



    cur.execute (add_person_query)


    con.commit()


    cur.execute('SELECT * FROM people')
    all_people = cur.fetchall()
    pprint(all_people)

    con.commit()




def populate_people_table():
    """Populates the people table with 200 fake people"""
    
    for i in range(200):
        name = fake.name()
        email = fake.email()
        address = fake.address()
        city = fake.city()
        province = fake.state()
        bio = fake.text()
        age = fake.random_int(1, 100)
        created_at = datetime.now()
        updated_at = datetime.now()

        cur.execute("INSERT INTO people (name, email, address, city, province, bio, age, created_at, updated_at)VALUES (?,?,?,?,?,?,?,?,?)", (name, email, address, city, province, bio, age, created_at, updated_at))

        con.commit()

    cur.execute("SELECT * FROM people")
    all_people = cur.fetchall()
    dataframe = pandas.DataFrame(all_people, columns=['id', 'name', 'email', 'address', 'city', 'province', 'bio', 'age', 'created_at', 'updated_at'])
    dataframe.to_csv('people.csv')    


    # TODO: Create function body
    return

# SQL query that creates a table named 'relationships'.
create_relationships_tbl_query = """
 CREATE TABLE IF NOT EXISTS relationships
 (
 id INTEGER PRIMARY KEY,
 person1_id INTEGER NOT NULL,
 person2_id INTEGER NOT NULL,
 type TEXT NOT NULL,
 start_date DATE NOT NULL,
 FOREIGN KEY (person1_id) REFERENCES people (id),
 FOREIGN KEY (person2_id) REFERENCES people (id)
 );
"""
# Execute the SQL query to create the 'relationships' table.
cur.execute(create_relationships_tbl_query)


# SQL query that inserts a row of data in the relationships table.
add_relationship_query = """
 INSERT INTO relationships
 (
 person1_id,
 person2_id,
 type,
 start_date
 )
 VALUES (?, ?, ?, ?);
"""
fake = Faker()
# Randomly select first person in relationship
person1_id = randint(1, 200)
# Randomly select second person in relationship
# Loop ensures person will not be in a relationship with themself
person2_id = randint(1, 200)
while person2_id == person1_id:
 person2_id = randint(1, 200)
# Randomly select a relationship type
rel_type = choice(('friend', 'spouse', 'partner', 'relative'))
# Randomly select a relationship start date between now and 50 years ago
start_date = fake.date_between(start_date='-50y', end_date='today')
# Create tuple of data for the new relationship
new_relationship = (person1_id, person2_id, rel_type, start_date)
# Add the new relationship to the DB
cur.execute(add_relationship_query, new_relationship)

# SQL query to get all relationships
all_relationships_query = """
 SELECT person1.name, person2.name, start_date, type FROM relationships
 JOIN people person1 ON person1_id = person1.id
 JOIN people person2 ON person2_id = person2.id;
"""
# Execute the query and get all results
cur.execute(all_relationships_query)

all_relationships = cur.fetchall()

# Print sentences describing each relationship
for person1, person2, start_date, type in all_relationships:
 print(f'{person1} has been a {type} of {person2} since {start_date}.')



def get_script_dir():
    """Determines the path of the directory in which this script resides

    Returns:
        str: Full path of the directory in which this script resides
    """
    script_path = os.path.abspath(inspect.getframeinfo(inspect.currentframe()).filename)
    return os.path.dirname(script_path)



if __name__ == '__main__':
   main()