# Spark project for Big Data university course
### Made by Artem Hrechka [(Source)](https://github.com/W1nLin4n/big-data-course-project)

## Dataset description
The dataset used for this task is [IMDb dataset](https://developer.imdb.com/non-commercial-datasets/). 
It contains data about titles, their authors, ratings, and so on. It is made up 
of 7 tables, connected to each other with string keys. Tables are stored as tsv 
files encoded in utf-8 charset. These tables are not normalized, as columns can 
contain arrays. Also, some columns require transformations before they can be 
used in requests.

## Data processing steps
### 1. Arrays
In source files array data type is represented as a string of values separated
by commas. To process them correctly, they are read as strings, and later split
by commas, converting them into proper arrays.
### 2. Booleans
In source files boolean data type is represented as an integer 0 or 1. To process 
them correctly, they are read as integers, and later converted into boolean.
### 3. Column renaming
Columns in source files use camelCase. This code changes their names into snake_case.
### 4. Null values
Many columns contain null values. This is how they were dealt with:
- primary_name(name.basics) - removed all rows with null, required for requests
- death_year(name.basics) - removed column, not necessary for requests
- primary_profession(name.basics) - removed column, not necessary for requests
- known_for_titles(name.basics) - removed column, not necessary for requests
- region(title.akas) - removed all rows with null, required for requests
- language(title.akas) - removed column, not necessary for requests
- types(title.akas) - removed column, not necessary for requests
- attributes(title.akas) - removed column, not necessary for requests
- is_original_title(title.akas) - removed column, not necessary for requests
- is_adult(title.basics) - fill null with false  
All other null values were left untouched.
### 5. Duplicates
There are no duplicates in dataframes' primary keys, so no need to remove any rows.

## 20 questions to IMDb dataset
1. What are the 5 most liked genres in the last 5 years?
2. How does the type/format of a title affect its popularity in the last 5 years?
3. How does the age rating of a title affect its rating on average?
4. How many actors/actresses from each age bracket star in a film on average?
5. Which film is the most popular in each year?
6. What is the average amount of seasons for TV series?
7. How does the amount of people working on a film affect its popularity?
8. Which 5 regions are the most popular translation targets?
9. Which 5 actors/actresses are rated the highest?
10. How does the film's length influence its rating?
11. How many episodes are there in a TV series season on average?
12. What is the average number of titles an actor/actress stars in per year?
13. How does the average popularity of films change through years?
14. How many films are produced in each year?
15. Which 10 combinations of two genres occur most frequently?
16. Who are the directors of the 10 most popular films in the last 5 years?
17. Which 5 TV series had the longest runtime?
18. What is the least popular title from each of the 5 most popular directors?
19. How does the regional distribution of a title affect its popularity?
20. Which 5 genres are the most popular in titles for adults?

Popularity in this context is related to the amount of votes under a title. 
For people, it is measured as an average popularity of titles they participated in.