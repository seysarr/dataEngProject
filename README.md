# DataEng 2024 Template Repository

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: **Seynabou SARR, Roua BOUSLIMI**

## Abstract

Our project aims to recommend healthy recipes according to their benefits and effects on different parts of the body. Here are the different questions we would like our database to answer:

1: Which recipes are best for specific health objectives (e.g. improving digestion)?

2: Which recipes are suitable for specific diets (e.g. vegetarian, vegan, gluten-free)?

3: Which ingredients have the most health effects ?

Detailed report and the presentation poster are available in "pdfs" folder.

## Datasets
1-	Hummus Data : https://gitlab.com/felix134/connected-recipe-data-set 

2-	Spooncular : https://spoonacular.com/food-api

## Project steps
### Ingestion

We retrieved a dataset from a previous study of recipes carried out by a team at INSA: Hummus data. This dataset contains details of recipes scrapped from food.com, along with authors data and review ratings. We have added a few recipes from the spooncular api. The ingestion part is fairly simple.

For this ingestion part, we chose to persist our data in csv format.

### Wrangling
Hummus data came with authors information and review we didn't want to use. So we removed columns related to them. We also did some cleaning in data from Hummus and spooncular. After that we had to add diets information. Diets are affected to a recipe using keywords in ingredients list.

Another part of our project was to link recipes with some body objectives. To do so, we tried to categorize them usings amount of nutrients within recipes. We used some recommendations of macro-nutrient proportions per day and per meal. The calculation approach is generalized and doesn't include detailed factors such as age, height, weight, activity level, and overall health.

Finally we store our data on mongoDB collection.

### Production
We construct a graph DB using neo4j and according to this schema.

![Graph_Schema](./images/graph_schema.png)

### Requirements
- You need docker installed in your computer.
- Spooncular is a freemium api. So one can only have 50 free calls a day. An api key is required. So to run completely the dag, you will need to get one. Here is the link to do so : https://www.postman.com/spoonacular-api/spoonacular-api/collection/rqqne3j/spoonacular-api. Then, you have to provide the value in the .env file. The corresponding variable is "SPOONCULAR_API_KEY".

Note: I added one for teacher's tests, to be removed

Required libraries are included in the .env file for airflow pipeline and in notebooks/requirements.txt for the analytic part.

## How to run
In the root folder of the project where is located the docker-compose.yaml file, run "docker compose up -d --build" or "docker-compose up -d --build if you have an older version of Docker.
- "-d" is to run in background and free the terminal
- "--build" is to build the solution, useful for the first run or when you make changes in config

We have exposed a mongodb client to view our database at the endpoint http://localhost:8081/. Credentials are: username="admin", password="admin"

The same is done for the graph database at the endpoint http://localhost:7474/browser/. Credentials are: username="neo4j", password="adminPass"

In case you are running the project offline, we have added test data in the "data_offline" folder.

## Analytics
We provided a notebook on the ./notebooks folder to give an overview of our database features.